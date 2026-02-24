/*
    Stored Procedure: usp_Parse_Kohls_PrePackSummaryReport
    Purpose: Parse Kohl's PREPACK/COMPOUND PREPACK EDI 850 orders from EDIGatewayInbound
             and populate PrePack Summary Report tables with unique BOM definitions.

    Source: EDIGatewayInbound (filtered by CompanyCode = 'Kohls', ReferencePOType IN ('PREPACK', 'COMPOUND PREPACK'))
    Target: Custom88PrePackSummaryHeader, Custom88PrePackSummaryDetail

    Prerequisite: DetailsReport must have processed the record first (DetailsReportStatus = 'Success')

    Note: All Kohl's PREPACK line items are BOM items.

    Kohl's PrePack Summary Mapping:
    - PrePackNUMBER = PurchaseOrderDetails.GTIN (parent level)
    - PrePackSKU = PurchaseOrderDetails.BuyerPartNumber (parent level)
    - PrePackUPC = PurchaseOrderDetails.GTIN (parent level)
    - ComponentSTYLE = BOMDetails.VendorItemNumber
    - ComponentCOLOR = BOMDetails.ColorDescription
    - ComponentSIZE = BOMDetails.SizeDescription
    - ComponentUPC = BOMDetails.GTIN
    - ComponentSKU = BOMDetails.BuyerPartNumber
    - ComponentQTY = BOMDetails.Quantity

    BOM Deduplication: Unique BOMs determined by component composition (sorted signature
    of VendorItemNumber|ColorDescription|SizeDescription|Quantity for all components).
    Each unique BOM listed once per PO regardless of how many line items share that BOM.
*/

CREATE OR ALTER PROCEDURE dbo.usp_Parse_Kohls_PrePackSummaryReport
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Id INT,
            @JSONContent NVARCHAR(MAX),
            @DownloadDate DATETIME;

    DECLARE record_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            Id,
            JSONContent,
            Created AS DownloadDate
        FROM EDIGatewayInbound
        WHERE CompanyCode = 'Kohls'
          AND TransactionType = '850'
          AND Status = 'Downloaded'
          AND PrePackSummaryReportStatus IS NULL
          AND DetailsReportStatus = 'Success'
          AND ISJSON(JSONContent) = 1
          AND JSON_VALUE(JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.ReferencePOType') IN ('PREPACK', 'COMPOUND PREPACK')
        ORDER BY Created ASC;

    OPEN record_cursor;
    FETCH NEXT FROM record_cursor INTO @Id, @JSONContent, @DownloadDate;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @CustomerPO NVARCHAR(100),
                @Company NVARCHAR(100),
                @POType NVARCHAR(100),
                @Version INT,
                @HeaderId BIGINT,
                @HasBOMs BIT;

        -- Extract header fields
        SET @CustomerPO = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrderNumber');
        SET @Company = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.CompanyCode');
        SET @POType = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.ReferencePOType');

        -- Check if this PO has any BOM line items
        SET @HasBOMs = 0;

        -- Check array format (guard with format check before calling OPENJSON)
        IF LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '['
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM OPENJSON(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails') AS detail
                WHERE JSON_QUERY(detail.value, '$.BOMDetails') IS NOT NULL
            )
                SET @HasBOMs = 1;
        END

        -- Check single-object format
        IF @HasBOMs = 0
           AND JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.BOMDetails') IS NOT NULL
            SET @HasBOMs = 1;

        -- Skip POs with no BOMs (mark as processed and continue)
        IF @HasBOMs = 0
        BEGIN
            UPDATE EDIGatewayInbound
            SET PrePackSummaryReportStatus = 'Success',
                PrePackSummaryReportProcessed = GETDATE()
            WHERE Id = @Id;

            FETCH NEXT FROM record_cursor INTO @Id, @JSONContent, @DownloadDate;
            CONTINUE;
        END

        -- Look up version from DetailsReport header (matched by source record ID)
        SELECT @Version = Version
        FROM Custom88DetailsReportHeader
        WHERE SourceTableId = @Id;

        -- Insert header
        INSERT INTO Custom88PrePackSummaryHeader (
            Company, POType, CustomerPO, DateDownloaded, Version
        )
        VALUES (
            @Company, @POType, @CustomerPO, CAST(@DownloadDate AS DATE), @Version
        );

        SET @HeaderId = SCOPE_IDENTITY();

        -- Parse BOM line items, deduplicate by component composition, expand components
        ;WITH BOMLineItems AS (
            -- Case 1: PurchaseOrderDetails is an array
            SELECT
                JSON_VALUE(detail.value, '$.GTIN') AS PrePackNUMBER,
                JSON_VALUE(detail.value, '$.BuyerPartNumber') AS PrePackSKU,
                JSON_VALUE(detail.value, '$.GTIN') AS PrePackUPC,
                JSON_QUERY(detail.value, '$.BOMDetails') AS BOMDetails_JSON
            FROM OPENJSON(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails') AS detail
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '['
              AND JSON_QUERY(detail.value, '$.BOMDetails') IS NOT NULL

            UNION ALL

            -- Case 2: PurchaseOrderDetails is a single object
            SELECT
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.GTIN') AS PrePackNUMBER,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.BuyerPartNumber') AS PrePackSKU,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.GTIN') AS PrePackUPC,
                JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.BOMDetails') AS BOMDetails_JSON
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '{'
              AND JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.BOMDetails') IS NOT NULL
        ),
        -- Compute canonical BOM signature for deduplication (sorted by component fields)
        BOMWithSignature AS (
            SELECT
                li.PrePackNUMBER,
                li.PrePackSKU,
                li.PrePackUPC,
                li.BOMDetails_JSON,
                (
                    SELECT STRING_AGG(
                        CONCAT(
                            ISNULL(JSON_VALUE(bom.value, '$.VendorItemNumber'), ''), '|',
                            ISNULL(JSON_VALUE(bom.value, '$.ColorDescription'), ''), '|',
                            ISNULL(JSON_VALUE(bom.value, '$.SizeDescription'), ''), '|',
                            ISNULL(JSON_VALUE(bom.value, '$.Quantity'), '')
                        ), '~'
                    ) WITHIN GROUP (ORDER BY
                        ISNULL(JSON_VALUE(bom.value, '$.VendorItemNumber'), ''),
                        ISNULL(JSON_VALUE(bom.value, '$.SizeDescription'), ''))
                    FROM OPENJSON(li.BOMDetails_JSON) AS bom
                ) AS BOM_Signature
            FROM BOMLineItems li
        ),
        -- Deduplicate: keep one representative per unique BOM composition
        UniqueBOMs AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY BOM_Signature ORDER BY (SELECT NULL)) AS rn
            FROM BOMWithSignature
        ),
        -- Expand unique BOMs into component rows
        BOMComponents AS (
            SELECT
                ub.PrePackNUMBER,
                ub.PrePackSKU,
                ub.PrePackUPC,
                JSON_VALUE(bom.value, '$.VendorItemNumber') AS ComponentSTYLE,
                JSON_VALUE(bom.value, '$.ColorDescription') AS ComponentCOLOR,
                JSON_VALUE(bom.value, '$.SizeDescription') AS ComponentSIZE,
                JSON_VALUE(bom.value, '$.GTIN') AS ComponentUPC,
                JSON_VALUE(bom.value, '$.BuyerPartNumber') AS ComponentSKU,
                TRY_CAST(JSON_VALUE(bom.value, '$.Quantity') AS INT) AS ComponentQTY
            FROM UniqueBOMs ub
            CROSS APPLY OPENJSON(ub.BOMDetails_JSON) AS bom
            WHERE ub.rn = 1
        )
        INSERT INTO Custom88PrePackSummaryDetail (
            HeaderId, CustomerPO,
            PrePackNUMBER, PrePackSKU, PrePackSTYLE, PrePackCOLOR, PrePackSIZE, PrePackUPC,
            ComponentSTYLE, ComponentCOLOR, ComponentSIZE, ComponentUPC, ComponentSKU, ComponentQTY
        )
        SELECT
            @HeaderId,
            @CustomerPO,
            PrePackNUMBER,
            PrePackSKU,
            NULL,  -- PrePackSTYLE (Kohl's: NULL)
            NULL,  -- PrePackCOLOR (Kohl's: NULL)
            NULL,  -- PrePackSIZE (Kohl's: NULL)
            PrePackUPC,
            ComponentSTYLE,
            ComponentCOLOR,
            ComponentSIZE,
            ComponentUPC,
            ComponentSKU,
            ComponentQTY
        FROM BOMComponents;

        -- Mark as processed
        UPDATE EDIGatewayInbound
        SET PrePackSummaryReportStatus = 'Success',
            PrePackSummaryReportProcessed = GETDATE()
        WHERE Id = @Id;

        FETCH NEXT FROM record_cursor INTO @Id, @JSONContent, @DownloadDate;
    END;

    CLOSE record_cursor;
    DEALLOCATE record_cursor;
END;
GO
