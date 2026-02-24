/*
    Stored Procedure: usp_Parse_Belk_PrePackSummaryReport
    Purpose: Parse Belk BK and RL EDI 850 orders from EDIGatewayInbound
             and populate PrePack Summary Report tables with unique BOM definitions.

    Source: EDIGatewayInbound (filtered by CompanyCode = 'BELK', PurchaseOrderTypeCode IN ('BK', 'RL'))
    Target: Custom88PrePackSummaryHeader, Custom88PrePackSummaryDetail

    Prerequisite: DetailsReport must have processed the record first (DetailsReportStatus = 'Success')

    Note: Belk BK/RL POs are HYBRID — they may contain both BOM and non-BOM items in the same PO.
          Only BOM line items are processed; POs with no BOMs are skipped.
          Belk SA orders have no BOMs and are excluded entirely.

    Belk PrePack Summary Mapping:
    - PrePackSTYLE = PurchaseOrderDetails.VendorItemNumber + ' P' + SUM(BOMDetails.Quantity) (P# suffix)
    - PrePackCOLOR = BOMDetails[0].ColorDescription (first component's color)
    - PrePackSIZE = 'PPK' + CAST(SUM(BOMDetails.Quantity) AS NVARCHAR) (e.g., "PPK12")
    - PrePackUPC = PurchaseOrderDetails.GTIN (parent level)
    - ComponentSTYLE = BOMDetails.VendorItemNumber
    - ComponentCOLOR = BOMDetails.ColorDescription
    - ComponentSIZE = BOMDetails.SizeDescription
    - ComponentUPC = BOMDetails.GTIN
    - ComponentQTY = BOMDetails.Quantity

    BOM Deduplication: Unique BOMs determined by component composition (sorted signature
    of VendorItemNumber|ColorDescription|SizeDescription|Quantity for all components).
*/

CREATE OR ALTER PROCEDURE dbo.usp_Parse_Belk_PrePackSummaryReport
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
        WHERE CompanyCode = 'BELK'
          AND TransactionType = '850'
          AND Status = 'Downloaded'
          AND PrePackSummaryReportStatus IS NULL
          AND DetailsReportStatus = 'Success'
          AND ISJSON(JSONContent) = 1
          AND JSON_VALUE(JSONContent, '$.PurchaseOrderHeader.PurchaseOrderTypeCode') IN ('BK', 'RL')
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
        SET @POType = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrderTypeCode');

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
                JSON_VALUE(detail.value, '$.VendorItemNumber') AS VendorItemNumber,
                JSON_VALUE(detail.value, '$.GTIN') AS PrePackUPC,
                JSON_QUERY(detail.value, '$.BOMDetails') AS BOMDetails_JSON
            FROM OPENJSON(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails') AS detail
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '['
              AND JSON_QUERY(detail.value, '$.BOMDetails') IS NOT NULL

            UNION ALL

            -- Case 2: PurchaseOrderDetails is a single object
            SELECT
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorItemNumber') AS VendorItemNumber,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.GTIN') AS PrePackUPC,
                JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.BOMDetails') AS BOMDetails_JSON
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '{'
              AND JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.BOMDetails') IS NOT NULL
        ),
        -- Compute Belk-specific parent-level fields and BOM signature for deduplication
        BOMWithSignature AS (
            SELECT
                -- PrePackSTYLE: VendorItemNumber + ' P' + SUM(component quantities) (same P# pattern as other Belk reports)
                li.VendorItemNumber + ' P' +
                    CAST((SELECT SUM(TRY_CAST(JSON_VALUE(bom.value, '$.Quantity') AS INT))
                          FROM OPENJSON(li.BOMDetails_JSON) AS bom) AS NVARCHAR(10)) AS PrePackSTYLE,
                -- PrePackCOLOR: first component's ColorDescription
                (SELECT TOP 1 JSON_VALUE(bom.value, '$.ColorDescription')
                 FROM OPENJSON(li.BOMDetails_JSON) AS bom) AS PrePackCOLOR,
                -- PrePackSIZE: 'PPK' + SUM(component quantities) (e.g., "PPK12")
                'PPK' +
                    CAST((SELECT SUM(TRY_CAST(JSON_VALUE(bom.value, '$.Quantity') AS INT))
                          FROM OPENJSON(li.BOMDetails_JSON) AS bom) AS NVARCHAR(10)) AS PrePackSIZE,
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
                ub.PrePackSTYLE,
                ub.PrePackCOLOR,
                ub.PrePackSIZE,
                ub.PrePackUPC,
                JSON_VALUE(bom.value, '$.VendorItemNumber') AS ComponentSTYLE,
                JSON_VALUE(bom.value, '$.ColorDescription') AS ComponentCOLOR,
                JSON_VALUE(bom.value, '$.SizeDescription') AS ComponentSIZE,
                JSON_VALUE(bom.value, '$.GTIN') AS ComponentUPC,
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
            NULL,  -- PrePackNUMBER (Belk: NULL)
            NULL,  -- PrePackSKU (Belk: NULL)
            PrePackSTYLE,
            PrePackCOLOR,
            PrePackSIZE,
            PrePackUPC,
            ComponentSTYLE,
            ComponentCOLOR,
            ComponentSIZE,
            ComponentUPC,
            NULL,  -- ComponentSKU (Belk: NULL)
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
