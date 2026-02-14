/*
    Stored Procedure: usp_Parse_Maurices_StyleColorReport
    Purpose: Parse Maurices EDI 850 orders from EDIGatewayInbound and populate StyleColor Report tables

    Source: EDIGatewayInbound (filtered by CompanyCode = 'Maurices', ReferencePOType = 'SO')
    Target: Custom88StyleColorReportHeader, Custom88StyleColorReportDetail

    Prerequisite: DetailsReport must have processed the record first (DetailsReportStatus = 'Success')

    No SDQ parsing required (neither BOM nor regular items use SDQ).
    Qty is extracted directly from PurchaseOrderDetails.Quantity (parent Quantity for BOM items).
    Detail rows are aggregated by Style + Color + UPC with QtyOrdered = SUM of all line item quantities.
    UPC = NULL (not available in current Maurices EDI files).

    BOM Handling:
    - Color is BOM-conditional: COALESCE(BOMDetails[0].ColorDescription, ColorDescription)
    - Size not used in this report (aggregates by Style + Color + UPC only)
*/

CREATE OR ALTER PROCEDURE dbo.usp_Parse_Maurices_StyleColorReport
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
        WHERE CompanyCode = 'Maurices'
          AND TransactionType = '850'
          AND Status = 'Downloaded'
          AND StyleColorReportStatus IS NULL
          AND DetailsReportStatus = 'Success'
          AND ISJSON(JSONContent) = 1
          AND JSON_VALUE(JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.ReferencePOType') = 'SO'
        ORDER BY Created ASC;

    OPEN record_cursor;
    FETCH NEXT FROM record_cursor INTO @Id, @JSONContent, @DownloadDate;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @CustomerPO NVARCHAR(100),
                @Company NVARCHAR(100),
                @StartDate DATE,
                @CompleteDate DATE,
                @POType NVARCHAR(100),
                @Version INT,
                @HeaderId BIGINT;

        -- Extract header fields
        SET @CustomerPO = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrderNumber');
        SET @Company = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.CompanyCode');
        SET @POType = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.ReferencePOType');

        -- Parse dates (YYYYMMDD format)
        SET @StartDate = TRY_CONVERT(DATE, JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.DoNotDeliveryBefore'), 112);
        SET @CompleteDate = TRY_CONVERT(DATE, JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.CancelDate'), 112);

        -- Look up version from DetailsReport header (matched by source record ID)
        SELECT @Version = Version
        FROM Custom88DetailsReportHeader
        WHERE SourceTableId = @Id;

        -- Insert header
        INSERT INTO Custom88StyleColorReportHeader (
            Company, POType, CustomerPO, DateDownloaded,
            StartDate, CompleteDate, Version
        )
        VALUES (
            @Company, @POType, @CustomerPO, CAST(@DownloadDate AS DATE),
            @StartDate, @CompleteDate, @Version
        );

        SET @HeaderId = SCOPE_IDENTITY();

        -- Parse line items and aggregate by Style + Color
        -- No SDQ parsing needed for Maurices; Qty comes directly from PurchaseOrderDetails.Quantity
        -- Handle both array and single-object formats for PurchaseOrderDetails
        ;WITH LineItems AS (
            -- Case 1: PurchaseOrderDetails is an array
            SELECT
                JSON_VALUE(detail.value, '$.VendorItemNumber') AS Style,
                -- BOM-conditional Color: try BOM first, fallback to direct ColorDescription
                COALESCE(
                    (SELECT TOP 1 JSON_VALUE(bom.value, '$.ColorDescription')
                     FROM OPENJSON(JSON_QUERY(detail.value, '$.BOMDetails')) AS bom),
                    JSON_VALUE(detail.value, '$.ColorDescription')
                ) AS Color,
                CAST(NULL AS NVARCHAR(100)) AS UPC,
                CAST(TRY_CAST(JSON_VALUE(detail.value, '$.Quantity') AS FLOAT) AS INT) AS Qty
            FROM OPENJSON(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails') AS detail
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '['

            UNION ALL

            -- Case 2: PurchaseOrderDetails is a single object
            SELECT
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorItemNumber') AS Style,
                -- BOM-conditional Color: try BOM first, fallback to direct ColorDescription
                COALESCE(
                    (SELECT TOP 1 JSON_VALUE(bom.value, '$.ColorDescription')
                     FROM OPENJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.BOMDetails')) AS bom),
                    JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.ColorDescription')
                ) AS Color,
                CAST(NULL AS NVARCHAR(100)) AS UPC,
                CAST(TRY_CAST(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.Quantity') AS FLOAT) AS INT) AS Qty
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '{'
        )
        -- Insert aggregated detail rows: one per unique Style + Color + UPC
        INSERT INTO Custom88StyleColorReportDetail (
            HeaderId, CustomerPO, Style, Color, UPC, QtyOrdered
        )
        SELECT
            @HeaderId,
            @CustomerPO,
            Style,
            Color,
            UPC,
            SUM(Qty) AS QtyOrdered
        FROM LineItems
        GROUP BY Style, Color, UPC;

        -- Mark as processed
        UPDATE EDIGatewayInbound
        SET StyleColorReportStatus = 'Success',
            StyleColorReportProcessed = GETDATE()
        WHERE Id = @Id;

        FETCH NEXT FROM record_cursor INTO @Id, @JSONContent, @DownloadDate;
    END;

    CLOSE record_cursor;
    DEALLOCATE record_cursor;
END;
GO
