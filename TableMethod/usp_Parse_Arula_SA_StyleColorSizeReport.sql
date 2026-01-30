/*
    Stored Procedure: usp_Parse_Arula_SA_StyleColorSizeReport
    Purpose: Parse Arula SA EDI 850 orders from EDIGatewayInbound and populate StyleColorSize Report tables

    Source: EDIGatewayInbound (filtered by CompanyCode = 'ARULA', PurchaseOrderTypeCode = 'SA')
    Target: Custom88StyleColorSizeReportHeader, Custom88StyleColorSizeReportDetail

    Prerequisite: DetailsReport must have processed the record first (DetailsReportStatus = 'Success')

    SDQ is a single JSON object (BULK pattern).
    Detail rows are aggregated by Style + Color + Size + UPC with Qty = SUM of all store quantities.
    Amount = UnitPrice * Qty for each detail row.

    Arula SA-specific:
    - Size = 'PPK' when BOMDetails exists, otherwise PurchaseOrderDetails.SizeDescription
    - UPC = PurchaseOrderDetails.ProductId (not GTIN)
    - SKU = PurchaseOrderDetails.ProductId (not BuyerPartNumber)
*/

CREATE OR ALTER PROCEDURE dbo.usp_Parse_Arula_SA_StyleColorSizeReport
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
        WHERE CompanyCode = 'ARULA'
          AND TransactionType = '850'
          AND Status = 'Downloaded'
          AND StyleColorSizeReportStatus IS NULL
          AND DetailsReportStatus = 'Success'
          AND ISJSON(JSONContent) = 1
          AND JSON_VALUE(JSONContent, '$.PurchaseOrderHeader.PurchaseOrderTypeCode') = 'SA'
        ORDER BY Created ASC;

    OPEN record_cursor;
    FETCH NEXT FROM record_cursor INTO @Id, @JSONContent, @DownloadDate;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @CustomerPO NVARCHAR(100),
                @Company NVARCHAR(100),
                @StartDate DATE,
                @CompleteDate DATE,
                @Department NVARCHAR(100),
                @POType NVARCHAR(100),
                @Version INT,
                @HeaderId BIGINT;

        -- Extract header fields
        SET @CustomerPO = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrderNumber');
        SET @Company = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.CompanyCode');
        SET @Department = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.DepartmentNumber');
        SET @POType = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrderTypeCode');

        -- Parse dates (YYYYMMDD format)
        SET @StartDate = TRY_CONVERT(DATE, JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.RequestedShipDate'), 112);
        SET @CompleteDate = TRY_CONVERT(DATE, JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.CancelDate'), 112);

        -- Look up version from DetailsReport header (matched by source record ID)
        SELECT @Version = Version
        FROM Custom88DetailsReportHeader
        WHERE SourceTableId = @Id;

        -- Insert header (TotalOrderQty and TotalOrderAmount will be updated after details are inserted)
        INSERT INTO Custom88StyleColorSizeReportHeader (
            Company, POType, CustomerPO, DateDownloaded,
            TotalOrderQty, TotalOrderAmount, StartDate, CompleteDate, Department, Version
        )
        VALUES (
            @Company, @POType, @CustomerPO, CAST(@DownloadDate AS DATE),
            0, 0, @StartDate, @CompleteDate, @Department, @Version
        );

        SET @HeaderId = SCOPE_IDENTITY();

        -- Parse line items and aggregate by Style + Color + Size + UPC
        -- Handle both array and single-object formats for PurchaseOrderDetails
        -- Size = 'PPK' when BOMDetails exists, otherwise SizeDescription
        ;WITH LineItems AS (
            -- Case 1: PurchaseOrderDetails is an array
            SELECT
                JSON_VALUE(detail.value, '$.VendorItemNumber') AS Style,
                JSON_VALUE(detail.value, '$.ColorDescription') AS Color,
                CASE
                    WHEN JSON_QUERY(detail.value, '$.BOMDetails') IS NOT NULL THEN 'PPK'
                    ELSE JSON_VALUE(detail.value, '$.SizeDescription')
                END AS Size,
                JSON_VALUE(detail.value, '$.ProductId') AS UPC,
                JSON_VALUE(detail.value, '$.ProductId') AS SKU,
                TRY_CAST(JSON_VALUE(detail.value, '$.UnitPrice') AS FLOAT) AS UnitPrice,
                TRY_CAST(NULLIF(LTRIM(RTRIM(JSON_VALUE(detail.value, '$.SalesPrice'))), '') AS FLOAT) AS RetailPrice,
                JSON_QUERY(detail.value, '$.DestinationInfo.SDQ') AS SDQ_JSON
            FROM OPENJSON(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails') AS detail
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '['

            UNION ALL

            -- Case 2: PurchaseOrderDetails is a single object
            SELECT
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorItemNumber') AS Style,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.ColorDescription') AS Color,
                CASE
                    WHEN JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.BOMDetails') IS NOT NULL THEN 'PPK'
                    ELSE JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.SizeDescription')
                END AS Size,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.ProductId') AS UPC,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.ProductId') AS SKU,
                TRY_CAST(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.UnitPrice') AS FLOAT) AS UnitPrice,
                TRY_CAST(NULLIF(LTRIM(RTRIM(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.SalesPrice'))), '') AS FLOAT) AS RetailPrice,
                JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.DestinationInfo.SDQ') AS SDQ_JSON
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '{'
        ),
        -- Parse SDQ key-value pairs
        SDQ_Parsed AS (
            SELECT
                li.*,
                sdq.[key] AS SDQ_Key,
                sdq.value AS SDQ_Value,
                TRY_CAST(SUBSTRING(sdq.[key], 4, 2) AS INT) AS SDQ_Index
            FROM LineItems li
            CROSS APPLY OPENJSON(li.SDQ_JSON) AS sdq
            WHERE li.SDQ_JSON IS NOT NULL
              AND sdq.[key] LIKE 'SDQ%'
              AND TRY_CAST(SUBSTRING(sdq.[key], 4, 2) AS INT) >= 3
        ),
        -- Pair stores with quantities
        StoreAllocations AS (
            SELECT
                s.Style, s.Color, s.Size, s.UPC, s.SKU,
                s.UnitPrice, s.RetailPrice,
                TRY_CAST(q.SDQ_Value AS INT) AS Qty
            FROM SDQ_Parsed s
            INNER JOIN SDQ_Parsed q
                ON s.UPC = q.UPC
                AND s.SDQ_Index + 1 = q.SDQ_Index
            WHERE s.SDQ_Index % 2 = 1
              AND q.SDQ_Index % 2 = 0
        )
        -- Insert aggregated detail rows: one per unique Style + Color + Size + UPC
        INSERT INTO Custom88StyleColorSizeReportDetail (
            HeaderId, Style, Color, Size, UPC, SKU, UnitPrice, RetailPrice, Qty, Amount
        )
        SELECT
            @HeaderId,
            Style, Color, Size, UPC, SKU, UnitPrice, RetailPrice,
            SUM(Qty) AS Qty,
            UnitPrice * SUM(Qty) AS Amount
        FROM StoreAllocations
        WHERE Qty > 0
        GROUP BY Style, Color, Size, UPC, SKU, UnitPrice, RetailPrice;

        -- Update header with TotalOrderQty and TotalOrderAmount
        UPDATE Custom88StyleColorSizeReportHeader
        SET TotalOrderQty = (SELECT ISNULL(SUM(Qty), 0) FROM Custom88StyleColorSizeReportDetail WHERE HeaderId = @HeaderId),
            TotalOrderAmount = (SELECT ISNULL(SUM(Amount), 0) FROM Custom88StyleColorSizeReportDetail WHERE HeaderId = @HeaderId)
        WHERE Id = @HeaderId;

        -- Mark as processed
        UPDATE EDIGatewayInbound
        SET StyleColorSizeReportStatus = 'Success',
            StyleColorSizeReportProcessed = GETDATE()
        WHERE Id = @Id;

        FETCH NEXT FROM record_cursor INTO @Id, @JSONContent, @DownloadDate;
    END;

    CLOSE record_cursor;
    DEALLOCATE record_cursor;
END;
GO
