/*
    Stored Procedure: usp_Parse_Kohls_BULK
    Purpose: Parse Kohl's BULK EDI 850 orders from EDIGatewayInbound and populate reporting tables

    Source: EDIGatewayInbound (filtered by CompanyCode = 'Kohls', ReferencePOType = 'BULK')
    Target: EDI_Report_Header, EDI_Report_Detail

    Note: This is a simplified version with no logging, minimal error handling, no transactions.
*/

CREATE OR ALTER PROCEDURE dbo.usp_Parse_Kohls_BULK
AS
BEGIN
    SET NOCOUNT ON;

    -- Process each unprocessed Kohl's BULK record
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
          AND ReportingProcessStatus IS NULL
          AND JSON_VALUE(JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.ReferencePOType') = 'BULK'
        ORDER BY Created ASC;

    OPEN record_cursor;
    FETCH NEXT FROM record_cursor INTO @Id, @JSONContent, @DownloadDate;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Variables for header
        DECLARE @CustomerPO VARCHAR(50),
                @Company VARCHAR(100),
                @OrderDate DATE,
                @StartDate DATE,
                @CompleteDate DATE,
                @Department VARCHAR(100),
                @Version INT,
                @HeaderId INT;

        -- Extract header fields
        SET @CustomerPO = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrderNumber');
        SET @Company = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.CompanyCode');
        SET @Department = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.DepartmentNumber');

        -- Parse dates (YYYYMMDD format)
        SET @OrderDate = TRY_CONVERT(DATE, JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.OrderDate'), 112);
        SET @StartDate = TRY_CONVERT(DATE, JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.RequestedShipDate'), 112);
        SET @CompleteDate = TRY_CONVERT(DATE, JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.CancelDate'), 112);

        -- Calculate version number (count of existing records with earlier download dates + 1)
        SELECT @Version = COUNT(*) + 1
        FROM EDI_Report_Header
        WHERE CustomerPO = @CustomerPO
          AND DownloadDate < @DownloadDate;

        -- Insert header
        INSERT INTO EDI_Report_Header (
            CustomerPO, Company, StartDate, CompleteDate, Department,
            DownloadDate, OrderDate, POType, Version, SourceTableId, ProcessedDate
        )
        VALUES (
            @CustomerPO, @Company, @StartDate, @CompleteDate, @Department,
            @DownloadDate, @OrderDate, 'BULK', @Version, @Id, GETDATE()
        );

        SET @HeaderId = SCOPE_IDENTITY();

        -- Parse line items and explode by store allocation
        -- Handle both array and single-object formats for PurchaseOrderDetails
        -- (Kohl's sends single-item orders as an object, not an array)
        ;WITH LineItems AS (
            -- Case 1: PurchaseOrderDetails is an array
            SELECT
                JSON_VALUE(detail.value, '$.VendorItemNumber') AS Style,
                JSON_VALUE(detail.value, '$.ColorDescription') AS Color,
                JSON_VALUE(detail.value, '$.SizeDescription') AS Size,
                JSON_VALUE(detail.value, '$.GTIN') AS UPC,
                JSON_VALUE(detail.value, '$.BuyerPartNumber') AS SKU,
                JSON_VALUE(detail.value, '$.UOMTypeCode') AS UOM,
                TRY_CAST(JSON_VALUE(detail.value, '$.UnitPrice') AS DECIMAL(18,4)) AS UnitPrice,
                TRY_CAST(JSON_VALUE(detail.value, '$.RetailPrice') AS DECIMAL(18,4)) AS RetailPrice,
                TRY_CAST(JSON_VALUE(detail.value, '$.PackSize') AS INT) AS InnerPack,
                TRY_CAST(JSON_VALUE(detail.value, '$.Pack') AS INT) AS QtyPerInnerPack,
                JSON_QUERY(detail.value, '$.DestinationInfo.SDQ') AS SDQ_JSON
            FROM OPENJSON(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails') AS detail
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '['

            UNION ALL

            -- Case 2: PurchaseOrderDetails is a single object
            SELECT
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorItemNumber') AS Style,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.ColorDescription') AS Color,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.SizeDescription') AS Size,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.GTIN') AS UPC,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.BuyerPartNumber') AS SKU,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.UOMTypeCode') AS UOM,
                TRY_CAST(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.UnitPrice') AS DECIMAL(18,4)) AS UnitPrice,
                TRY_CAST(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.RetailPrice') AS DECIMAL(18,4)) AS RetailPrice,
                TRY_CAST(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.PackSize') AS INT) AS InnerPack,
                TRY_CAST(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.Pack') AS INT) AS QtyPerInnerPack,
                JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.DestinationInfo.SDQ') AS SDQ_JSON
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '{'
        ),
        -- Parse SDQ key-value pairs and pair up stores with quantities
        SDQ_Parsed AS (
            SELECT
                li.*,
                sdq.[key] AS SDQ_Key,
                sdq.value AS SDQ_Value,
                -- Extract the numeric part of the key (e.g., 'SDQ03' -> 3)
                TRY_CAST(SUBSTRING(sdq.[key], 4, 2) AS INT) AS SDQ_Index
            FROM LineItems li
            CROSS APPLY OPENJSON(li.SDQ_JSON) AS sdq
            WHERE li.SDQ_JSON IS NOT NULL  -- Skip line items with no SDQ data
              AND sdq.[key] LIKE 'SDQ%'
              AND TRY_CAST(SUBSTRING(sdq.[key], 4, 2) AS INT) >= 3  -- Skip SDQ01, SDQ02
        ),
        -- Pair stores (odd index) with quantities (even index)
        StoreAllocations AS (
            SELECT
                s.Style, s.Color, s.Size, s.UPC, s.SKU, s.UOM,
                s.UnitPrice, s.RetailPrice, s.InnerPack, s.QtyPerInnerPack,
                s.SDQ_Value AS StoreNumber,
                TRY_CAST(q.SDQ_Value AS INT) AS Qty
            FROM SDQ_Parsed s
            INNER JOIN SDQ_Parsed q
                ON s.Style = q.Style
                AND s.UPC = q.UPC  -- Use UPC as unique line identifier
                AND s.SDQ_Index + 1 = q.SDQ_Index  -- Pair odd with next even
            WHERE s.SDQ_Index % 2 = 1  -- Odd indices are stores (3, 5, 7, ...)
              AND q.SDQ_Index % 2 = 0  -- Even indices are quantities (4, 6, 8, ...)
        ),
        -- Line items without SDQ data get NULL store/qty
        NoSDQ_Items AS (
            SELECT
                Style, Color, Size, UPC, SKU, UOM,
                UnitPrice, RetailPrice, InnerPack, QtyPerInnerPack,
                CAST(NULL AS VARCHAR(50)) AS StoreNumber,
                CAST(NULL AS INT) AS Qty
            FROM LineItems
            WHERE SDQ_JSON IS NULL
        ),
        -- Combine both sets
        AllDetails AS (
            SELECT * FROM StoreAllocations WHERE Qty > 0
            UNION ALL
            SELECT * FROM NoSDQ_Items
        )
        INSERT INTO EDI_Report_Detail (
            HeaderId, Style, Color, Size, UPC, SKU,
            Qty, UOM, UnitPrice, RetailPrice, InnerPack, QtyPerInnerPack,
            DC, StoreNumber, IsBOM
        )
        SELECT
            @HeaderId,
            Style,
            Color,
            Size,
            UPC,
            SKU,
            Qty,
            UOM,
            UnitPrice,
            RetailPrice,
            InnerPack,
            QtyPerInnerPack,
            NULL AS DC,
            StoreNumber,
            0 AS IsBOM  -- BULK orders are not BOM
        FROM AllDetails;

        -- Mark as processed
        UPDATE EDIGatewayInbound
        SET ReportingProcessStatus = 'Success',
            ReportingProcessed = GETDATE()
        WHERE Id = @Id;

        FETCH NEXT FROM record_cursor INTO @Id, @JSONContent, @DownloadDate;
    END;

    CLOSE record_cursor;
    DEALLOCATE record_cursor;
END;
GO
