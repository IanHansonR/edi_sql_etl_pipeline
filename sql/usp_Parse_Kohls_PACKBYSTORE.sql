/*
    Stored Procedure: usp_Parse_Kohls_PACKBYSTORE
    Purpose: Parse Kohl's PACK BY STORE EDI 850 orders from EDIGatewayInbound and populate reporting tables

    Source: EDIGatewayInbound (filtered by CompanyCode = 'Kohls', ReferencePOType = 'PACK BY STORE')
    Target: EDI_Report_Header, EDI_Report_Detail

    Note: This is a simplified version with no logging, minimal error handling, no transactions.

    PACK BY STORE orders are similar to BULK but with one key difference:
    The SDQ is an ARRAY of objects (not a single object) because there are too many
    store allocations to fit in one EDI segment.

    Example SDQ structure:
    "SDQ": [
        {"SDQ01":"EA","SDQ02":"92","SDQ03":"00108","SDQ04":"2","SDQ05":"00110","SDQ06":"1",...},
        {"SDQ01":"EA","SDQ02":"92","SDQ03":"00234","SDQ04":"3","SDQ05":"00345","SDQ06":"2",...},
        ...
    ]
*/

CREATE OR ALTER PROCEDURE dbo.usp_Parse_Kohls_PACKBYSTORE
AS
BEGIN
    SET NOCOUNT ON;

    -- Process each unprocessed Kohl's PACK BY STORE record
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
          AND ReportingProcessStatus IS NULL
          AND JSON_VALUE(JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.ReferencePOType') = 'PACK BY STORE'
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
            @DownloadDate, @OrderDate, 'PACK BY STORE', @Version, @Id, GETDATE()
        );

        SET @HeaderId = SCOPE_IDENTITY();

        -- Parse line items and explode by store allocation
        -- PurchaseOrderDetails is always an array for PACK BY STORE
        -- SDQ is an ARRAY of objects (multiple SDQ segments)
        ;WITH LineItems AS (
            SELECT
                JSON_VALUE(detail.value, '$.LineItemId') AS LineItemId,
                JSON_VALUE(detail.value, '$.VendorItemNumber') AS Style,
                JSON_VALUE(detail.value, '$.ColorDescription') AS Color,
                JSON_VALUE(detail.value, '$.SizeDescription') AS Size,
                JSON_VALUE(detail.value, '$.GTIN') AS UPC,
                JSON_VALUE(detail.value, '$.BuyerPartNumber') AS SKU,
                JSON_VALUE(detail.value, '$.UOMTypeCode') AS UOM,
                TRY_CAST(JSON_VALUE(detail.value, '$.UnitPrice') AS DECIMAL(18,4)) AS UnitPrice,
                TRY_CAST(JSON_VALUE(detail.value, '$.SalesPrice') AS DECIMAL(18,4)) AS RetailPrice,
                TRY_CAST(JSON_VALUE(detail.value, '$.PackSize') AS INT) AS InnerPack,
                TRY_CAST(JSON_VALUE(detail.value, '$.Pack') AS INT) AS QtyPerInnerPack,
                JSON_QUERY(detail.value, '$.DestinationInfo.SDQ') AS SDQ_JSON
            FROM OPENJSON(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails') AS detail
        ),
        -- Unwrap the SDQ array - each element is an SDQ segment object
        SDQ_Segments AS (
            SELECT
                li.LineItemId,
                li.Style,
                li.Color,
                li.Size,
                li.UPC,
                li.SKU,
                li.UOM,
                li.UnitPrice,
                li.RetailPrice,
                li.InnerPack,
                li.QtyPerInnerPack,
                sdq_segment.value AS SDQ_Segment_JSON
            FROM LineItems li
            CROSS APPLY OPENJSON(li.SDQ_JSON) AS sdq_segment
            WHERE li.SDQ_JSON IS NOT NULL
              AND ISJSON(li.SDQ_JSON) = 1
              AND LEFT(LTRIM(li.SDQ_JSON), 1) = '['  -- SDQ is an array
        ),
        -- Parse each SDQ segment's key-value pairs
        SDQ_Parsed AS (
            SELECT
                ss.*,
                sdq.[key] AS SDQ_Key,
                sdq.value AS SDQ_Value,
                TRY_CAST(SUBSTRING(sdq.[key], 4, 2) AS INT) AS SDQ_Index
            FROM SDQ_Segments ss
            CROSS APPLY OPENJSON(ss.SDQ_Segment_JSON) AS sdq
            WHERE sdq.[key] LIKE 'SDQ%'
              AND TRY_CAST(SUBSTRING(sdq.[key], 4, 2) AS INT) >= 3  -- Skip SDQ01, SDQ02
        ),
        -- Pair stores (odd index) with quantities (even index)
        -- Need to join within the same segment (use SDQ_Segment_JSON as grouping key)
        StoreAllocations AS (
            SELECT
                s.LineItemId,
                s.Style,
                s.Color,
                s.Size,
                s.UPC,
                s.SKU,
                s.UOM,
                s.UnitPrice,
                s.RetailPrice,
                s.InnerPack,
                s.QtyPerInnerPack,
                s.SDQ_Value AS StoreNumber,
                TRY_CAST(q.SDQ_Value AS INT) AS Qty
            FROM SDQ_Parsed s
            INNER JOIN SDQ_Parsed q
                ON s.LineItemId = q.LineItemId
                AND s.UPC = q.UPC
                AND s.SDQ_Segment_JSON = q.SDQ_Segment_JSON  -- Must be same SDQ segment
                AND s.SDQ_Index + 1 = q.SDQ_Index
            WHERE s.SDQ_Index % 2 = 1  -- Odd indices are stores (3, 5, 7, ...)
              AND q.SDQ_Index % 2 = 0  -- Even indices are quantities (4, 6, 8, ...)
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
            0 AS IsBOM  -- PACK BY STORE orders are not BOM
        FROM StoreAllocations
        WHERE Qty > 0;

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
