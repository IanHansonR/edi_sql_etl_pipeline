/*
    Stored Procedure: usp_Parse_Kohls_PACKBYSTORE_DetailsReport
    Purpose: Parse Kohl's PACK BY STORE EDI 850 orders from EDIGatewayInbound and populate Details Report tables

    Source: EDIGatewayInbound (filtered by CompanyCode = 'Kohls', ReferencePOType = 'PACK BY STORE')
    Target: Custom88DetailsReportHeader, Custom88DetailsReportDetail

    Note: This is a simplified version with no logging, minimal error handling, no transactions.

    PACK BY STORE orders are similar to BULK but with one key difference:
    The SDQ is an ARRAY of objects (not a single object) because there are too many
    store allocations to fit in one EDI segment.
*/

CREATE OR ALTER PROCEDURE dbo.usp_Parse_Kohls_PACKBYSTORE_DetailsReport
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
          AND DetailsReportStatus IS NULL
          AND ISJSON(JSONContent) = 1
          AND JSON_VALUE(JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.ReferencePOType') = 'PACK BY STORE'
        ORDER BY Created ASC;

    OPEN record_cursor;
    FETCH NEXT FROM record_cursor INTO @Id, @JSONContent, @DownloadDate;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Variables for header
        DECLARE @CustomerPO NVARCHAR(100),
                @Company NVARCHAR(100),
                @StartDate DATE,
                @CancelDate DATE,
                @Department NVARCHAR(100),
                @Version INT,
                @HeaderId BIGINT;

        -- Extract header fields
        SET @CustomerPO = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrderNumber');
        SET @Company = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.CompanyCode');
        SET @Department = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.DepartmentNumber');

        -- Parse dates (YYYYMMDD format)
        SET @StartDate = TRY_CONVERT(DATE, JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.RequestedShipDate'), 112);
        SET @CancelDate = TRY_CONVERT(DATE, JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.CancelDate'), 112);

        -- Calculate version number scoped by Company + CustomerPO with datetime precision
        SELECT @Version = COUNT(*) + 1
        FROM Custom88DetailsReportHeader
        WHERE Company = @Company
          AND CustomerPO = @CustomerPO
          AND DateDownloadedDateTime < @DownloadDate;

        -- Insert header (TotalItems and TotalQty will be updated after details are inserted)
        INSERT INTO Custom88DetailsReportHeader (
            Company, POType, CustomerPO, DateDownloaded, DateDownloadedDateTime,
            TotalItems, TotalQty, StartDate, CancelDate, Department, Version, SourceTableId
        )
        VALUES (
            @Company, 'PACK BY STORE', @CustomerPO, CAST(@DownloadDate AS DATE), @DownloadDate,
            0, 0, @StartDate, @CancelDate, @Department, @Version, @Id
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
                TRY_CAST(JSON_VALUE(detail.value, '$.UnitPrice') AS DECIMAL(18,2)) AS UnitPrice,
                TRY_CAST(NULLIF(LTRIM(RTRIM(JSON_VALUE(detail.value, '$.SalesPrice'))), '') AS DECIMAL(18,2)) AS RetailPrice,
                TRY_CAST(NULLIF(LTRIM(RTRIM(JSON_VALUE(detail.value, '$.Pack'))), '') AS INT) AS InnerPack,
                TRY_CAST(NULLIF(LTRIM(RTRIM(JSON_VALUE(detail.value, '$.PackSize'))), '') AS INT) AS QtyPerInnerPack,
                JSON_QUERY(detail.value, '$.DestinationInfo.SDQ') AS SDQ_JSON
            FROM OPENJSON(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails') AS detail
        ),
        -- Unwrap the SDQ - handles both array of segment objects and single object
        SDQ_Segments AS (
            -- Case 1: SDQ is an array of segment objects
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
                sdq_segment.[key] AS SDQ_Segment_Index,
                sdq_segment.value AS SDQ_Segment_JSON
            FROM LineItems li
            CROSS APPLY OPENJSON(li.SDQ_JSON) AS sdq_segment
            WHERE li.SDQ_JSON IS NOT NULL
              AND ISJSON(li.SDQ_JSON) = 1
              AND LEFT(LTRIM(li.SDQ_JSON), 1) = '['

            UNION ALL

            -- Case 2: SDQ is a single object
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
                '0' AS SDQ_Segment_Index,
                li.SDQ_JSON AS SDQ_Segment_JSON
            FROM LineItems li
            WHERE li.SDQ_JSON IS NOT NULL
              AND ISJSON(li.SDQ_JSON) = 1
              AND LEFT(LTRIM(li.SDQ_JSON), 1) = '{'
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
              AND TRY_CAST(SUBSTRING(sdq.[key], 4, 2) AS INT) >= 3
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
                AND s.SDQ_Segment_Index = q.SDQ_Segment_Index
                AND s.SDQ_Index + 1 = q.SDQ_Index
            WHERE s.SDQ_Index % 2 = 1
              AND q.SDQ_Index % 2 = 0
        )
        INSERT INTO Custom88DetailsReportDetail (
            HeaderId, CustomerPO, Style, Color, Size, UPC, SKU,
            Qty, UOM, UnitPrice, RetailPrice, InnerPack, QtyPerInnerPack,
            StoreNumber
        )
        SELECT
            @HeaderId,
            @CustomerPO,
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
            StoreNumber
        FROM StoreAllocations
        WHERE Qty > 0;

        -- Update header with TotalItems and TotalQty
        UPDATE Custom88DetailsReportHeader
        SET TotalItems = (SELECT COUNT(DISTINCT UPC) FROM Custom88DetailsReportDetail WHERE HeaderId = @HeaderId),
            TotalQty = (SELECT ISNULL(SUM(Qty), 0) FROM Custom88DetailsReportDetail WHERE HeaderId = @HeaderId)
        WHERE Id = @HeaderId;

        -- Mark as processed
        UPDATE EDIGatewayInbound
        SET DetailsReportStatus = 'Success',
            DetailsReportProcessed = GETDATE()
        WHERE Id = @Id;

        FETCH NEXT FROM record_cursor INTO @Id, @JSONContent, @DownloadDate;
    END;

    CLOSE record_cursor;
    DEALLOCATE record_cursor;
END;
GO
