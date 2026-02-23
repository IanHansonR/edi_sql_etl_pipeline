/*
    Stored Procedure: usp_Parse_Belk_BK_StoreReport
    Purpose: Parse Belk BK (Bulk) and RL EDI 850 orders from EDIGatewayInbound and populate Store Report tables

    Source: EDIGatewayInbound (filtered by CompanyCode = 'BELK', PurchaseOrderTypeCode IN ('BK', 'RL'))
    Target: Custom88StoreReportHeader, Custom88StoreReportDetail

    Prerequisite: DetailsReport must have processed the record first (DetailsReportStatus = 'Success')

    BK/RL Store Mapping:
    - UPC = PurchaseOrderDetails.ProductId
    - StoreNumber = Parsed from SDQ
    - StoreQty = SUM of all line item quantities for that store (aggregated by StoreNumber)

    Note: BK SDQ is always a single object (not array), so no SDQ_Segments CTE needed.
*/

CREATE OR ALTER PROCEDURE dbo.usp_Parse_Belk_BK_StoreReport
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
          AND StoreReportStatus IS NULL
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
                @StartDate DATE,
                @CompleteDate DATE,
                @Department NVARCHAR(50),
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

        -- Insert header (OrderQtyTotal will be updated after details are inserted)
        INSERT INTO Custom88StoreReportHeader (
            Company, POType, CustomerPo, DateDownloaded,
            OrderQtyTotal, StartDate, CompleteDate, Department, Version
        )
        VALUES (
            @Company, @POType, @CustomerPO, CAST(@DownloadDate AS DATE),
            0, @StartDate, @CompleteDate, @Department, @Version
        );

        SET @HeaderId = SCOPE_IDENTITY();

        -- Parse line items and aggregate by store
        -- Handle both array and single-object formats for PurchaseOrderDetails
        ;WITH LineItems AS (
            -- Case 1: PurchaseOrderDetails is an array
            SELECT
                JSON_VALUE(detail.value, '$.ProductId') AS UPC,
                JSON_QUERY(detail.value, '$.DestinationInfo.SDQ') AS SDQ_JSON
            FROM OPENJSON(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails') AS detail
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '['

            UNION ALL

            -- Case 2: PurchaseOrderDetails is a single object
            SELECT
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.ProductId') AS UPC,
                JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.DestinationInfo.SDQ') AS SDQ_JSON
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '{'
        ),
        -- Handle both array and single-object SDQ formats (RL uses array segments, BK uses single object)
        SDQ_Segments AS (
            -- Case 1: SDQ is an array of segment objects
            SELECT
                li.UPC,
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
                li.UPC,
                '0' AS SDQ_Segment_Index,
                li.SDQ_JSON AS SDQ_Segment_JSON
            FROM LineItems li
            WHERE li.SDQ_JSON IS NOT NULL
              AND ISJSON(li.SDQ_JSON) = 1
              AND LEFT(LTRIM(li.SDQ_JSON), 1) = '{'
        ),
        -- Parse SDQ key-value pairs (handles both array and single-object SDQ)
        SDQ_Parsed AS (
            SELECT
                seg.UPC,
                seg.SDQ_Segment_Index,
                sdq.[key] AS SDQ_Key,
                sdq.value AS SDQ_Value,
                TRY_CAST(SUBSTRING(sdq.[key], 4, 2) AS INT) AS SDQ_Index
            FROM SDQ_Segments seg
            CROSS APPLY OPENJSON(seg.SDQ_Segment_JSON) AS sdq
            WHERE sdq.[key] LIKE 'SDQ%'
              AND TRY_CAST(SUBSTRING(sdq.[key], 4, 2) AS INT) >= 3
        ),
        -- Pair stores with quantities
        StoreAllocations AS (
            SELECT
                s.SDQ_Value AS StoreNumber,
                TRY_CAST(q.SDQ_Value AS INT) AS Qty
            FROM SDQ_Parsed s
            INNER JOIN SDQ_Parsed q
                ON s.UPC = q.UPC
                AND s.SDQ_Segment_Index = q.SDQ_Segment_Index
                AND s.SDQ_Index + 1 = q.SDQ_Index
            WHERE s.SDQ_Index % 2 = 1
              AND q.SDQ_Index % 2 = 0
        )
        -- Insert aggregated detail rows: one per unique store
        INSERT INTO Custom88StoreReportDetail (
            HeaderId, CustomerPO, StoreNumber, StoreQty
        )
        SELECT
            @HeaderId,
            @CustomerPO,
            CAST(StoreNumber AS INT),
            SUM(Qty) AS StoreQty
        FROM StoreAllocations
        WHERE Qty > 0
        GROUP BY StoreNumber;

        -- Update header with OrderQtyTotal
        UPDATE Custom88StoreReportHeader
        SET OrderQtyTotal = (SELECT ISNULL(SUM(StoreQty), 0) FROM Custom88StoreReportDetail WHERE HeaderId = @HeaderId)
        WHERE Id = @HeaderId;

        -- Mark as processed
        UPDATE EDIGatewayInbound
        SET StoreReportStatus = 'Success',
            StoreReportProcessed = GETDATE()
        WHERE Id = @Id;

        FETCH NEXT FROM record_cursor INTO @Id, @JSONContent, @DownloadDate;
    END;

    CLOSE record_cursor;
    DEALLOCATE record_cursor;
END;
GO
