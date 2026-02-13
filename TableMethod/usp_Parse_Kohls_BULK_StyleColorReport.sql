/*
    Stored Procedure: usp_Parse_Kohls_BULK_StyleColorReport
    Purpose: Parse Kohl's BULK EDI 850 orders from EDIGatewayInbound and populate StyleColor Report tables

    Source: EDIGatewayInbound (filtered by CompanyCode = 'Kohls', ReferencePOType = 'BULK')
    Target: Custom88StyleColorReportHeader, Custom88StyleColorReportDetail

    Prerequisite: DetailsReport must have processed the record first (DetailsReportStatus = 'Success')

    Detail rows are aggregated by Style + Color with QtyOrdered = SUM of all store quantities.
*/

CREATE OR ALTER PROCEDURE dbo.usp_Parse_Kohls_BULK_StyleColorReport
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
          AND StyleColorReportStatus IS NULL
          AND DetailsReportStatus = 'Success'
          AND ISJSON(JSONContent) = 1
          AND JSON_VALUE(JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.ReferencePOType') = 'BULK'
        ORDER BY Created ASC;

    OPEN record_cursor;
    FETCH NEXT FROM record_cursor INTO @Id, @JSONContent, @DownloadDate;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @CustomerPO NVARCHAR(100),
                @Company NVARCHAR(100),
                @StartDate DATE,
                @CompleteDate DATE,
                @Version INT,
                @HeaderId BIGINT;

        -- Extract header fields
        SET @CustomerPO = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrderNumber');
        SET @Company = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.CompanyCode');

        -- Parse dates (YYYYMMDD format)
        SET @StartDate = TRY_CONVERT(DATE, JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.RequestedShipDate'), 112);
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
            @Company, 'BULK', @CustomerPO, CAST(@DownloadDate AS DATE),
            @StartDate, @CompleteDate, @Version
        );

        SET @HeaderId = SCOPE_IDENTITY();

        -- Parse line items and aggregate by Style + Color
        -- Handle both array and single-object formats for PurchaseOrderDetails
        ;WITH LineItems AS (
            -- Case 1: PurchaseOrderDetails is an array
            SELECT
                JSON_VALUE(detail.value, '$.VendorItemNumber') AS Style,
                JSON_VALUE(detail.value, '$.ColorDescription') AS Color,
                JSON_VALUE(detail.value, '$.GTIN') AS UPC,
                JSON_QUERY(detail.value, '$.DestinationInfo.SDQ') AS SDQ_JSON
            FROM OPENJSON(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails') AS detail
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '['

            UNION ALL

            -- Case 2: PurchaseOrderDetails is a single object
            SELECT
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorItemNumber') AS Style,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.ColorDescription') AS Color,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.GTIN') AS UPC,
                JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.DestinationInfo.SDQ') AS SDQ_JSON
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '{'
        ),
        -- Parse SDQ key-value pairs
        SDQ_Parsed AS (
            SELECT
                li.Style,
                li.Color,
                li.UPC,
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
                s.Style,
                s.Color,
                TRY_CAST(q.SDQ_Value AS INT) AS Qty
            FROM SDQ_Parsed s
            INNER JOIN SDQ_Parsed q
                ON s.Style = q.Style
                AND s.UPC = q.UPC
                AND s.SDQ_Index + 1 = q.SDQ_Index
            WHERE s.SDQ_Index % 2 = 1
              AND q.SDQ_Index % 2 = 0
        )
        -- Insert aggregated detail rows: one per unique Style + Color
        INSERT INTO Custom88StyleColorReportDetail (
            HeaderId, CustomerPO, Style, Color, QtyOrdered
        )
        SELECT
            @HeaderId,
            @CustomerPO,
            Style,
            Color,
            SUM(Qty) AS QtyOrdered
        FROM StoreAllocations
        WHERE Qty > 0
        GROUP BY Style, Color;

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
