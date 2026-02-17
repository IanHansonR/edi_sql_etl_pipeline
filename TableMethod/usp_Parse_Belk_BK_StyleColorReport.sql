/*
    Stored Procedure: usp_Parse_Belk_BK_StyleColorReport
    Purpose: Parse Belk BK (Bulk) and RL EDI 850 orders from EDIGatewayInbound and populate StyleColor Report tables

    Source: EDIGatewayInbound (filtered by CompanyCode = 'BELK', PurchaseOrderTypeCode IN ('BK', 'RL'))
    Target: Custom88StyleColorReportHeader, Custom88StyleColorReportDetail

    Prerequisite: DetailsReport must have processed the record first (DetailsReportStatus = 'Success')

    BK/RL StyleColor Mapping:
    - Style = PurchaseOrderDetails.VendorItemNumber
    - Color = COALESCE(BOMDetails[0].ColorDescription, ColorDescription) -- BOM-conditional
    - QtyOrdered = SUM of SDQ quantities across all stores (aggregated by Style + Color)

    Detail rows are aggregated by Style + Color with QtyOrdered = SUM of all store quantities.
*/

CREATE OR ALTER PROCEDURE dbo.usp_Parse_Belk_BK_StyleColorReport
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Id INT,
            @JSONContent NVARCHAR(MAX),
            @DownloadDate DATETIME,
            @CompanyId nvarchar(100);

    DECLARE record_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            Id,
            JSONContent,
            Created AS DownloadDate
        FROM EDIGatewayInbound
        WHERE CompanyCode = 'BELK'
          AND TransactionType = '850'
          AND Status = 'Downloaded'
          AND StyleColorReportStatus IS NULL
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
                @POType NVARCHAR(100),
                @Version INT,
                @HeaderId BIGINT;

        -- Extract header fields
        SET @CustomerPO = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrderNumber');
        SET @Company = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.CompanyCode');
        SET @POType = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrderTypeCode');

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
            @Company, @POType, @CustomerPO, CAST(@DownloadDate AS DATE),
            @StartDate, @CompleteDate, @Version
        );

        SET @HeaderId = SCOPE_IDENTITY();
        SET @CompanyId = (SELECT TOP 1 CompanyId FROM WMS.dbo.Company WHERE Company = @Company) --new


        -- Parse line items and aggregate by Style + Color + UPC
        -- Handle both array and single-object formats for PurchaseOrderDetails
        ;WITH LineItems AS (
            -- Case 1: PurchaseOrderDetails is an array
            SELECT
                JSON_VALUE(detail.value, '$.LineItemId') AS LineItemId,
                JSON_VALUE(detail.value, '$.VendorItemNumber') AS Style,
                JSON_VALUE(detail.value, '$.ProductId') AS UPC,
                JSON_VALUE(detail.value, '$.ColorDescription') AS ColorDescription,
                JSON_QUERY(detail.value, '$.DestinationInfo.SDQ') AS SDQ_JSON,
                JSON_QUERY(detail.value, '$.BOMDetails') AS BOMDetails_JSON
            FROM OPENJSON(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails') AS detail
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '['

            UNION ALL

            -- Case 2: PurchaseOrderDetails is a single object
            SELECT
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.LineItemId') AS LineItemId,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorItemNumber') AS Style,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.ProductId') AS UPC,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.ColorDescription') AS ColorDescription,
                JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.DestinationInfo.SDQ') AS SDQ_JSON,
                JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.BOMDetails') AS BOMDetails_JSON
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '{'
        ),
        -- Get color with BOM-conditional logic: try BOM first, fall back to ColorDescription
        LineItemsWithColor AS (
            SELECT
                li.LineItemId,
                li.Style,
                li.UPC,
                li.SDQ_JSON,
                li.BOMDetails_JSON,
                -- BOM-conditional Color: try BOM first, fall back to direct ColorDescription
                COALESCE(
                    (SELECT TOP 1 JSON_VALUE(bom.value, '$.ColorDescription')
                     FROM OPENJSON(li.BOMDetails_JSON) AS bom),
                    li.ColorDescription
                ) AS Color
            FROM LineItems li
        ),
        -- Lookup Color and Size from WMS database
        WMS_Lookup AS (
            SELECT
                lic.LineItemId,
                lic.UPC,
                pr.Color AS WMS_Color, --new
                pr.Size AS WMS_Size --new
            FROM LineItemsWithColor lic
            LEFT JOIN WMS.dbo.product p ON p.ProductId = lic.UPC --new
            LEFT JOIN WMS.dbo.product_retail pr ON pr.Pid = p.Pid --new
            WHERE p.CompanyId = @CompanyId --new
        ),
        -- Parse SDQ key-value pairs
        SDQ_Parsed AS (
            SELECT
                lic.LineItemId,
                lic.Style,
                -- BOM-conditional Color with WMS fallback for non-BOM items
                CASE
                    WHEN lic.BOMDetails_JSON IS NOT NULL THEN lic.Color  -- BOM item: use BOM Color
                    ELSE COALESCE(wms.WMS_Color, lic.Color)              -- Non-BOM item: WMS first, fallback to JSON
                END AS Color,
                lic.UPC,
                sdq.[key] AS SDQ_Key,
                sdq.value AS SDQ_Value,
                TRY_CAST(SUBSTRING(sdq.[key], 4, 2) AS INT) AS SDQ_Index
            FROM LineItemsWithColor lic
            LEFT JOIN WMS_Lookup wms ON wms.LineItemId = lic.LineItemId
            CROSS APPLY OPENJSON(lic.SDQ_JSON) AS sdq
            WHERE lic.SDQ_JSON IS NOT NULL
              AND sdq.[key] LIKE 'SDQ%'
              AND TRY_CAST(SUBSTRING(sdq.[key], 4, 2) AS INT) >= 3
        ),
        -- Pair stores with quantities
        StoreAllocations AS (
            SELECT
                s.Style,
                s.Color,
                s.UPC,
                TRY_CAST(q.SDQ_Value AS INT) AS Qty
            FROM SDQ_Parsed s
            INNER JOIN SDQ_Parsed q
                ON s.LineItemId = q.LineItemId
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
