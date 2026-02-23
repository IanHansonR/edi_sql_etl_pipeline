    /*
    Stored Procedure: usp_Parse_Belk_BK_DetailsReport
    Purpose: Parse Belk BK (Bulk) and RL EDI 850 orders from EDIGatewayInbound and populate Details Report tables

    Source: EDIGatewayInbound (filtered by CompanyCode = 'BELK', PurchaseOrderTypeCode IN ('BK', 'RL'))
    Target: Custom88DetailsReportHeader, Custom88DetailsReportDetail

    Note: BK and RL orders are HYBRID - can contain BOTH BOM items (prepack-style) AND single items in the SAME purchase order.

    BK/RL Detail Mapping:
    - UPC = PurchaseOrderDetails.ProductId
    - SKU = NULL (not BuyerPartNumber per user requirement)
    - Style = VendorItemNumber + ' P' + [BOM count] when BOM exists, else VendorItemNumber
    - Color = COALESCE(BOMDetails[0].ColorDescription, ColorDescription) -- BOM-conditional
    - Size = 'PPK' if BOM exists, else VendorSizeDescription[1] -- BOM-conditional, array index
    - UnitPrice = PurchaseOrderDetails.UnitPrice
    - RetailPrice = PurchaseOrderDetails.SalesPrice
    - UOM = PurchaseOrderDetails.UOMTypeCode
    - Qty = Parsed from SDQ
    - InnerPack = Pack if BOM exists, else NULL -- BOM-conditional
    - QtyPerInnerPack = PackSize if BOM exists, else NULL -- BOM-conditional
    - StoreNumber = Parsed from SDQ
*/

CREATE OR ALTER PROCEDURE dbo.usp_Parse_Belk_BK_DetailsReport
AS
BEGIN
    SET NOCOUNT ON;

    -- Process each unprocessed Belk BK record
    DECLARE @Id INT,
            @JSONContent NVARCHAR(MAX),
            @DownloadDate DATETIME,
            @CompanyId nvarchar(100); --new

    DECLARE record_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            Id,
            JSONContent,
            Created AS DownloadDate
        FROM EDIGatewayInbound
        WHERE CompanyCode = 'BELK'
          AND TransactionType = '850'
          AND Status = 'Downloaded'
          AND DetailsReportStatus IS NULL
          AND ISJSON(JSONContent) = 1
          AND JSON_VALUE(JSONContent, '$.PurchaseOrderHeader.PurchaseOrderTypeCode') IN ('BK', 'RL')
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
            @Company, @POType, @CustomerPO, CAST(@DownloadDate AS DATE), @DownloadDate,
            0, 0, @StartDate, @CancelDate, @Department, @Version, @Id
        );

        SET @HeaderId = SCOPE_IDENTITY();
        SET @CompanyId = (SELECT TOP 1 CompanyId FROM WMS.dbo.Company WHERE Company = @Company) --new

        -- Parse line items from PurchaseOrderDetails
        -- Handle both array and single-object formats for PurchaseOrderDetails
        ;WITH LineItems AS (
            -- Case 1: PurchaseOrderDetails is an array
            SELECT
                JSON_VALUE(detail.value, '$.LineItemId') AS LineItemId,
                JSON_VALUE(detail.value, '$.VendorItemNumber') AS Style,
                JSON_VALUE(detail.value, '$.ProductId') AS UPC,
                CAST(NULL AS NVARCHAR(100)) AS SKU,  -- SKU is always NULL per user requirement
                JSON_VALUE(detail.value, '$.UOMTypeCode') AS UOM,
                TRY_CAST(JSON_VALUE(detail.value, '$.UnitPrice') AS DECIMAL(18,2)) AS UnitPrice,
                TRY_CAST(NULLIF(LTRIM(RTRIM(JSON_VALUE(detail.value, '$.SalesPrice'))), '') AS DECIMAL(18,2)) AS RetailPrice,
                JSON_VALUE(detail.value, '$.ColorDescription') AS ColorDescription,
                JSON_VALUE(detail.value, '$.VendorSizeDescription[1]') AS VendorSizeDescription,  -- Array index [1] for 2nd element
                TRY_CAST(NULLIF(LTRIM(RTRIM(JSON_VALUE(detail.value, '$.Pack'))), '') AS INT) AS Pack,
                TRY_CAST(NULLIF(LTRIM(RTRIM(JSON_VALUE(detail.value, '$.PackSize'))), '') AS INT) AS PackSize,
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
                CAST(NULL AS NVARCHAR(100)) AS SKU,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.UOMTypeCode') AS UOM,
                TRY_CAST(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.UnitPrice') AS DECIMAL(18,2)) AS UnitPrice,
                TRY_CAST(NULLIF(LTRIM(RTRIM(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.SalesPrice'))), '') AS DECIMAL(18,2)) AS RetailPrice,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.ColorDescription') AS ColorDescription,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorSizeDescription[1]') AS VendorSizeDescription,
                TRY_CAST(NULLIF(LTRIM(RTRIM(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.Pack'))), '') AS INT) AS Pack,
                TRY_CAST(NULLIF(LTRIM(RTRIM(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.PackSize'))), '') AS INT) AS PackSize,
                JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.DestinationInfo.SDQ') AS SDQ_JSON,
                JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.BOMDetails') AS BOMDetails_JSON
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '{'
        ),
        -- Get color with BOM-conditional logic: try BOM first, fall back to ColorDescription
        LineItemsWithColor AS (
            SELECT
                li.LineItemId,
                CASE
                    WHEN li.BOMDetails_JSON IS NOT NULL THEN
                        li.Style + ' P' + CAST((SELECT SUM(TRY_CAST(JSON_VALUE(bom.value, '$.Quantity') AS INT))
                                                FROM OPENJSON(li.BOMDetails_JSON) AS bom) AS NVARCHAR(10))
                    ELSE
                        li.Style
                END AS Style,
                li.UPC,
                li.SKU,
                li.UOM,
                li.UnitPrice,
                li.RetailPrice,
                li.VendorSizeDescription,
                li.Pack,
                li.PackSize,
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

        -- Handle both array and single-object SDQ formats (RL uses array segments, BK uses single object)
        SDQ_Segments AS (
            -- Case 1: SDQ is an array of segment objects
            SELECT
                lic.LineItemId,
                lic.Style,
                lic.UPC,
                lic.SKU,
                lic.UOM,
                lic.UnitPrice,
                lic.RetailPrice,
                lic.VendorSizeDescription,
                lic.Pack,
                lic.PackSize,
                lic.BOMDetails_JSON,
                lic.Color,
                sdq_segment.[key] AS SDQ_Segment_Index,
                sdq_segment.value AS SDQ_Segment_JSON
            FROM LineItemsWithColor lic
            CROSS APPLY OPENJSON(lic.SDQ_JSON) AS sdq_segment
            WHERE lic.SDQ_JSON IS NOT NULL
              AND ISJSON(lic.SDQ_JSON) = 1
              AND LEFT(LTRIM(lic.SDQ_JSON), 1) = '['

            UNION ALL

            -- Case 2: SDQ is a single object
            SELECT
                lic.LineItemId,
                lic.Style,
                lic.UPC,
                lic.SKU,
                lic.UOM,
                lic.UnitPrice,
                lic.RetailPrice,
                lic.VendorSizeDescription,
                lic.Pack,
                lic.PackSize,
                lic.BOMDetails_JSON,
                lic.Color,
                '0' AS SDQ_Segment_Index,
                lic.SDQ_JSON AS SDQ_Segment_JSON
            FROM LineItemsWithColor lic
            WHERE lic.SDQ_JSON IS NOT NULL
              AND ISJSON(lic.SDQ_JSON) = 1
              AND LEFT(LTRIM(lic.SDQ_JSON), 1) = '{'
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

            --LEFT JOIN WMS.dbo.product_retail pr ON pr.productid = lic.UPC
            --LEFT JOIN WMS.dbo.product p ON p.Pid = pr.Pid
        ),
        -- Parse SDQ key-value pairs (handles both array and single-object SDQ)
        SDQ_Parsed AS (
            SELECT
                seg.*,
                wms.WMS_Color,
                wms.WMS_Size,
                sdq.[key] AS SDQ_Key,
                sdq.value AS SDQ_Value,
                TRY_CAST(SUBSTRING(sdq.[key], 4, 2) AS INT) AS SDQ_Index
            FROM SDQ_Segments seg
            LEFT JOIN WMS_Lookup wms ON wms.LineItemId = seg.LineItemId
            CROSS APPLY OPENJSON(seg.SDQ_Segment_JSON) AS sdq
            WHERE sdq.[key] LIKE 'SDQ%'
              AND TRY_CAST(SUBSTRING(sdq.[key], 4, 2) AS INT) >= 3
        ),
        -- Pair stores (odd index) with quantities (even index)
        StoreAllocations AS (
            SELECT
                s.LineItemId,
                s.Style,
                s.UPC,
                s.SKU,
                s.UOM,
                s.UnitPrice,
                s.RetailPrice,
                -- BOM-conditional InnerPack: Pack if BOM exists, else NULL
                CASE
                    WHEN s.BOMDetails_JSON IS NOT NULL THEN s.Pack
                    ELSE NULL
                END AS InnerPack,
                -- BOM-conditional QtyPerInnerPack: PackSize if BOM exists, else NULL
                CASE
                    WHEN s.BOMDetails_JSON IS NOT NULL THEN s.PackSize
                    ELSE NULL
                END AS QtyPerInnerPack,
                -- BOM-conditional Color with WMS fallback for non-BOM items
                CASE
                    WHEN s.BOMDetails_JSON IS NOT NULL THEN s.Color  -- BOM item: use BOM Color
                    ELSE COALESCE(s.WMS_Color, s.Color)              -- Non-BOM item: WMS first, fallback to JSON
                END AS Color,
                -- BOM-conditional Size with WMS fallback for non-BOM items
                CASE
                    WHEN s.BOMDetails_JSON IS NOT NULL THEN 'PPK'    -- BOM item: always 'PPK'
                    ELSE COALESCE(s.WMS_Size, s.VendorSizeDescription)  -- Non-BOM item: WMS first, fallback to JSON
                END AS Size,
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
        -- Insert detail rows: one per UPC per store (no aggregation)
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
