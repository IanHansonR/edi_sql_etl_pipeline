/*
    Stored Procedure: usp_Parse_Kohls_PREPACK
    Purpose: Parse Kohl's PREPACK and COMPOUND PREPACK EDI 850 orders from EDIGatewayInbound and populate reporting tables

    Source: EDIGatewayInbound (filtered by CompanyCode = 'Kohls', ReferencePOType IN ('PREPACK', 'COMPOUND PREPACK'))
    Target: EDI_Report_Header, EDI_Report_Detail, EDI_Report_BOM_Component

    Note: This is a simplified version with no logging, minimal error handling, no transactions.

    PREPACK orders have BOMDetails - each line item is a "master" prepack SKU that breaks down into
    component sizes. For each store allocation, we create one detail row per BOM component.
*/

CREATE OR ALTER PROCEDURE dbo.usp_Parse_Kohls_PREPACK
AS
BEGIN
    SET NOCOUNT ON;

    -- Process each unprocessed Kohl's PREPACK/COMPOUND PREPACK record
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
          AND JSON_VALUE(JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.ReferencePOType') IN ('PREPACK', 'COMPOUND PREPACK')
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
                @POType VARCHAR(50),
                @Version INT,
                @HeaderId INT;

        -- Extract header fields
        SET @CustomerPO = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrderNumber');
        SET @Company = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.CompanyCode');
        SET @Department = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.DepartmentNumber');
        SET @POType = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.ReferencePOType');

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
            @DownloadDate, @OrderDate, @POType, @Version, @Id, GETDATE()
        );

        SET @HeaderId = SCOPE_IDENTITY();

        -- Parse line items with BOM details and store allocations
        -- Handle both array and single-object formats for PurchaseOrderDetails
        ;WITH LineItems AS (
            -- Case 1: PurchaseOrderDetails is an array
            SELECT
                JSON_VALUE(detail.value, '$.LineItemId') AS LineItemId,
                JSON_VALUE(detail.value, '$.VendorItemNumber') AS Style,
                JSON_VALUE(detail.value, '$.ColorDescription') AS Color,
                JSON_VALUE(detail.value, '$.SizeDescription') AS Size,
                JSON_VALUE(detail.value, '$.GTIN') AS MasterUPC,
                JSON_VALUE(detail.value, '$.BuyerPartNumber') AS SKU,
                JSON_VALUE(detail.value, '$.UOMTypeCode') AS UOM,
                TRY_CAST(JSON_VALUE(detail.value, '$.UnitPrice') AS DECIMAL(18,4)) AS MasterUnitPrice,
                TRY_CAST(JSON_VALUE(detail.value, '$.SalesPrice') AS DECIMAL(18,4)) AS MasterRetailPrice,
                TRY_CAST(NULLIF(JSON_VALUE(detail.value, '$.Pack'), '') AS INT) AS InnerPack,
                TRY_CAST(NULLIF(JSON_VALUE(detail.value, '$.PackSize'), '') AS INT) AS QtyPerInnerPack,
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
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.ColorDescription') AS Color,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.SizeDescription') AS Size,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.GTIN') AS MasterUPC,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.BuyerPartNumber') AS SKU,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.UOMTypeCode') AS UOM,
                TRY_CAST(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.UnitPrice') AS DECIMAL(18,4)) AS MasterUnitPrice,
                TRY_CAST(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.SalesPrice') AS DECIMAL(18,4)) AS MasterRetailPrice,
                TRY_CAST(NULLIF(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.Pack'), '') AS INT) AS InnerPack,
                TRY_CAST(NULLIF(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.PackSize'), '') AS INT) AS QtyPerInnerPack,
                JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.DestinationInfo.SDQ') AS SDQ_JSON,
                JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.BOMDetails') AS BOMDetails_JSON
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '{'
        ),
        -- Parse BOM components for each line item
        BOMComponents AS (
            SELECT
                li.LineItemId,
                li.Style,
                li.MasterUPC,
                li.SKU,
                li.UOM,
                li.MasterUnitPrice,
                li.MasterRetailPrice,
                li.InnerPack,
                li.QtyPerInnerPack,
                li.SDQ_JSON,
                JSON_VALUE(bom.value, '$.GTIN') AS ComponentUPC,
                JSON_VALUE(bom.value, '$.ColorDescription') AS ComponentColor,
                JSON_VALUE(bom.value, '$.SizeDescription') AS ComponentSize,
                TRY_CAST(JSON_VALUE(bom.value, '$.Quantity') AS INT) AS ComponentQtyPerPack,
                TRY_CAST(JSON_VALUE(bom.value, '$.UnitPrice') AS DECIMAL(18,4)) AS ComponentUnitPrice
            FROM LineItems li
            CROSS APPLY OPENJSON(li.BOMDetails_JSON) AS bom
            WHERE li.BOMDetails_JSON IS NOT NULL
        ),
        -- Parse SDQ key-value pairs for each line item
        SDQ_Parsed AS (
            SELECT
                bc.*,
                sdq.[key] AS SDQ_Key,
                sdq.value AS SDQ_Value,
                TRY_CAST(SUBSTRING(sdq.[key], 4, 2) AS INT) AS SDQ_Index
            FROM BOMComponents bc
            CROSS APPLY OPENJSON(bc.SDQ_JSON) AS sdq
            WHERE bc.SDQ_JSON IS NOT NULL
              AND sdq.[key] LIKE 'SDQ%'
              AND TRY_CAST(SUBSTRING(sdq.[key], 4, 2) AS INT) >= 3
        ),
        -- Pair stores (odd index) with quantities (even index)
        -- For PREPACK, the SDQ qty is number of PACKS, not units
        StoreAllocations AS (
            SELECT
                s.LineItemId,
                s.Style,
                s.MasterUPC,
                s.SKU,
                s.UOM,
                s.MasterUnitPrice,
                s.MasterRetailPrice,
                s.InnerPack,
                s.QtyPerInnerPack,
                s.ComponentUPC,
                s.ComponentColor,
                s.ComponentSize,
                s.ComponentQtyPerPack,
                s.ComponentUnitPrice,
                s.SDQ_Value AS StoreNumber,
                TRY_CAST(q.SDQ_Value AS INT) AS PackQty
            FROM SDQ_Parsed s
            INNER JOIN SDQ_Parsed q
                ON s.LineItemId = q.LineItemId
                AND s.MasterUPC = q.MasterUPC
                AND s.ComponentUPC = q.ComponentUPC
                AND s.SDQ_Index + 1 = q.SDQ_Index
            WHERE s.SDQ_Index % 2 = 1
              AND q.SDQ_Index % 2 = 0
        )
        -- Insert detail rows: one per BOM component per store
        -- Qty = PackQty * ComponentQtyPerPack (packs × units per pack)
        INSERT INTO EDI_Report_Detail (
            HeaderId, Style, Color, Size, UPC, SKU,
            Qty, UOM, UnitPrice, RetailPrice, InnerPack, QtyPerInnerPack,
            DC, StoreNumber, IsBOM
        )
        SELECT
            @HeaderId,
            Style,
            ComponentColor,
            ComponentSize,
            ComponentUPC,
            SKU,
            PackQty * ComponentQtyPerPack AS Qty,  -- Total units = packs × units per pack
            UOM,
            MasterUnitPrice,      -- Line item level unit price
            MasterRetailPrice,    -- Line item level retail price (from SalesPrice)
            InnerPack,
            QtyPerInnerPack,
            NULL AS DC,
            StoreNumber,
            1 AS IsBOM  -- PREPACK orders are BOM
        FROM StoreAllocations
        WHERE PackQty > 0;

        -- Insert BOM component records
        -- Need to get the DetailId for each inserted detail row
        -- Using a separate insert with OUTPUT or matching after the fact
        ;WITH LineItems AS (
            SELECT
                JSON_VALUE(detail.value, '$.LineItemId') AS LineItemId,
                JSON_VALUE(detail.value, '$.GTIN') AS MasterUPC,
                JSON_QUERY(detail.value, '$.DestinationInfo.SDQ') AS SDQ_JSON,
                JSON_QUERY(detail.value, '$.BOMDetails') AS BOMDetails_JSON
            FROM OPENJSON(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails') AS detail
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '['

            UNION ALL

            SELECT
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.LineItemId') AS LineItemId,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.GTIN') AS MasterUPC,
                JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.DestinationInfo.SDQ') AS SDQ_JSON,
                JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.BOMDetails') AS BOMDetails_JSON
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '{'
        ),
        BOMComponents AS (
            SELECT
                li.LineItemId,
                li.MasterUPC,
                li.SDQ_JSON,
                JSON_VALUE(bom.value, '$.GTIN') AS ComponentUPC,
                JSON_VALUE(bom.value, '$.SizeDescription') AS ComponentSize,
                TRY_CAST(JSON_VALUE(bom.value, '$.Quantity') AS INT) AS ComponentQtyPerPack,
                TRY_CAST(JSON_VALUE(bom.value, '$.UnitPrice') AS DECIMAL(18,4)) AS ComponentUnitPrice
            FROM LineItems li
            CROSS APPLY OPENJSON(li.BOMDetails_JSON) AS bom
            WHERE li.BOMDetails_JSON IS NOT NULL
        ),
        SDQ_Parsed AS (
            SELECT
                bc.*,
                sdq.[key] AS SDQ_Key,
                sdq.value AS SDQ_Value,
                TRY_CAST(SUBSTRING(sdq.[key], 4, 2) AS INT) AS SDQ_Index
            FROM BOMComponents bc
            CROSS APPLY OPENJSON(bc.SDQ_JSON) AS sdq
            WHERE bc.SDQ_JSON IS NOT NULL
              AND sdq.[key] LIKE 'SDQ%'
              AND TRY_CAST(SUBSTRING(sdq.[key], 4, 2) AS INT) >= 3
        ),
        StoreAllocations AS (
            SELECT
                s.LineItemId,
                s.MasterUPC,
                s.ComponentUPC,
                s.ComponentSize,
                s.ComponentQtyPerPack,
                s.ComponentUnitPrice,
                s.SDQ_Value AS StoreNumber,
                TRY_CAST(q.SDQ_Value AS INT) AS PackQty
            FROM SDQ_Parsed s
            INNER JOIN SDQ_Parsed q
                ON s.LineItemId = q.LineItemId
                AND s.MasterUPC = q.MasterUPC
                AND s.ComponentUPC = q.ComponentUPC
                AND s.SDQ_Index + 1 = q.SDQ_Index
            WHERE s.SDQ_Index % 2 = 1
              AND q.SDQ_Index % 2 = 0
        )
        INSERT INTO EDI_Report_BOM_Component (
            DetailId, ComponentSKU, ComponentSize, ComponentQty, ComponentUnitPrice, ComponentRetailPrice
        )
        SELECT
            d.Id AS DetailId,
            sa.ComponentUPC AS ComponentSKU,
            sa.ComponentSize,
            sa.PackQty * sa.ComponentQtyPerPack AS ComponentQty,
            sa.ComponentUnitPrice,
            NULL AS ComponentRetailPrice
        FROM StoreAllocations sa
        INNER JOIN EDI_Report_Detail d
            ON d.HeaderId = @HeaderId
            AND d.UPC = sa.ComponentUPC
            AND d.StoreNumber = sa.StoreNumber
        WHERE sa.PackQty > 0;

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
