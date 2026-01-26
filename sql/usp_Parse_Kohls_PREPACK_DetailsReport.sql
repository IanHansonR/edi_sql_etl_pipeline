/*
    Stored Procedure: usp_Parse_Kohls_PREPACK_DetailsReport
    Purpose: Parse Kohl's PREPACK and COMPOUND PREPACK EDI 850 orders from EDIGatewayInbound and populate Details Report tables

    Source: EDIGatewayInbound (filtered by CompanyCode = 'Kohls', ReferencePOType IN ('PREPACK', 'COMPOUND PREPACK'))
    Target: Custom88DetailsReportHeader, Custom88DetailsReportDetail

    Note: This is a simplified version with no logging, minimal error handling, no transactions.

    PREPACK orders have BOMDetails - each line item is a "master" prepack SKU that breaks down into
    component sizes. For each store allocation, we create one detail row per BOM component.
    The detail rows contain the exploded BOM component data (ComponentUPC, ComponentColor, ComponentSize).
*/

CREATE OR ALTER PROCEDURE dbo.usp_Parse_Kohls_PREPACK_DetailsReport
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
          AND ISJSON(JSONContent) = 1
          AND JSON_VALUE(JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.ReferencePOType') IN ('PREPACK', 'COMPOUND PREPACK')
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
        SET @POType = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.ReferencePOType');

        -- Parse dates (YYYYMMDD format)
        SET @StartDate = TRY_CONVERT(DATE, JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.RequestedShipDate'), 112);
        SET @CancelDate = TRY_CONVERT(DATE, JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.CancelDate'), 112);

        -- Calculate version number (count of existing records with earlier download dates + 1)
        SELECT @Version = COUNT(*) + 1
        FROM Custom88DetailsReportHeader
        WHERE CustomerPO = @CustomerPO
          AND DateDownloaded < @DownloadDate;

        -- Insert header (TotalItems and TotalQty will be updated after details are inserted)
        INSERT INTO Custom88DetailsReportHeader (
            Company, POType, CustomerPO, DateDownloaded,
            TotalItems, TotalQty, StartDate, CancelDate, Department, Version
        )
        VALUES (
            @Company, @POType, @CustomerPO, CAST(@DownloadDate AS DATE),
            0, 0, @StartDate, @CancelDate, @Department, @Version
        );

        SET @HeaderId = SCOPE_IDENTITY();

        -- Parse line items with BOM details and store allocations
        -- Handle both array and single-object formats for PurchaseOrderDetails
        ;WITH LineItems AS (
            -- Case 1: PurchaseOrderDetails is an array
            SELECT
                JSON_VALUE(detail.value, '$.LineItemId') AS LineItemId,
                JSON_VALUE(detail.value, '$.VendorItemNumber') AS Style,
                JSON_VALUE(detail.value, '$.GTIN') AS MasterUPC,
                JSON_VALUE(detail.value, '$.BuyerPartNumber') AS SKU,
                JSON_VALUE(detail.value, '$.UOMTypeCode') AS UOM,
                TRY_CAST(JSON_VALUE(detail.value, '$.UnitPrice') AS FLOAT) AS MasterUnitPrice,
                TRY_CAST(JSON_VALUE(detail.value, '$.SalesPrice') AS FLOAT) AS MasterRetailPrice,
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
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.GTIN') AS MasterUPC,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.BuyerPartNumber') AS SKU,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.UOMTypeCode') AS UOM,
                TRY_CAST(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.UnitPrice') AS FLOAT) AS MasterUnitPrice,
                TRY_CAST(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.SalesPrice') AS FLOAT) AS MasterRetailPrice,
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
                TRY_CAST(JSON_VALUE(bom.value, '$.Quantity') AS INT) AS ComponentQtyPerPack
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
        -- Qty = PackQty * ComponentQtyPerPack (packs x units per pack)
        INSERT INTO Custom88DetailsReportDetail (
            HeaderId, Style, Color, Size, UPC, SKU,
            Qty, UOM, UnitPrice, RetailPrice, InnerPack, QtyPerInnerPack,
            StoreNumber
        )
        SELECT
            @HeaderId,
            Style,
            ComponentColor,
            ComponentSize,
            ComponentUPC,
            SKU,
            PackQty * ComponentQtyPerPack AS Qty,
            UOM,
            MasterUnitPrice,
            MasterRetailPrice,
            InnerPack,
            QtyPerInnerPack,
            StoreNumber
        FROM StoreAllocations
        WHERE PackQty > 0;

        -- Update header with TotalItems and TotalQty
        UPDATE Custom88DetailsReportHeader
        SET TotalItems = (SELECT COUNT(*) FROM Custom88DetailsReportDetail WHERE HeaderId = @HeaderId),
            TotalQty = (SELECT ISNULL(SUM(Qty), 0) FROM Custom88DetailsReportDetail WHERE HeaderId = @HeaderId)
        WHERE Id = @HeaderId;

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
