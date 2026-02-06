/*
    Stored Procedure: usp_Parse_Maurices_DetailsReport
    Purpose: Parse Maurices EDI 850 orders from EDIGatewayInbound and populate Details Report tables

    Source: EDIGatewayInbound (filtered by CompanyCode = 'Maurices', ReferencePOType = 'SO')
    Target: Custom88DetailsReportHeader, Custom88DetailsReportDetail

    Maurices Detail Mapping:
    - Style = PurchaseOrderDetails.VendorItemNumber
    - Color = PurchaseOrderDetails.ColorDescription
    - Size = PurchaseOrderDetails.VendorSizeDescription[1] (2nd element of JSON array)
    - UPC = NULL (not available in current Maurices EDI files)
    - SKU = PurchaseOrderDetails.ProductId
    - InnerPack = NULL (not applicable for Maurices)
    - QtyPerInnerPack = NULL (not applicable for Maurices)
    - Qty = PurchaseOrderDetails.Quantity (decimal string, FLOAT->INT conversion)
    - StoreNumber = PurchaseOrder.DivisionIdentifier (header-level, one store per order)
    - No SDQ parsing required
    - TotalItems = COUNT(DISTINCT SKU) since UPC is NULL
*/

CREATE OR ALTER PROCEDURE dbo.usp_Parse_Maurices_DetailsReport
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
        WHERE CompanyCode = 'Maurices'
          AND TransactionType = '850'
          AND Status = 'Downloaded'
          AND DetailsReportStatus IS NULL
          AND ISJSON(JSONContent) = 1
          AND JSON_VALUE(JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.ReferencePOType') = 'SO'
        ORDER BY Created ASC;

    OPEN record_cursor;
    FETCH NEXT FROM record_cursor INTO @Id, @JSONContent, @DownloadDate;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @CustomerPO NVARCHAR(100),
                @Company NVARCHAR(100),
                @StartDate DATE,
                @CancelDate DATE,
                @Department NVARCHAR(100),
                @POType NVARCHAR(100),
                @StoreNumber NVARCHAR(100),
                @Version INT,
                @HeaderId BIGINT;

        -- Extract header fields
        SET @CustomerPO = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrderNumber');
        SET @Company = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.CompanyCode');
        SET @Department = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.DepartmentNumber');
        SET @POType = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.ReferencePOType');
        SET @StoreNumber = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.DivisionIdentifier');

        -- Parse dates (YYYYMMDD format)
        SET @StartDate = TRY_CONVERT(DATE, JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.DoNotDeliveryBefore'), 112);
        SET @CancelDate = TRY_CONVERT(DATE, JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.CancelDate'), 112);

        -- Calculate version number scoped by Company + CustomerPO with datetime precision
        SELECT @Version = COUNT(*) + 1
        FROM Custom88DetailsReportHeader
        WHERE Company = @Company
          AND CustomerPO = @CustomerPO
          AND DateDownloadedDateTime < @DownloadDate;

        -- Insert header
        INSERT INTO Custom88DetailsReportHeader (
            Company, POType, CustomerPO, DateDownloaded, DateDownloadedDateTime,
            TotalItems, TotalQty, StartDate, CancelDate, Department, Version, SourceTableId
        )
        VALUES (
            @Company, @POType, @CustomerPO, CAST(@DownloadDate AS DATE), @DownloadDate,
            0, 0, @StartDate, @CancelDate, @Department, @Version, @Id
        );

        SET @HeaderId = SCOPE_IDENTITY();

        -- Parse line items directly (no SDQ parsing needed for Maurices)
        -- Qty comes from PurchaseOrderDetails.Quantity, StoreNumber from header-level DivisionIdentifier
        -- Handle both array and single-object formats for PurchaseOrderDetails
        ;WITH LineItems AS (
            -- Case 1: PurchaseOrderDetails is an array
            SELECT
                JSON_VALUE(detail.value, '$.VendorItemNumber') AS Style,
                JSON_VALUE(detail.value, '$.ColorDescription') AS Color,
                JSON_VALUE(detail.value, '$.VendorSizeDescription[1]') AS Size,
                CAST(NULL AS NVARCHAR(100)) AS UPC,
                JSON_VALUE(detail.value, '$.ProductId') AS SKU,
                JSON_VALUE(detail.value, '$.UOMTypeCode') AS UOM,
                TRY_CAST(JSON_VALUE(detail.value, '$.UnitPrice') AS DECIMAL(18,2)) AS UnitPrice,
                TRY_CAST(NULLIF(LTRIM(RTRIM(JSON_VALUE(detail.value, '$.RetailPrice'))), '') AS DECIMAL(18,2)) AS RetailPrice,
                CAST(NULL AS INT) AS InnerPack,
                CAST(NULL AS INT) AS QtyPerInnerPack,
                CAST(TRY_CAST(JSON_VALUE(detail.value, '$.Quantity') AS FLOAT) AS INT) AS Qty
            FROM OPENJSON(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails') AS detail
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '['

            UNION ALL

            -- Case 2: PurchaseOrderDetails is a single object
            SELECT
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorItemNumber') AS Style,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.ColorDescription') AS Color,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorSizeDescription[1]') AS Size,
                CAST(NULL AS NVARCHAR(100)) AS UPC,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.ProductId') AS SKU,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.UOMTypeCode') AS UOM,
                TRY_CAST(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.UnitPrice') AS DECIMAL(18,2)) AS UnitPrice,
                TRY_CAST(NULLIF(LTRIM(RTRIM(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.RetailPrice'))), '') AS DECIMAL(18,2)) AS RetailPrice,
                CAST(NULL AS INT) AS InnerPack,
                CAST(NULL AS INT) AS QtyPerInnerPack,
                CAST(TRY_CAST(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.Quantity') AS FLOAT) AS INT) AS Qty
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '{'
        )
        INSERT INTO Custom88DetailsReportDetail (
            HeaderId, Style, Color, Size, UPC, SKU,
            Qty, UOM, UnitPrice, RetailPrice, InnerPack, QtyPerInnerPack,
            StoreNumber
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
            @StoreNumber
        FROM LineItems;

        -- Update header with TotalItems (COUNT DISTINCT SKU since UPC is NULL) and TotalQty
        UPDATE Custom88DetailsReportHeader
        SET TotalItems = (SELECT COUNT(DISTINCT SKU) FROM Custom88DetailsReportDetail WHERE HeaderId = @HeaderId),
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
