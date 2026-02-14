/*
    Stored Procedure: usp_Parse_Maurices_StoreReport
    Purpose: Parse Maurices EDI 850 orders from EDIGatewayInbound and populate Store Report tables

    Source: EDIGatewayInbound (filtered by CompanyCode = 'Maurices', ReferencePOType = 'SO')
    Target: Custom88StoreReportHeader, Custom88StoreReportDetail

    Prerequisite: DetailsReport must have processed the record first (DetailsReportStatus = 'Success')

    No SDQ parsing required (neither BOM nor regular items use SDQ).
    Qty is extracted directly from PurchaseOrderDetails.Quantity (parent Quantity for BOM items).
    StoreNumber comes from header-level PurchaseOrder.DivisionIdentifier.
    Since there is one store per order, detail will always contain exactly one row per order.

    BOM Handling:
    - BOM items work automatically for this report (no Color/Size fields)
    - Quantity is extracted the same way for both BOM and regular items
*/

CREATE OR ALTER PROCEDURE dbo.usp_Parse_Maurices_StoreReport
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
          AND StoreReportStatus IS NULL
          AND DetailsReportStatus = 'Success'
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
                @CompleteDate DATE,
                @Department NVARCHAR(50),
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

        -- Parse line items and aggregate total quantity for the single store
        -- No SDQ parsing needed for Maurices; Qty comes directly from PurchaseOrderDetails.Quantity
        -- StoreNumber is header-level DivisionIdentifier (one store per order)
        -- Handle both array and single-object formats for PurchaseOrderDetails
        ;WITH LineItems AS (
            -- Case 1: PurchaseOrderDetails is an array
            SELECT
                CAST(TRY_CAST(JSON_VALUE(detail.value, '$.Quantity') AS FLOAT) AS INT) AS Qty
            FROM OPENJSON(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails') AS detail
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '['

            UNION ALL

            -- Case 2: PurchaseOrderDetails is a single object
            SELECT
                CAST(TRY_CAST(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.Quantity') AS FLOAT) AS INT) AS Qty
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '{'
        )
        -- Insert one detail row: single store with total quantity across all line items
        INSERT INTO Custom88StoreReportDetail (
            HeaderId, CustomerPO, StoreNumber, StoreQty
        )
        SELECT
            @HeaderId,
            @CustomerPO,
            CAST(@StoreNumber AS INT),
            SUM(Qty) AS StoreQty
        FROM LineItems;

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
