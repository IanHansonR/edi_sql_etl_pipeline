/*
    Stored Procedure: usp_Parse_Maurices_StyleColorSizeReport
    Purpose: Parse Maurices EDI 850 orders from EDIGatewayInbound and populate StyleColorSize Report tables

    Source: EDIGatewayInbound (filtered by CompanyCode = 'Maurices', ReferencePOType = 'SO')
    Target: Custom88StyleColorSizeReportHeader, Custom88StyleColorSizeReportDetail

    Prerequisite: DetailsReport must have processed the record first (DetailsReportStatus = 'Success')

    No SDQ parsing required (neither BOM nor regular items use SDQ).
    Qty is extracted directly from PurchaseOrderDetails.Quantity (parent Quantity for BOM items).
    Detail rows are aggregated by Style + Color + Size + UPC + SKU with Qty = SUM of all quantities.
    Amount = UnitPrice * Qty for each detail row.
    UPC = NULL (not available in current Maurices EDI files).

    BOM Handling:
    - Style = BOM-conditional: ProductId if BOM exists, else VendorItemNumber
    - Color is BOM-conditional: COALESCE(BOMDetails[0].ColorDescription, ColorDescription)
    - Size is BOM-conditional: 'CA' if BOM exists, else second word of VendorSizeDescription (e.g. "SMALL REGULAR" -> "REGULAR")
*/

CREATE OR ALTER PROCEDURE dbo.usp_Parse_Maurices_StyleColorSizeReport
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
          AND StyleColorSizeReportStatus IS NULL
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
        SET @StartDate = TRY_CONVERT(DATE, JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.DoNotDeliveryBefore'), 112);
        SET @CompleteDate = TRY_CONVERT(DATE, JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.CancelDate'), 112);

        -- Look up version from DetailsReport header (matched by source record ID)
        SELECT @Version = Version
        FROM Custom88DetailsReportHeader
        WHERE SourceTableId = @Id;

        -- Insert header (TotalOrderQty and TotalOrderAmount will be updated after details are inserted)
        INSERT INTO Custom88StyleColorSizeReportHeader (
            Company, POType, CustomerPO, DateDownloaded,
            TotalOrderQty, TotalOrderAmount, StartDate, CompleteDate, Department, Version
        )
        VALUES (
            @Company, @POType, @CustomerPO, CAST(@DownloadDate AS DATE),
            0, 0, @StartDate, @CompleteDate, @Department, @Version
        );

        SET @HeaderId = SCOPE_IDENTITY();

        -- Parse line items and aggregate by Style + Color + Size + UPC
        -- No SDQ parsing needed for Maurices; Qty comes directly from PurchaseOrderDetails.Quantity
        -- Handle both array and single-object formats for PurchaseOrderDetails
        ;WITH LineItems AS (
            -- Case 1: PurchaseOrderDetails is an array
            SELECT
                -- BOM-conditional Style: ProductId for BOM items, VendorItemNumber + size letter for regular items
                CASE
                    WHEN JSON_QUERY(detail.value, '$.BOMDetails') IS NOT NULL THEN
                        JSON_VALUE(detail.value, '$.ProductId')
                    ELSE
                        JSON_VALUE(detail.value, '$.VendorItemNumber') + ' (' +
                        COALESCE(
                            -- Old format: VendorSizeDescription is array, use [1] (first letter of last word)
                            NULLIF(
                                UPPER(LEFT(LTRIM(REVERSE(LEFT(REVERSE(LTRIM(RTRIM(JSON_VALUE(detail.value, '$.VendorSizeDescription[1]')))), CHARINDEX(' ', REVERSE(LTRIM(RTRIM(JSON_VALUE(detail.value, '$.VendorSizeDescription[1]')))) + ' ') - 1))), 1)),
                                ''
                            ),
                            -- New format: VendorSizeDescription is plain string, use second word
                            NULLIF(
                                UPPER(LEFT(LTRIM(SUBSTRING(
                                    LTRIM(RTRIM(JSON_VALUE(detail.value, '$.VendorSizeDescription'))),
                                    CHARINDEX(' ', LTRIM(RTRIM(JSON_VALUE(detail.value, '$.VendorSizeDescription'))) + ' ') + 1,
                                    LEN(LTRIM(RTRIM(JSON_VALUE(detail.value, '$.VendorSizeDescription'))))
                                )), 1)),
                                ''
                            ),
                            'NULL'
                        ) + ')'
                END AS Style,
                -- BOM-conditional Color: try BOM first, fallback to direct ColorDescription
                COALESCE(
                    (SELECT TOP 1 JSON_VALUE(bom.value, '$.ColorDescription')
                     FROM OPENJSON(JSON_QUERY(detail.value, '$.BOMDetails')) AS bom),
                    JSON_VALUE(detail.value, '$.ColorDescription')
                ) AS Color,
                -- BOM-conditional Size: 'CA' for BOM items
                -- else old format: VendorSizeDescription[1]; new format: second word of VendorSizeDescription
                CASE
                    WHEN JSON_QUERY(detail.value, '$.BOMDetails') IS NOT NULL THEN 'CA'
                    ELSE COALESCE(
                        NULLIF(LTRIM(RTRIM(JSON_VALUE(detail.value, '$.VendorSizeDescription[1]'))), ''),
                        LTRIM(SUBSTRING(
                            LTRIM(RTRIM(JSON_VALUE(detail.value, '$.VendorSizeDescription'))),
                            CHARINDEX(' ', LTRIM(RTRIM(JSON_VALUE(detail.value, '$.VendorSizeDescription'))) + ' ') + 1,
                            LEN(LTRIM(RTRIM(JSON_VALUE(detail.value, '$.VendorSizeDescription'))))
                        ))
                    )
                END AS Size,
                CAST(NULL AS NVARCHAR(100)) AS UPC,
                JSON_VALUE(detail.value, '$.ProductId') AS SKU,
                TRY_CAST(JSON_VALUE(detail.value, '$.UnitPrice') AS DECIMAL(18,2)) AS UnitPrice,
                TRY_CAST(NULLIF(LTRIM(RTRIM(JSON_VALUE(detail.value, '$.RetailPrice'))), '') AS DECIMAL(18,2)) AS RetailPrice,
                CAST(TRY_CAST(JSON_VALUE(detail.value, '$.Quantity') AS FLOAT) AS INT) AS Qty
            FROM OPENJSON(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails') AS detail
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '['

            UNION ALL

            -- Case 2: PurchaseOrderDetails is a single object
            SELECT
                -- BOM-conditional Style: ProductId for BOM items, VendorItemNumber + size letter for regular items
                CASE
                    WHEN JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.BOMDetails') IS NOT NULL THEN
                        JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.ProductId')
                    ELSE
                        JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorItemNumber') + ' (' +
                        COALESCE(
                            -- Old format: VendorSizeDescription is array, use [1] (first letter of last word)
                            NULLIF(
                                UPPER(LEFT(LTRIM(REVERSE(LEFT(REVERSE(LTRIM(RTRIM(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorSizeDescription[1]')))), CHARINDEX(' ', REVERSE(LTRIM(RTRIM(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorSizeDescription[1]')))) + ' ') - 1))), 1)),
                                ''
                            ),
                            -- New format: VendorSizeDescription is plain string, use second word
                            NULLIF(
                                UPPER(LEFT(LTRIM(SUBSTRING(
                                    LTRIM(RTRIM(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorSizeDescription'))),
                                    CHARINDEX(' ', LTRIM(RTRIM(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorSizeDescription'))) + ' ') + 1,
                                    LEN(LTRIM(RTRIM(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorSizeDescription'))))
                                )), 1)),
                                ''
                            ),
                            'NULL'
                        ) + ')'
                END AS Style,
                -- BOM-conditional Color: try BOM first, fallback to direct ColorDescription
                COALESCE(
                    (SELECT TOP 1 JSON_VALUE(bom.value, '$.ColorDescription')
                     FROM OPENJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.BOMDetails')) AS bom),
                    JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.ColorDescription')
                ) AS Color,
                -- BOM-conditional Size: 'CA' for BOM items
                -- else old format: VendorSizeDescription[1]; new format: second word of VendorSizeDescription
                CASE
                    WHEN JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.BOMDetails') IS NOT NULL THEN 'CA'
                    ELSE COALESCE(
                        NULLIF(LTRIM(RTRIM(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorSizeDescription[1]'))), ''),
                        LTRIM(SUBSTRING(
                            LTRIM(RTRIM(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorSizeDescription'))),
                            CHARINDEX(' ', LTRIM(RTRIM(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorSizeDescription'))) + ' ') + 1,
                            LEN(LTRIM(RTRIM(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.VendorSizeDescription'))))
                        ))
                    )
                END AS Size,
                CAST(NULL AS NVARCHAR(100)) AS UPC,
                JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.ProductId') AS SKU,
                TRY_CAST(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.UnitPrice') AS DECIMAL(18,2)) AS UnitPrice,
                TRY_CAST(NULLIF(LTRIM(RTRIM(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.RetailPrice'))), '') AS DECIMAL(18,2)) AS RetailPrice,
                CAST(TRY_CAST(JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails.Quantity') AS FLOAT) AS INT) AS Qty
            WHERE ISJSON(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')) = 1
              AND LEFT(LTRIM(JSON_QUERY(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrder.PurchaseOrderDetails')), 1) = '{'
        )
        -- Insert aggregated detail rows: one per unique Style + Color + Size + UPC
        INSERT INTO Custom88StyleColorSizeReportDetail (
            HeaderId, CustomerPO, Style, Color, Size, UPC, SKU, UnitPrice, RetailPrice, Qty, Amount
        )
        SELECT
            @HeaderId,
            @CustomerPO,
            Style, Color, Size, UPC, SKU, UnitPrice, RetailPrice,
            SUM(Qty) AS Qty,
            UnitPrice * SUM(Qty) AS Amount
        FROM LineItems
        GROUP BY Style, Color, Size, UPC, SKU, UnitPrice, RetailPrice;

        -- Update header with TotalOrderQty and TotalOrderAmount
        UPDATE Custom88StyleColorSizeReportHeader
        SET TotalOrderQty = (SELECT ISNULL(SUM(Qty), 0) FROM Custom88StyleColorSizeReportDetail WHERE HeaderId = @HeaderId),
            TotalOrderAmount = (SELECT ISNULL(SUM(Amount), 0) FROM Custom88StyleColorSizeReportDetail WHERE HeaderId = @HeaderId)
        WHERE Id = @HeaderId;

        -- Mark as processed
        UPDATE EDIGatewayInbound
        SET StyleColorSizeReportStatus = 'Success',
            StyleColorSizeReportProcessed = GETDATE()
        WHERE Id = @Id;

        FETCH NEXT FROM record_cursor INTO @Id, @JSONContent, @DownloadDate;
    END;

    CLOSE record_cursor;
    DEALLOCATE record_cursor;
END;
GO
