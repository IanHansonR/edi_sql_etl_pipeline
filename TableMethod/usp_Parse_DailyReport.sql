/*
    Stored Procedure: usp_Parse_DailyReport
    Purpose: Parse all EDI 850 orders from EDIGatewayInbound and populate Daily Report table

    Source: EDIGatewayInbound (ALL companies, ALL order types in one pass)
    Target: Custom88DailyReport

    Prerequisite: DetailsReport must have processed the record first (DetailsReportStatus = 'Success')
    This ensures the Version number is available for lookup via SourceTableId.

    Field Mappings:
    - Company = JSON path $.PurchaseOrderHeader.CompanyCode
    - CustomerPO = JSON path $.PurchaseOrderHeader.PurchaseOrderNumber
    - DownloadDate = EDIGatewayInbound.Created (cast to DATE)
    - Version = Lookup from Custom88DetailsReportHeader WHERE SourceTableId = @Id

    NOTE: This is a UNIVERSAL sproc - no CompanyCode filter, no order type filter.
    It processes ALL companies (Kohls, Arula, Maurices, Belk) and ALL order types
    (BULK, PREPACK, PACKBYSTORE, SA, KN, SO, BK, etc.) in a single pass.

    Execution Order: Run AFTER both ALTER scripts have been executed in SSMS
*/

CREATE OR ALTER PROCEDURE dbo.usp_Parse_DailyReport
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Id INT,
            @JSONContent NVARCHAR(MAX),
            @DownloadDate DATETIME;

    -- CURSOR: Universal - processes ALL companies and ALL order types
    -- No CompanyCode filter, no ReferencePOType filter
    -- Prerequisite: DetailsReportStatus = 'Success' (ensures Version is available)
    DECLARE record_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            Id,
            JSONContent,
            Created AS DownloadDate
        FROM EDIGatewayInbound
        WHERE TransactionType = '850'
          AND Status = 'Downloaded'
          AND DailyReportStatus IS NULL
          AND DetailsReportStatus = 'Success'
          AND ISJSON(JSONContent) = 1
        ORDER BY Created ASC;

    OPEN record_cursor;
    FETCH NEXT FROM record_cursor INTO @Id, @JSONContent, @DownloadDate;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @CustomerPO NVARCHAR(100),
                @Company NVARCHAR(100),
                @Version INT;

        -- Extract header fields from JSON
        SET @CustomerPO = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.PurchaseOrderNumber');
        SET @Company = JSON_VALUE(@JSONContent, '$.PurchaseOrderHeader.CompanyCode');

        -- Look up version from DetailsReport header (matched by source record ID)
        SELECT @Version = Version
        FROM Custom88DetailsReportHeader
        WHERE SourceTableId = @Id;

        -- Insert into Daily Report table
        INSERT INTO Custom88DailyReport (
            Company,
            CustomerPO,
            DownloadDate,
            Version
        )
        VALUES (
            @Company,
            @CustomerPO,
            CAST(@DownloadDate AS DATE),
            @Version
        );

        -- Mark as processed
        UPDATE EDIGatewayInbound
        SET DailyReportStatus = 'Success',
            DailyReportProcessed = GETDATE()
        WHERE Id = @Id;

        FETCH NEXT FROM record_cursor INTO @Id, @JSONContent, @DownloadDate;
    END;

    CLOSE record_cursor;
    DEALLOCATE record_cursor;
END;
GO
