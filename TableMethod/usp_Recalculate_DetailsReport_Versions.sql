/*
    Stored Procedure: usp_Recalculate_DetailsReport_Versions
    Purpose: Recalculate version numbers for all records in Custom88DetailsReportHeader
             based on Company + CustomerPO + DateDownloadedDateTime ordering

    Background:
    Individual DetailsReport sprocs calculate versions during processing using COUNT(*).
    If sprocs execute out of chronological order (e.g., KN sproc runs before SA sproc),
    version numbers may not respect the actual timeline of source data.

    This sproc fixes version numbers by recalculating them based on DateDownloadedDateTime,
    ensuring versions are sequential within each Company + CustomerPO combination
    regardless of sproc execution order.

    Usage:
    Run this after all DetailsReport sprocs have executed:
        EXEC usp_Parse_Kohls_BULK_DetailsReport;
        EXEC usp_Parse_Kohls_PACKBYSTORE_DetailsReport;
        -- ... all other DetailsReport sprocs
        EXEC usp_Recalculate_DetailsReport_Versions;  -- Fix any version ordering issues

    How It Works:
    1. Uses ROW_NUMBER() to calculate correct version for each record
    2. Partitions by Company + CustomerPO (version scope)
    3. Orders by DateDownloadedDateTime (chronological)
    4. Updates only records where CurrentVersion != CorrectVersion
    5. Reports number of records updated

    Properties:
    - Idempotent: Can run multiple times safely
    - Efficient: Only updates records with incorrect versions
    - Order-Independent: Works regardless of sproc execution order
*/

CREATE OR ALTER PROCEDURE dbo.usp_Recalculate_DetailsReport_Versions
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RecordsUpdated INT = 0;

    -- Recalculate all version numbers based on DateDownloadedDateTime
    WITH RankedVersions AS (
        SELECT
            Id,
            Version AS CurrentVersion,
            ROW_NUMBER() OVER (
                PARTITION BY Company, CustomerPO
                ORDER BY DateDownloadedDateTime
            ) AS CorrectVersion
        FROM Custom88DetailsReportHeader
    )
    UPDATE h
    SET Version = r.CorrectVersion
    FROM Custom88DetailsReportHeader h
    INNER JOIN RankedVersions r ON h.Id = r.Id
    WHERE h.Version != r.CorrectVersion;

    SET @RecordsUpdated = @@ROWCOUNT;

    PRINT 'Version recalculation complete. Records updated: ' + CAST(@RecordsUpdated AS VARCHAR(10));
END;
GO
