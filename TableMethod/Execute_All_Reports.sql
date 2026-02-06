/*
    Execute All EDI Report Sprocs in Correct Order

    Execution Pattern:
    1. All DetailsReport sprocs (in any order - versions will be fixed)
    2. Version recalculation (fixes any version ordering issues)
    3. All downstream reports (use corrected versions via SourceTableId lookup)
*/

PRINT '======================================================================';
PRINT 'PHASE 1: Processing DetailsReport sprocs';
PRINT '======================================================================';
PRINT '';

-- Kohl's DetailsReports
PRINT 'Processing Kohls DetailsReports...';
EXEC usp_Parse_Kohls_BULK_DetailsReport;
EXEC usp_Parse_Kohls_PACKBYSTORE_DetailsReport;
EXEC usp_Parse_Kohls_PREPACK_DetailsReport;
PRINT 'Kohls DetailsReports complete.';
PRINT '';

-- Arula DetailsReports
PRINT 'Processing Arula DetailsReports...';
EXEC usp_Parse_Arula_KN_DetailsReport;
EXEC usp_Parse_Arula_SA_DetailsReport;
PRINT 'Arula DetailsReports complete.';
PRINT '';

-- Maurices DetailsReports
PRINT 'Processing Maurices DetailsReports...';
EXEC usp_Parse_Maurices_DetailsReport;
PRINT 'Maurices DetailsReports complete.';
PRINT '';

-- Belk DetailsReports
PRINT 'Processing Belk DetailsReports...';
EXEC usp_Parse_Belk_BK_DetailsReport;
EXEC usp_Parse_Belk_SA_DetailsReport;
PRINT 'Belk DetailsReports complete.';
PRINT '';

PRINT '======================================================================';
PRINT 'PHASE 2: Recalculating version numbers';
PRINT '======================================================================';
PRINT '';

-- Recalculate versions to ensure chronological ordering across all order types
EXEC usp_Recalculate_DetailsReport_Versions;
PRINT '';

PRINT '======================================================================';
PRINT 'PHASE 3: Processing downstream reports (StyleColor, Store, StyleColorSize)';
PRINT '======================================================================';
PRINT '';

-- Kohl's Downstream Reports
PRINT 'Processing Kohls downstream reports...';
EXEC usp_Parse_Kohls_BULK_StyleColorReport;
EXEC usp_Parse_Kohls_BULK_StoreReport;
EXEC usp_Parse_Kohls_BULK_StyleColorSizeReport;

EXEC usp_Parse_Kohls_PACKBYSTORE_StyleColorReport;
EXEC usp_Parse_Kohls_PACKBYSTORE_StoreReport;
EXEC usp_Parse_Kohls_PACKBYSTORE_StyleColorSizeReport;

EXEC usp_Parse_Kohls_PREPACK_StyleColorReport;
EXEC usp_Parse_Kohls_PREPACK_StoreReport;
EXEC usp_Parse_Kohls_PREPACK_StyleColorSizeReport;
PRINT 'Kohls downstream reports complete.';
PRINT '';

-- Arula Downstream Reports
PRINT 'Processing Arula downstream reports...';
EXEC usp_Parse_Arula_KN_StyleColorReport;
EXEC usp_Parse_Arula_KN_StoreReport;
EXEC usp_Parse_Arula_KN_StyleColorSizeReport;

EXEC usp_Parse_Arula_SA_StyleColorReport;
EXEC usp_Parse_Arula_SA_StoreReport;
EXEC usp_Parse_Arula_SA_StyleColorSizeReport;
PRINT 'Arula downstream reports complete.';
PRINT '';

-- Maurices Downstream Reports
PRINT 'Processing Maurices downstream reports...';
EXEC usp_Parse_Maurices_StyleColorReport;
EXEC usp_Parse_Maurices_StoreReport;
EXEC usp_Parse_Maurices_StyleColorSizeReport;
PRINT 'Maurices downstream reports complete.';
PRINT '';

-- Belk Downstream Reports
PRINT 'Processing Belk downstream reports...';
EXEC usp_Parse_Belk_BK_StyleColorReport;
EXEC usp_Parse_Belk_BK_StoreReport;
EXEC usp_Parse_Belk_BK_StyleColorSizeReport;

EXEC usp_Parse_Belk_SA_StyleColorReport;
EXEC usp_Parse_Belk_SA_StoreReport;
EXEC usp_Parse_Belk_SA_StyleColorSizeReport;
PRINT 'Belk downstream reports complete.';
PRINT '';

PRINT '======================================================================';
PRINT 'PHASE 4: Processing DailyReport';
PRINT '======================================================================';
PRINT '';

EXEC usp_Parse_DailyReport;
PRINT 'DailyReport complete.';
PRINT '';

PRINT '======================================================================';
PRINT 'ALL REPORTS PROCESSED SUCCESSFULLY';
PRINT '======================================================================';
