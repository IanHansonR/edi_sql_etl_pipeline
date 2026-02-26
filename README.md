# EDI Purchase Order ELT Pipeline

A production ELT pipeline built in T-SQL that ingests, parses, and structures EDI 850 (Purchase Order) data for a wholesale apparel business. The system processes purchase orders from four major retail partners across multiple order types, populates business-ready reporting tables, and surfaces versioned order history for daily operational use.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Pipeline Stages](#pipeline-stages)
- [Data Sources & Order Types](#data-sources--order-types)
- [Report Types](#report-types)
- [Key Engineering Challenges](#key-engineering-challenges)
- [Project Structure](#project-structure)
- [Schema Design](#schema-design)
- [Orchestration](#orchestration)
- [Prerequisites](#prerequisites)

---

## Overview

Retail partners transmit purchase orders via EDI (Electronic Data Interchange). These transactions are translated from raw X12 EDI into JSON by an upstream integration layer (CData Arc) and stored in an internal staging table (`EDIGatewayInbound`). This pipeline picks up from there.

**What this pipeline does:**

- Filters and routes each incoming JSON record by company, transaction type, and order type
- Parses complex, non-uniform JSON structures into normalized, report-ready tables
- Generates five distinct report types per company/order type combination
- Tracks a full version history of every purchase order transmission, since most POs are sent multiple times with amendments
- Exposes the latest version of each report through database views for clean daily user access
- Surfaces processing errors through a dedicated monitoring view
- Runs automatically on an hourly SQL Server Agent Job with phase-aware execution ordering

**Scale:**
- **4 retail partners**: Kohl's, Arula, Maurices, Belk
- **8 distinct company/order-type combinations**
- **37 stored procedures** covering all parsing and reporting logic
- **5 report types** per company/order type
- **Hourly automated execution** via SQL Server Agent

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     UPSTREAM (Pre-existing)                     │
│   Raw EDI X12  ──►  CData Arc (translation)  ──►  JSON stored  │
│                                                  in staging DB  │
└───────────────────────────────────┬─────────────────────────────┘
                                    │  EDIGatewayInbound
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                     THIS PIPELINE                               │
│                                                                 │
│  Phase 1 ── DetailsReport sprocs (8 sprocs, all companies)      │
│      │       Filter → Parse JSON → Insert header + detail rows  │
│      │       Compute version number per PO                      │
│      ▼                                                          │
│  Phase 2 ── Version Recalculation                               │
│      │       Corrects version ordering after parallel inserts   │
│      ▼                                                          │
│  Phase 3 ── Downstream Reports (28 sprocs)                      │
│      │       StyleColor, Store, StyleColorSize, PrePackSummary  │
│      │       Each inherits version from DetailsReport           │
│      ▼                                                          │
│  Phase 4 ── DailyReport                                         │
│              Cross-company transmission log                     │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                    ┌───────────────────────────┐
                    │   Reporting Layer         │
                    │  - Latest-version views   │
                    │  - Front-end report pages │
                    │  - Error monitoring view  │
                    └───────────────────────────┘
```

---

## Pipeline Stages

### Phase 1 — Details Report
Each of the 8 DetailsReport sprocs handles one company/order-type combination. For each unprocessed record it:
1. Reads the JSON from `EDIGatewayInbound`
2. Parses PurchaseOrderDetails (which can be a JSON array or a single object)
3. Extracts SDQ (Store Distribution Quantity) segments to fan out store-level allocations
4. Computes a version number scoped to `Company + CustomerPO`
5. Inserts one header row and one detail row per UPC × Store into the DetailsReport tables
6. Marks the source record `DetailsReportStatus = 'Success'`

### Phase 2 — Version Recalculation
Because DetailsReport sprocs may process records slightly out of chronological order, a dedicated sproc (`usp_Recalculate_DetailsReport_Versions`) runs after Phase 1 and corrects any misnumbering using `ROW_NUMBER() OVER (PARTITION BY Company, CustomerPO ORDER BY DateDownloadedDateTime)`. This is idempotent — it only updates rows where the version number is incorrect.

### Phase 3 — Downstream Reports
Twenty-eight sprocs generate four additional report types per company/order type. Each downstream sproc:
- Requires `DetailsReportStatus = 'Success'` and its own report status still NULL
- Looks up the correct version number via the `SourceTableId` foreign key to the DetailsReport header, so downstream reports always share the same version as the DetailsReport they derived from
- Aggregates data at the appropriate granularity (style/color, store, UPC, or BOM component)

### Phase 4 — Daily Report
A single cross-company sproc records a transmission log entry for every successfully processed PO, providing a simple daily audit trail.

---

## Data Sources & Order Types

Each retail partner uses a distinct order type code and may have structural differences in how their JSON is formed. The pipeline handles all combinations:

| Company | Order Type | SDQ Format | BOM Items | CTE Pattern |
|---------|------------|------------|-----------|-------------|
| Kohl's | BULK | Single object | No | BULK |
| Kohl's | PACKBYSTORE | Array **or** single object | No | PACKBYSTORE |
| Kohl's | PREPACK / COMPOUND PREPACK | Single object | Yes (all items) | PREPACK |
| Arula | KN | Array **or** single object | Some items | PACKBYSTORE + BOM |
| Arula | SA | Single object | Some items | BULK + BOM |
| Maurices | SO | None | Some items | SIMPLE |
| Belk | BK / RL | Array **or** single object | Some items (hybrid) | PREPACK + BOM |
| Belk | SA | Array **or** single object | No | PACKBYSTORE |

**SDQ (Store Distribution Quantity)** is the EDI mechanism that encodes which stores receive which quantities within a single line item. It can arrive as either a JSON array of segment objects or a single JSON object depending on the trading partner — the pipeline detects and handles both via `UNION ALL` branching in the `SDQ_Segments` CTE.

**BOM (Bill of Materials)** line items represent pre-pack assemblies. A BOM parent item contains a nested array of component items, each with its own style, color, size, and quantity. Handling BOM data requires a different approach to field extraction, style naming (P-pack nomenclature for Arula and Belk), and aggregation.

---

## Report Types

| Report | Granularity | Key Fields |
|--------|-------------|------------|
| **Details Report** | UPC × Store | Style, Color, Size, UPC, SKU, StoreNumber, Qty, UOM, UnitPrice, RetailPrice, InnerPack |
| **Style Color Report** | Style × Color | QtyOrdered (summed across all stores) |
| **Store Report** | Store | StoreQty, OrderQtyTotal |
| **Style Color Size Report** | UPC (Style × Color × Size) | Qty, UnitPrice, RetailPrice, Amount |
| **PrePack Summary Report** | BOM parent × component | PrePack fields, ComponentStyle, ComponentColor, ComponentSize, ComponentUPC, ComponentQty |
| **Daily Report** | Company × PO × Date | Version tracking and transmission log |

Each report type has a `Header` table (one row per PO transmission) and a `Detail` table (one row per logical unit of that report). All header tables carry a `Version` field that distinguishes multiple transmissions of the same PO over time.

---

## Key Engineering Challenges

### Versioned PO History
Most purchase orders are transmitted multiple times — initial send, amendments, corrections. Rather than overwriting, each transmission is stored as a new versioned record. The version number is computed at insert time and corrected post-hoc by the recalculation sproc. Database views expose only the highest version per PO for daily use, while the full history remains queryable for auditing.

### Non-Uniform JSON Structure
The upstream JSON does not have a single canonical shape. `PurchaseOrderDetails` can be an array or a single object. SDQ segments can be an array of objects or a single object. Every sproc uses `UNION ALL` branching with `LEFT(LTRIM(...), 1) = '['` detection to handle both cases transparently.

### BOM-Conditional Field Logic
For companies that mix BOM and non-BOM line items within the same purchase order (Belk BK/RL is the primary case), every field that differs between BOM and standard items — Style, Color, Size, InnerPack, QtyPerInnerPack — is computed with a `CASE WHEN BOMDetails_JSON IS NOT NULL THEN ... ELSE ...` branch. This hybrid handling had to be correct for all field combinations simultaneously.

### P-Pack Style Nomenclature
Arula and Belk BK/RL use a company-specific naming convention where BOM parent styles are suffixed with `' P' + [total BOM quantity]` (e.g., `"A1234 P12"`). This suffix is derived by summing quantities across all BOM component rows using a correlated subquery. The rule is deliberately excluded from Kohl's PREPACK and Maurices, which use their own conventions.

### SDQ Pairing Logic
Within each SDQ segment, store numbers occupy odd-indexed fields (SDQ03, SDQ05, ...) and quantities occupy even-indexed fields (SDQ04, SDQ06, ...). The pairing is resolved with a self-join on the parsed SDQ rows using `SDQ_Index % 2` filtering and `SDQ_Index + 1` adjacency — within the same segment index to prevent cross-segment contamination.

### BOM Deduplication in PrePack Summary
Multiple line items in a PO may reference the exact same pre-pack composition. The PrePack Summary report stores each unique composition once, deduplicating via a canonical signature built from `STRING_AGG` over sorted component fields (requires SQL Server 2017+).

### Maurices Quantity Type Mismatch
Maurices transmits quantities as decimal strings (e.g., `"238.0"`). All quantity fields for Maurices — including BOM component quantities — use `CAST(TRY_CAST(... AS FLOAT) AS INT)` to safely convert without string manipulation.

---

## Project Structure

```
TableMethod/
├── Execute_All_Reports.sql                          # Orchestration script (run by SQL Agent Job)
├── usp_Recalculate_DetailsReport_Versions.sql       # Post-Phase-1 version correction
│
├── usp_Parse_Kohls_BULK_DetailsReport.sql
├── usp_Parse_Kohls_BULK_StyleColorReport.sql
├── usp_Parse_Kohls_BULK_StoreReport.sql
├── usp_Parse_Kohls_BULK_StyleColorSizeReport.sql
│
├── usp_Parse_Kohls_PACKBYSTORE_DetailsReport.sql
├── usp_Parse_Kohls_PACKBYSTORE_StyleColorReport.sql
├── usp_Parse_Kohls_PACKBYSTORE_StoreReport.sql
├── usp_Parse_Kohls_PACKBYSTORE_StyleColorSizeReport.sql
│
├── usp_Parse_Kohls_PREPACK_DetailsReport.sql
├── usp_Parse_Kohls_PREPACK_StyleColorReport.sql
├── usp_Parse_Kohls_PREPACK_StoreReport.sql
├── usp_Parse_Kohls_PREPACK_StyleColorSizeReport.sql
├── usp_Parse_Kohls_PrePackSummaryReport.sql
│
├── usp_Parse_Arula_KN_DetailsReport.sql
├── usp_Parse_Arula_KN_StyleColorReport.sql
├── usp_Parse_Arula_KN_StoreReport.sql
├── usp_Parse_Arula_KN_StyleColorSizeReport.sql
├── usp_Parse_Arula_SA_DetailsReport.sql
├── usp_Parse_Arula_SA_StyleColorReport.sql
├── usp_Parse_Arula_SA_StoreReport.sql
├── usp_Parse_Arula_SA_StyleColorSizeReport.sql
├── usp_Parse_Arula_PrePackSummaryReport.sql
│
├── usp_Parse_Maurices_DetailsReport.sql
├── usp_Parse_Maurices_StyleColorReport.sql
├── usp_Parse_Maurices_StoreReport.sql
├── usp_Parse_Maurices_StyleColorSizeReport.sql
├── usp_Parse_Maurices_PrePackSummaryReport.sql
│
├── usp_Parse_Belk_BK_DetailsReport.sql             # Handles both BK and RL order types
├── usp_Parse_Belk_BK_StyleColorReport.sql
├── usp_Parse_Belk_BK_StoreReport.sql
├── usp_Parse_Belk_BK_StyleColorSizeReport.sql
├── usp_Parse_Belk_SA_DetailsReport.sql
├── usp_Parse_Belk_SA_StyleColorReport.sql
├── usp_Parse_Belk_SA_StoreReport.sql
├── usp_Parse_Belk_SA_StyleColorSizeReport.sql
├── usp_Parse_Belk_PrePackSummaryReport.sql
│
└── usp_Parse_DailyReport.sql
```

---

## Schema Design

The pipeline uses a **flat header/detail model** rather than a fully normalized schema. This was a deliberate decision: report types have meaningfully different shapes, and a normalized approach would require wide joins at query time that created timeout issues in the operational environment. Each report type owns its own pair of tables, which keeps queries fast and the sproc logic straightforward.

**Status tracking** is maintained on `EDIGatewayInbound` itself. Each report type has a corresponding `*Status` and `*Processed` column on the source record. A sproc only picks up records where its own status is NULL and (for downstream reports) where `DetailsReportStatus = 'Success'`. This makes reprocessing safe and gives a clear per-record audit trail.

### Core Tables

| Table | Purpose |
|-------|---------|
| `Custom88DetailsReportHeader` / `Detail` | Base-level UPC × Store data with versioning |
| `Custom88StyleColorReportHeader` / `Detail` | Aggregated by style and color |
| `Custom88StoreReportHeader` / `Detail` | Aggregated by store number |
| `Custom88StyleColorSizeReportHeader` / `Detail` | Aggregated by UPC with pricing |
| `Custom88PrePackSummaryHeader` / `Detail` | BOM parent-to-component breakdown |
| `Custom88DailyReport` | Cross-company daily transmission log |

---

## Orchestration

`Execute_All_Reports.sql` is the entry point for the SQL Server Agent Job, which runs hourly. It executes all 37 sprocs in the correct dependency order across four phases. The phase structure ensures:

1. All DetailsReport data is committed before version recalculation runs
2. Version recalculation completes before any downstream report reads version numbers
3. The DailyReport only runs after all other reports are settled

Each sproc is self-contained and idempotent with respect to already-processed records — re-running the full script will not duplicate data.

---

## Prerequisites

- **SQL Server 2017 or later** (required for `STRING_AGG` used in PrePack Summary deduplication)
- **EDIGatewayInbound** staging table with `JSONContent` (NVARCHAR(MAX)), transaction metadata columns, and per-report `*Status` / `*Processed` columns
- **CData Arc** (or equivalent EDI-to-JSON translation layer) populating the staging table upstream
- All `Custom88*` report tables created before first execution
- A SQL Server Agent Job configured to execute `Execute_All_Reports.sql` on the desired schedule
