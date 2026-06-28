# Locked Decisions for Story aa945e5f-915f-4cf4-9a79-7a31dd91d89b

## Implementation Approach
## Implementation Approach — BigQuery Physical Schema DDL

### Deliverables

**3 BigQuery DDL SQL files** (one per dataset, matching the source Hive DDL grouping):

| File | Dataset | Tables | Source Hive DDL files parsed |
|---|---|---|---|
| `ddl/nbcs_staging.sql` | `nbcs_staging` | 45 | `02-staging-sqoop-mirrors.hql`, `03-staging-delta-feeds.hql`, `04-staging-file-feeds.hql` |
| `ddl/nbcs_ods.sql` | `nbcs_ods` | 30 | `05-ods-cleanse.hql`, `06-ods-delta-scd2.hql`, `07-ods-acid.hql` |
| `ddl/nbcs_dm.sql` | `nbcs_dm` | 25 | `08-dm-tables.hql` |

**9 MVS YAML specs** (one per acceptance criterion) — details in the Validation decision.

### DDL Generation Rules

**1. Type mapping** (per locked Schema and Type Mapping decision):

| Hive Type | BigQuery Type | Notes |
|---|---|---|
| `STRING` | `STRING` | Direct |
| `BIGINT` | `INT64` | Direct |
| `INT` | `INT64` | Widen (including inside STRUCT sub-fields) |
| `BOOLEAN` | `BOOL` | Direct |
| `DECIMAL(p,s)` | `NUMERIC(p,s)` | All 52 DECIMAL columns — max precision 14,4 fits NUMERIC |
| `DOUBLE` | `FLOAT64` | Direct |
| `TIMESTAMP` | `TIMESTAMP` | Direct (UTC) |
| `ARRAY<STRUCT<...>>` | `ARRAY<STRUCT<...>>` | Recursive type translation inside STRUCT fields |
| `ARRAY<STRING>` | `ARRAY<STRING>` | Direct |
| `MAP<STRING,STRING>` | `ARRAY<STRUCT<key STRING, value STRING>>` | BigQuery has no MAP type |

**2. Partition columns promoted from STRING to DATE:**

In Hive, partition columns appear in `PARTITIONED BY (col TYPE)` outside the column list. In BigQuery, they are regular columns. All string-date partition columns are promoted to `DATE` type and added to the column list:

| Column pattern | Source type | BigQuery type | DDL clause |
|---|---|---|---|
| `load_date` (27 sqoop) | STRING | DATE | `PARTITION BY load_date` |
| `feed_date` (10 file feeds) | STRING | DATE | `PARTITION BY feed_date` |
| `extract_ts` (8 deltas) | STRING | DATE | `PARTITION BY extract_ts` |
| `snapshot_date`, `event_date`, `sched_date`, `call_date` (15+8 ODS) | STRING | DATE | `PARTITION BY <col>` |
| `work_month`, `period_month`, `swap_month`, `event_month` (ODS delta + DM) | STRING | DATE | `PARTITION BY <col>` |
| `eff_from_year` (3 SCD-2) | INT | INT64 | `PARTITION BY RANGE_BUCKET(eff_from_year, GENERATE_ARRAY(2015, 2030, 1))` |
| `date_key`, `week_start_key` (DM facts/aggs) | INT | INT64 | `PARTITION BY RANGE_BUCKET(date_key, GENERATE_ARRAY(20200101, 20301231, 1))` |

**3. Multi-column Hive partitions collapsed (per locked Performance Optimization):**

| Table | Hive partition | BigQuery partition | BigQuery cluster |
|---|---|---|---|
| `stg_wfm_schedule` | `(load_date STRING, site_code STRING)` | `PARTITION BY load_date` (DATE) | `CLUSTER BY site_code` |
| 10 file-feed tables | `(client_code STRING, feed_date STRING)` | `PARTITION BY feed_date` (DATE) | `CLUSTER BY client_code` |
| `fact_interaction` | `(date_key INT, channel STRING)` | `PARTITION BY RANGE_BUCKET(date_key, GENERATE_ARRAY(20200101, 20301231, 1))` | `CLUSTER BY channel, agent_sk` |

**4. Clustering columns** (per locked Performance Optimization):

| Table | CLUSTER BY | Source |
|---|---|---|
| `fact_interaction` | `channel, agent_sk` | Second partition col + legacy bucketing |
| `fact_agent_activity` | `agent_sk` | Agent-centric queries |
| `fact_queue_interval` | `queue_sk` | Queue-centric queries |
| `fact_billing_line` | `client_sk, program_sk` | Billing lookups |
| `agg_agent_daily` | `agent_sk, site_code` | Agent/site drill-downs |
| `agg_queue_hourly` | `queue_sk` | Queue performance |
| 4 ODS ACID tables | Primary key (`client_id`, `agent_id`, `ticket_id`, `invoice_id`) | Replaces Hive bucketing |
| 9 DM dimensions | Surrogate key (`agent_sk`, `client_sk`, `program_sk`, `queue_sk`, `site_sk`, `shift_sk`, `org_sk`, `disposition_sk`, `date_key`) | Join key optimization |

**5. Table OPTIONS:**
- `fact_interaction`: `OPTIONS(require_partition_filter = true)` — prevents full-table scans
- All other tables: no special options

**6. ACID tables to standard BigQuery tables:**
- Drop `CLUSTERED BY ... INTO N BUCKETS`, `transactional=true`, `STORED AS ORC`
- BigQuery natively supports DML on all tables — no special ACID properties
- 4 tables: `ods_client_acid`, `ods_agent_acid`, `ods_ticket_acid`, `ods_invoice_acid`

**7. Column descriptions for epoch annotations:**
- All 68 source column COMMENTs carried as BigQuery `OPTIONS(description = '...')`
- The 2 lie columns get explicit descriptions:
  - `stg_fin_invoice.issued_ts_sec`: `OPTIONS(description = '!! name says seconds, VALUES ARE MILLIS !!')`
  - `stg_fin_invoice.due_ts_sec`: `OPTIONS(description = '!! name says seconds, VALUES ARE MILLIS !!')`

**8. Complex/nested type columns (4 total):**
- `stg_file_qa_forms.sections`: `ARRAY<STRUCT<section_code STRING, max_points INT64, scored_points INT64>>` (Hive INT widened to INT64)
- `stg_file_chat_transcripts.messages`: `ARRAY<STRUCT<sender STRING, ts_ms INT64, text STRING>>` (BIGINT to INT64)
- `stg_file_chat_transcripts.metadata`: `ARRAY<STRUCT<key STRING, value STRING>>` (MAP to ARRAY of STRUCT)
- `stg_file_speech_analytics.keywords`: `ARRAY<STRING>` (direct)

**9. Out-of-scope:**
- 15 dm views — authored by the Transform flow, NOT included in this DDL set
- No data loading — DDL only
- No IAM/GRANT statements — covered by the IaC/Security story

### DDL File Structure

Each file follows this pattern:
```sql
-- BigQuery DDL for nbcs_staging (45 tables)
-- Source: hive/ddl/02-staging-sqoop-mirrors.hql, 03-staging-delta-feeds.hql, 04-staging-file-feeds.hql
-- Type mapping per locked Schema and Type Mapping decision
-- Partitioning per locked Performance Optimization decision

CREATE TABLE IF NOT EXISTS stg_crm_client (
  client_id         INT64,
  client_code       STRING,
  ...
  created_ts        INT64 OPTIONS(description = 'epoch SECONDS (legacy)'),
  updated_ts        INT64 OPTIONS(description = 'epoch SECONDS (legacy)'),
  load_date         DATE
)
PARTITION BY load_date;
```

- Table names are unqualified (the harness sets the default dataset via `dataset_map`)
- `CREATE TABLE IF NOT EXISTS` for idempotency
- Partition column added as the last column in the column list
- `OPTIONS(description = ...)` on columns with source COMMENTs

### Harness Integration

The 9 MVS specs use `dataset_map` for multi-dataset builds:
```yaml
migration:
  dataset_map:
    nbcs_staging: dmt_build_staging
    nbcs_ods: dmt_build_ods
    nbcs_dm: dmt_build_dm
  steps:
    - { kind: ddl, dataset: dmt_build_staging, sql: ddl/nbcs_staging.sql }
    - { kind: ddl, dataset: dmt_build_ods, sql: ddl/nbcs_ods.sql }
    - { kind: ddl, dataset: dmt_build_dm, sql: ddl/nbcs_dm.sql }
```

`source_setup` references the original Hive DDL for live cross-checks:
```yaml
source_setup:
  location_base: /tmp/dmt_src
  ddl:
    - hive/ddl/01-create-databases.hql
    - hive/ddl/02-staging-sqoop-mirrors.hql
    - hive/ddl/03-staging-delta-feeds.hql
    - hive/ddl/04-staging-file-feeds.hql
    - hive/ddl/05-ods-cleanse.hql
    - hive/ddl/06-ods-delta-scd2.hql
    - hive/ddl/07-ods-acid.hql
    - hive/ddl/08-dm-tables.hql
```

Each MVS spec independently applies all 3 DDL files from a clean slate (fully self-contained, no shared state between specs).

## Validation
## Validation — 9 MVS Specs (One Per Acceptance Criterion)

Each MVS spec is fully self-contained: applies all 3 DDL files to ephemeral build datasets, runs its checks, tears down. Uses `source_setup` where the AC requires live Hive cross-checks. All specs share the same `migration:` and `source_setup:` blocks (redundant application is accepted for isolation).

### Common Blocks (shared by all 9 specs)

```yaml
connections:
  source: { engine: impala }
  target: { engine: bigquery }

migration:
  dataset_map:
    nbcs_staging: dmt_build_staging
    nbcs_ods: dmt_build_ods
    nbcs_dm: dmt_build_dm
  steps:
    - { kind: ddl, dataset: dmt_build_staging, sql: ddl/nbcs_staging.sql }
    - { kind: ddl, dataset: dmt_build_ods, sql: ddl/nbcs_ods.sql }
    - { kind: ddl, dataset: dmt_build_dm, sql: ddl/nbcs_dm.sql }

source_setup:
  location_base: /tmp/dmt_src
  ddl:
    - hive/ddl/01-create-databases.hql
    - hive/ddl/02-staging-sqoop-mirrors.hql
    - hive/ddl/03-staging-delta-feeds.hql
    - hive/ddl/04-staging-file-feeds.hql
    - hive/ddl/05-ods-cleanse.hql
    - hive/ddl/06-ods-delta-scd2.hql
    - hive/ddl/07-ods-acid.hql
    - hive/ddl/08-dm-tables.hql
```

---

### AC1: `ac1_ddl_apply.mvs.yaml` — Table count applied with 0 DDL errors

**What it proves:** All 100 CREATE TABLE statements apply to 3 datasets with 0 errors.

**Pattern:** `schema_conformance`

**Suite structure:**
- 3 suites (one per dataset), each with `expect_table_count` assertion
- Each suite lists all tables in that dataset with minimal column checks (just first column — existence is the gate)
- `expect_table_count: 45` for staging, `30` for ods, `25` for dm
- Any DDL error aborts the build step and the suite reports FAIL naming the table and error

**Pass rule:** 100/100 tables created, 0 DDL errors. The harness's build step fails loudly on any CREATE error — never swallowed.

---

### AC2: `ac2_column_fidelity.mvs.yaml` — Per-column fidelity across all 916 columns

**What it proves:** Every column present, name preserved, correct mapped type including DECIMAL precision/scale and complex types checked recursively, descriptions carried.

**Pattern:** `schema_conformance` with `source_database` cross-check

**Suite structure:**
- 3 suites (one per dataset), each listing all tables with FULL column specs
- Every column declares: `name`, `type`, `scale` (for NUMERIC), `source_type`, `source_name` (if renamed)
- Complex type columns use full nested signature: `type: "ARRAY<STRUCT<section_code STRING, max_points INT64, scored_points INT64>>"`
- `description` field on all 68 epoch-annotated columns, including the 2 lie columns
- `source_database: staging` / `ods` / `dm` and `source_table` on each table for live Hive cross-check
- Partition columns marked with `source_type: STRING` (promoted to DATE) for cross-check awareness

**Pass rule:** 916/916 columns checked across 100/100 tables, 0 mismatches.

---

### AC3: `ac3_object_type.mvs.yaml` — Object-type fidelity

**What it proves:** Every source table lands as BASE TABLE; no source table silently becomes a view; no view name appears as a table.

**Pattern:** `schema_conformance`

**Suite structure:**
- 3 suites, one per dataset
- Each table entry has `expect_object_type: TABLE` and minimal columns (just PK)
- An additional check in the `nbcs_dm` suite: 15 "absence" entries — one for each view name (`vw_org_hierarchy`, `vw_active_agents_ndv`, etc.) — these must NOT appear as tables
- Absence is checked by asserting `expect_table_count: 25` for `nbcs_dm` (if a view name were silently created as a table, count would exceed 25)

**Pass rule:** 100/100 source tables landed as BASE TABLE; 0 view-to-table flips.

---

### AC4: `ac4_partition_cluster.mvs.yaml` — Partition + cluster + key intent

**What it proves:** Every table's partition strategy, clustering columns, and table options match the locked Performance Optimization decision.

**Pattern:** `schema_conformance`

**Suite structure:**
- 3 suites, one per dataset
- Each table entry declares: `partition_by` (column name), `cluster_by` (ordered list), `table_options` where applicable
- 87 partitioned tables assert their specific partition column
- 13 unpartitioned tables (9 dims + 4 ACID) omit `partition_by` — harness confirms no partition metadata
- `fact_interaction` asserts `table_options: { require_partition_filter: true }`
- Clustering columns in exact order (e.g., `cluster_by: [channel, agent_sk]` for fact_interaction)

**Key entries (representative):**
```yaml
- table: fact_interaction
  partition_by: date_key
  cluster_by: [channel, agent_sk]
  table_options: { require_partition_filter: true }
  columns: [ { name: date_key, type: INT64 } ]

- table: ods_agent_scd2
  partition_by: eff_from_year
  columns: [ { name: eff_from_year, type: INT64 } ]

- table: ods_client_acid
  cluster_by: [client_id]
  columns: [ { name: client_id, type: INT64 } ]

- table: dim_agent
  cluster_by: [agent_sk]
  columns: [ { name: agent_sk, type: INT64 } ]
```

**Pass rule:** 87/87 partitioned tables match; 13/13 unpartitioned confirmed; all clustering correct; `require_partition_filter=true` on `fact_interaction`.

---

### AC5: `ac5_fk_type_consistency.mvs.yaml` — Cross-dataset FK/PK type consistency

**What it proves:** All surrogate-key, natural-key, and cross-layer join columns have matching types across datasets (INT64-to-INT64, STRING-to-STRING), preventing implicit type coercion.

**Pattern:** `schema_conformance` (multi-dataset)

**Suite structure:**
- 3 suites covering all 3 datasets, focusing specifically on FK/PK join columns
- Each join column is declared with its expected type on BOTH sides:
  - DM facts/aggs → DM dimensions: `agent_sk INT64`, `client_sk INT64`, `program_sk INT64`, `queue_sk INT64`, `date_key INT64` — asserted on both fact and dimension tables
  - Staging → ODS: `client_id INT64`, `agent_id INT64`, `program_id INT64`, `queue_id INT64`, `ticket_id INT64`, `invoice_id INT64` — same INT64 on both sides after BIGINT mapping
  - Cross-layer STRING keys: `interaction_ref STRING`, `survey_id STRING`, `session_ref STRING`, `chat_ref STRING`
- Since schema_conformance checks each column's type independently per table, type consistency across datasets is proven by the union of all checks: if `dim_agent.agent_sk = INT64` and `fact_interaction.agent_sk = INT64`, the join path is type-safe.

**Pass rule:** All documented FK/PK join paths type-consistent across 3 datasets, 0 type-coercion risks.

---

### AC6: `ac6_queryability_smoke.mvs.yaml` — Queryability smoke

**What it proves:** `SELECT *` succeeds for all 100 tables AND representative cross-table queries run with 0 type-coercion errors.

**Pattern:** `query_performance` in `measure` mode

**Suite structure:**
- One `query_performance` suite with 103 queries:
  - 100 `SELECT * FROM <table> LIMIT 0` queries (one per table, measure mode, no thresholds)
  - 3 representative tier queries:
    1. **Staging:** `SELECT load_date, start_epoch, ani FROM dmt_build_staging.stg_tel_call WHERE load_date = DATE '2026-06-01' LIMIT 10`
    2. **ODS:** `SELECT c.call_id, c.start_ts, q.queue_name FROM dmt_build_ods.ods_call c JOIN dmt_build_ods.ods_queue q ON c.queue_id = q.queue_id WHERE c.call_date = DATE '2026-06-01' LIMIT 10`
    3. **DM:** `SELECT f.interaction_id, a.full_name, f.start_ts FROM dmt_build_dm.fact_interaction f JOIN dmt_build_dm.dim_agent a ON f.agent_sk = a.agent_sk WHERE f.date_key BETWEEN 20260601 AND 20260630 LIMIT 10`
- All 103 queries use `mode: measure` (no assertions beyond "query succeeds")
- A query that fails to execute results in ERROR status = HARD FAIL

**Pass rule:** 100/100 SELECT * succeed; 3/3 representative tier queries succeed with 0 errors.

---

### AC7: `ac7_integrity_guards.mvs.yaml` — Integrity guards

**What it proves:** Every table and column is catalog-readable post-DDL; no skips or swallowed errors.

**Pattern:** `schema_conformance`

**Suite structure:**
- 3 suites with `expect_table_count` per dataset (45, 30, 25)
- Every table listed with ALL columns — the harness's introspection reads `INFORMATION_SCHEMA.TABLES` and `INFORMATION_SCHEMA.COLUMNS`
- A table present in DDL but missing from catalog = HARD FAIL (harness behavior: table not in `actual_tables` → `Status.FAIL, "table missing from target dataset"`)
- A column present in source but missing from target = HARD FAIL (harness behavior: column not in `info.column(cn)` → `Status.FAIL, "column missing"`)
- The harness never counts "absent on both sides" as a match for required structural objects (tables, columns, types)

**Pass rule:** 100/100 tables catalog-readable; 916/916 columns catalog-readable; 0 skips.

---

### AC8: `ac8_no_silent_skip.mvs.yaml` — No-silent-skip (live dual-catalog)

**What it proves:** Every check runs against LIVE BigQuery target AND LIVE Hive source catalogs — no offline/parse-only validation.

**Pattern:** `schema_conformance` with full `source_setup` and `source_database` cross-checks

**Suite structure:**
- This is the COMPREHENSIVE spec — combines AC2's column fidelity with AC4's partition checks, all with `source_database` cross-referencing
- `source_setup` applies the 8 Hive DDL files to a live Hive metastore (the harness rewrites HDFS locations via `location_base` and flips ACID to non-ACID)
- Every table declares `source_table` and every column declares `source_type` — the harness introspects the LIVE Hive source and compares
- The harness reads source via `ctx.source.introspect_table()` (Impala) with fallback to `ctx.hive.introspect_table()` (Hive HS2) for SerDe tables (RegexSerDe, JsonSerDe)
- 3 suites (one per dataset), all with `source_database: staging` / `ods` / `dm`

**Pass rule:** 100/100 tables exercised live on both source and target; 916/916 columns cross-checked; 0 offline-only checks.

---

### AC9: `ac9_perf_hot_path.mvs.yaml` — Physical-access performance on hot-path tables

**What it proves:** Partition/cluster-filtered queries scan materially fewer bytes than unfiltered scans on 5 hot-path tables.

**Pattern:** `query_performance` in `compare` mode with `dry_run: true`

**Suite structure:**
- One `query_performance` suite with 6 query pairs:

1. **fact_interaction (partition + cluster):**
   - A: `SELECT * FROM dmt_build_dm.fact_interaction` (unfiltered)
   - B: `SELECT * FROM dmt_build_dm.fact_interaction WHERE date_key BETWEEN 20260101 AND 20260131 AND channel = 'VOICE'` (pruned)
   - Compare: `bytes_scanned: "b <= a"`

2. **fact_interaction (require_partition_filter rejection):**
   - Single query with NO date_key filter, `mode: assert`, asserting it FAILS (BigQuery rejects filterless queries when `require_partition_filter=true`)
   - This is a special negative test — the query must error

3. **agg_agent_daily:**
   - A: unfiltered full scan
   - B: `WHERE date_key BETWEEN 20260101 AND 20260131` + agent_sk/site_code filter
   - Compare: `bytes_scanned: "b <= a"`

4. **agg_queue_hourly:**
   - A: unfiltered, B: date_key + queue_sk filtered
   - Compare: `bytes_scanned: "b <= a"`

5. **fact_billing_line:**
   - A: unfiltered, B: period_month + client_sk/program_sk filtered
   - Compare: `bytes_scanned: "b <= a"`

6. **fact_queue_interval:**
   - A: unfiltered, B: date_key + queue_sk filtered
   - Compare: `bytes_scanned: "b <= a"`

All comparisons use `dry_run: true` (free, deterministic byte estimates from BigQuery's query planner — no data needed in the tables).

**Pass rule:** 5/5 hot-path tables show material byte reduction; fact_interaction filterless query rejected.

---

### File Layout

```
ddl/
  nbcs_staging.sql          # 45 CREATE TABLE statements
  nbcs_ods.sql              # 30 CREATE TABLE statements
  nbcs_dm.sql               # 25 CREATE TABLE statements
specs/
  ac1_ddl_apply.mvs.yaml
  ac2_column_fidelity.mvs.yaml
  ac3_object_type.mvs.yaml
  ac4_partition_cluster.mvs.yaml
  ac5_fk_type_consistency.mvs.yaml
  ac6_queryability_smoke.mvs.yaml
  ac7_integrity_guards.mvs.yaml
  ac8_no_silent_skip.mvs.yaml
  ac9_perf_hot_path.mvs.yaml
```

### Key Harness Patterns Used

| AC | Harness pattern | Key fields |
|---|---|---|
| 1 | `schema_conformance` | `expect_table_count`, table existence |
| 2 | `schema_conformance` | Full column spec with `source_type`, `scale`, `description`, nested types |
| 3 | `schema_conformance` | `expect_object_type: TABLE`, `expect_table_count` for absence |
| 4 | `schema_conformance` | `partition_by`, `cluster_by`, `table_options` |
| 5 | `schema_conformance` | Multi-dataset type assertions on FK/PK columns |
| 6 | `query_performance` | `mode: measure`, 103 SELECT queries |
| 7 | `schema_conformance` | Full table+column listing, `expect_table_count` |
| 8 | `schema_conformance` | `source_setup` + `source_database` cross-check |
| 9 | `query_performance` | `mode: compare`, `dry_run: true`, A/B byte comparison |
