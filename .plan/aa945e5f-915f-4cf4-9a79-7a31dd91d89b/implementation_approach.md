# Implementation Approach

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
