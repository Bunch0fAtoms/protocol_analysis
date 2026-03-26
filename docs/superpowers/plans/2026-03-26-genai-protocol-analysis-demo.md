# Gen AI Protocol Analysis Demo — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an end-to-end Databricks demo proving the platform can run GMR's clinical protocol assessment pipeline — synthetic data, LDP ingestion, LLM-powered protocol execution via RAG, and AI/BI analytics.

**Architecture:** Lakeflow Declarative Pipeline (medallion) for ingestion + notebook tasks for LLM steps orchestrated by a Databricks Workflow. Vector Search for protocol RAG. AI/BI Dashboard + Genie Space for analytics.

**Tech Stack:** Databricks (LDP, Vector Search, ai_query, AI/BI Lakeview, Genie), Python, SQL, Unity Catalog

**Spec:** `docs/superpowers/specs/2026-03-26-genai-protocol-analysis-demo-design.md`

**Important context:**
- This is a Databricks demo project — "files" are notebooks deployed to a workspace
- No traditional unit tests — verification is running each step and checking outputs via `execute_sql` or notebook execution
- Use `@fe-databricks-tools:databricks-authentication` skill before any Databricks operations
- Use `@databricks-ai-dev-kit:databricks-config` to verify workspace connection
- Catalog/schema are parameterized — use a config cell at the top of each notebook
- **Critical pattern:** All notebooks that combine Python config with SQL must use `spark.sql(f"...")` in Python cells — NOT `# MAGIC %sql` with f-string interpolation (magic cells do not support Python variable interpolation)
- **ai_query() return types:** When generating embeddings, use `ai_query(..., returnType => 'ARRAY<FLOAT>')`. For LLM JSON responses, parse inline with `from_json()` rather than overwriting the table in a second pass.
- **Row deduplication:** `silver_calls_routed` has multiple rows per call (one per Q&A). Aggregate to one row per `call_id` before joining to `silver_call_narratives` for LLM steps.
- **Morgan's analysis notebook** is out of scope for this plan — Morgan will build it separately.

---

## File Structure

All notebooks live in the workspace under a single project folder. Local files mirror this structure:

```
geniedemo1/
├── docs/superpowers/specs/...          # Design spec (exists)
├── docs/superpowers/plans/...          # This plan (exists)
├── notebooks/
│   ├── 00_config.py                    # Shared config: catalog, schema, volume path
│   ├── 01_synthetic_data_gen.py        # Generate all CSV source data, upload to UC Volume
│   ├── 02_ldp_pipeline.sql             # LDP pipeline definition (bronze + non-LLM silver)
│   ├── 03_narrative_generation.py      # ai_query: Q&A rows → narratives
│   ├── 04_protocol_embedding.py        # ai_query: embeddings + Vector Search setup
│   ├── 05_protocol_execution.py        # ai_query: narrative + protocol → disposition + audit
│   ├── 06_evaluation_summary.py        # ai_query: execution → clinical evaluation
│   ├── 07_care_assessment.py           # ai_query: dispatch necessity, tiering, quality
│   ├── 08_analytics_aggregation.sql    # SQL aggregations → gold_analytics_summary
│   └── 09_export_review.py            # Export flagged results to CSV
└── data/                               # Local staging for synthetic data (optional)
```

---

## Task 1: Configuration & Workspace Setup

**Files:**
- Create: `notebooks/00_config.py`

**Prerequisites:** Authenticate to Databricks workspace, verify connection. Use `@fe-databricks-tools:databricks-authentication` and `@databricks-ai-dev-kit:databricks-config`.

- [ ] **Step 1: Verify Databricks workspace connection**

Confirm which workspace we're connected to and that we have permissions to create catalogs/schemas.

- [ ] **Step 2: Create the config notebook**

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # GMR Protocol Analysis Demo — Configuration
# MAGIC Shared configuration for all demo notebooks.

# Catalog and schema — change these to match your workspace
CATALOG = "morgancatalog"
SCHEMA = "protocol_analysis"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/source_data"

# Model endpoints for ai_query
LLM_MODEL = "databricks-meta-llama-3-3-70b-instruct"  # verify availability
EMBEDDING_MODEL = "databricks-bge-large-en"  # fallback: databricks-gte-large-en

# Vector Search
VS_ENDPOINT_NAME = "gmr_protocol_vs_endpoint"
VS_INDEX_NAME = f"{CATALOG}.{SCHEMA}.protocol_chunks_index"

# Full table references
BRONZE_CALL_LOGS = f"{CATALOG}.{SCHEMA}.bronze_call_logs"
BRONZE_PROTOCOLS = f"{CATALOG}.{SCHEMA}.bronze_protocols"
BRONZE_PROTOCOL_MAPPING = f"{CATALOG}.{SCHEMA}.bronze_protocol_mapping"
SILVER_CALLS_DEIDENTIFIED = f"{CATALOG}.{SCHEMA}.silver_calls_deidentified"
SILVER_CALL_NARRATIVES = f"{CATALOG}.{SCHEMA}.silver_call_narratives"
SILVER_CALLS_ROUTED = f"{CATALOG}.{SCHEMA}.silver_calls_routed"
SILVER_PROTOCOL_CHUNKS = f"{CATALOG}.{SCHEMA}.silver_protocol_chunks"
GOLD_PROTOCOL_EXECUTION = f"{CATALOG}.{SCHEMA}.gold_protocol_execution"
GOLD_EVALUATION_SUMMARIES = f"{CATALOG}.{SCHEMA}.gold_evaluation_summaries"
GOLD_CARE_ASSESSMENT = f"{CATALOG}.{SCHEMA}.gold_care_assessment"
GOLD_ANALYTICS_SUMMARY = f"{CATALOG}.{SCHEMA}.gold_analytics_summary"
```

- [ ] **Step 3: Create catalog, schema, and volume on the workspace**

Use `execute_sql` to run:
```sql
CREATE CATALOG IF NOT EXISTS morgancatalog;
CREATE SCHEMA IF NOT EXISTS morgancatalog.protocol_analysis;
CREATE VOLUME IF NOT EXISTS morgancatalog.protocol_analysis.source_data;
```

- [ ] **Step 4: Verify the model endpoints are available**

Use `execute_sql`:
```sql
SELECT ai_query('databricks-meta-llama-3-3-70b-instruct', 'Say hello') AS test;
```
If it fails, check available endpoints via `list_serving_endpoints` and update the config.

- [ ] **Step 5: Upload config notebook to workspace**

Upload `00_config.py` to the workspace project folder.

---

## Task 2: Synthetic Data — Clinical Protocol Files

**Files:**
- Create: `notebooks/01_synthetic_data_gen.py` (protocol generation section)

Generate 10 realistic MPDS-style clinical protocol CSV files and upload to UC Volume.

- [ ] **Step 1: Build the protocol data structures**

Create Python data for all 10 protocols. Each protocol has 40-80 rows with columns: `protocol_name`, `row_number`, `module_name`, `question_text`, `condition`, `disposition`, `stop_rule`.

Protocols to generate:
1. Chest Pain (~60 rows) — case entry, key questions about pain location/severity/duration/history, determinant logic Alpha-Echo, pre-arrival instructions (aspirin, positioning), case exit
2. Breathing Problems (~55 rows) — similar structure, questions about breathing quality, wheezing, choking, cyanosis
3. Falls/Traumatic Injury (~50 rows) — mechanism of injury, head/spine involvement, bleeding
4. Abdominal Pain (~45 rows) — location, nausea, pregnancy possibility, rigidity
5. Allergic Reaction (~45 rows) — airway involvement, epi-pen, hives vs anaphylaxis
6. Seizures (~40 rows) — active vs post-ictal, history, breathing
7. Overdose/Poisoning (~40 rows) — substance, consciousness, breathing, time since ingestion
8. Stroke/Neurological (~45 rows) — FAST criteria, onset time, consciousness
9. Pregnancy Complications (~40 rows) — gestational age, bleeding, contractions, cord
10. Unconscious/Unresponsive (~50 rows) — breathing, pulse, trauma signs, blood sugar

Each protocol MUST follow the MPDS module structure:
- Case Entry (3-5 rows): verify address, callback, "tell me what happened"
- Key Questions (15-35 rows): scripted triage questions with conditions
- Determinant Assignment (10-20 rows): decision logic with dispositions 0-5 and stop rules
- Pre-Arrival Instructions (5-10 rows): word-for-word caller instructions
- Case Exit (2-3 rows): closing script

- [ ] **Step 2: Generate protocol CSVs and upload to volume**

Write each protocol as a CSV to `{VOLUME_PATH}/protocols/{protocol_name}.csv`.

- [ ] **Step 3: Verify protocol files in volume**

```sql
LIST '/Volumes/morgancatalog/protocol_analysis/source_data/protocols/'
```
Confirm 10 CSV files are present.

---

## Task 3: Synthetic Data — Protocol Mapping Table

**Files:**
- Modify: `notebooks/01_synthetic_data_gen.py` (add mapping section)

- [ ] **Step 1: Create the mapping table**

Map legacy protocol names to new protocol names. Example:

| legacy_protocol_name | new_protocol_name | protocol_file |
|---------------------|-------------------|---------------|
| CHEST_PAIN_V1 | Chest Pain | chest_pain.csv |
| HEART_PROBLEMS_V1 | Chest Pain | chest_pain.csv |
| BREATHING_V1 | Breathing Problems | breathing_problems.csv |
| RESPIRATORY_V1 | Breathing Problems | breathing_problems.csv |
| ... | ... | ... |

Include 2-3 legacy names per new protocol to show consolidation.

- [ ] **Step 2: Upload mapping CSV to volume**

Write to `{VOLUME_PATH}/mappings/protocol_mapping.csv`.

- [ ] **Step 3: Verify mapping file**

```sql
LIST '/Volumes/morgancatalog/protocol_analysis/source_data/mappings/'
```

---

## Task 4: Synthetic Data — Patient Call Logs

**Files:**
- Modify: `notebooks/01_synthetic_data_gen.py` (add call generation section)

Generate 500 synthetic patient calls with realistic Q&A rows.

- [ ] **Step 1: Define call generation logic**

For each call, generate:
- `call_id`: unique UUID or sequential ID (CALL-001 through CALL-500)
- `timestamp`: random datetime within last 6 months
- `legacy_protocol_used`: selected from the mapping table's legacy names
- `complaint_type`: derived from legacy protocol
- `caller_name`, `caller_phone`, `caller_address`: fake PII using realistic patterns
- `actual_dispatch_type`: BLS, ALS, supervisor, or no_dispatch (weighted by complaint severity)
- `on_scene_outcome`: transported_to_er, treated_on_scene, refusal, phone_resolved, no_patient_contact
- 5-15 Q&A rows per call with `question_number`, `dispatcher_question`, `caller_answer`

Distribution:
- Chest Pain: 100 calls, Breathing: 100, Falls: 75, Abdominal: 50, Allergic: 40, Seizures: 35, Overdose: 35, Stroke: 25, Pregnancy: 20, Unconscious: 20
- ~15% of calls (75) designed with ambiguous/conflicting answers
- ~5% of calls (25) designed with insufficient information (non-replayable)

- [ ] **Step 2: Build Q&A templates per complaint type**

For each complaint type, create 5-8 realistic dispatcher question templates and varied caller answer templates at different severity levels. The caller answers should use colloquial language ("yeah he's real pale and sweaty", "she can't hardly breathe").

- [ ] **Step 3: Generate all 500 calls and write to volume**

Write to `{VOLUME_PATH}/calls/call_logs_batch_001.csv` with one row per Q&A entry (so ~3,500-7,500 total rows).

- [ ] **Step 4: Verify call log file**

```sql
SELECT COUNT(*) as total_rows, COUNT(DISTINCT call_id) as total_calls
FROM read_files('/Volumes/morgancatalog/protocol_analysis/source_data/calls/')
```
Expected: ~500 distinct call_ids, 3,500-7,500 total rows.

- [ ] **Step 5: Upload complete synthetic data notebook to workspace**

Upload the full `01_synthetic_data_gen.py` notebook.

---

## Task 5: Lakeflow Declarative Pipeline (Bronze + Silver)

**Files:**
- Create: `notebooks/02_ldp_pipeline.sql`

Build the LDP pipeline definition for bronze ingestion and non-LLM silver tables.

- [ ] **Step 1: Write the LDP pipeline SQL notebook**

Use `@databricks-ai-dev-kit:databricks-spark-declarative-pipelines` skill for correct LDP syntax.

```sql
-- Databricks notebook source
-- MAGIC %md
-- MAGIC # GMR Protocol Analysis — Lakeflow Declarative Pipeline
-- MAGIC Bronze ingestion from UC Volume + Silver transformations (non-LLM)

-- Bronze: Ingest call logs
CREATE OR REFRESH STREAMING TABLE bronze_call_logs
AS SELECT * FROM STREAM read_files(
  '/Volumes/morgancatalog/protocol_analysis/source_data/calls/',
  format => 'csv',
  header => true
);

-- Bronze: Ingest protocols
CREATE OR REFRESH STREAMING TABLE bronze_protocols
AS SELECT * FROM STREAM read_files(
  '/Volumes/morgancatalog/protocol_analysis/source_data/protocols/',
  format => 'csv',
  header => true
);

-- Bronze: Ingest protocol mapping
CREATE OR REFRESH STREAMING TABLE bronze_protocol_mapping
AS SELECT * FROM STREAM read_files(
  '/Volumes/morgancatalog/protocol_analysis/source_data/mappings/',
  format => 'csv',
  header => true
);

-- Silver: De-identify call records (strip PII)
CREATE OR REFRESH MATERIALIZED VIEW silver_calls_deidentified AS
SELECT
  call_id,
  timestamp,
  legacy_protocol_used,
  complaint_type,
  question_number,
  dispatcher_question,
  -- PII redaction: replace phone numbers, addresses, and names using multiple patterns
  regexp_replace(
    regexp_replace(
      regexp_replace(caller_answer,
        '\\b\\d{3}[-.]?\\d{3}[-.]?\\d{4}\\b', '[PHONE_REDACTED]'),  -- phone numbers
      '\\b\\d+\\s+[A-Z][a-z]+\\s+(St|Ave|Blvd|Dr|Rd|Ln|Ct|Way)\\b', '[ADDRESS_REDACTED]'),  -- street addresses
    '\\b(Mr|Mrs|Ms|Dr)\\.?\\s+[A-Z][a-z]+\\b', '[NAME_REDACTED]')  -- titles + names
  AS caller_answer,
  actual_dispatch_type,
  on_scene_outcome,
  '[REDACTED]' AS caller_name,
  '[REDACTED]' AS caller_phone,
  '[REDACTED]' AS caller_address
FROM bronze_call_logs;

-- Silver: Route calls to new protocols
CREATE OR REFRESH MATERIALIZED VIEW silver_calls_routed AS
SELECT
  d.*,
  m.new_protocol_name,
  m.protocol_file
FROM silver_calls_deidentified d
JOIN bronze_protocol_mapping m
  ON d.legacy_protocol_used = m.legacy_protocol_name;
```

- [ ] **Step 2: Create the LDP pipeline on the workspace**

Use `@databricks-ai-dev-kit:databricks-spark-declarative-pipelines` or the create_pipeline MCP tool. Configure:
- Pipeline name: `gmr_protocol_analysis_pipeline`
- Target catalog: `morgancatalog`
- Target schema: `protocol_analysis`
- Source notebook: the uploaded `02_ldp_pipeline.sql`
- Serverless compute

- [ ] **Step 3: Run the pipeline and verify**

Trigger a pipeline update. After completion, verify:

```sql
SELECT COUNT(*) FROM morgancatalog.protocol_analysis.bronze_call_logs;
-- Expected: ~3,500-7,500 rows

SELECT COUNT(*) FROM morgancatalog.protocol_analysis.bronze_protocols;
-- Expected: ~470 rows (sum of all protocol files)

SELECT COUNT(*) FROM morgancatalog.protocol_analysis.silver_calls_deidentified;
-- Expected: same as bronze_call_logs, with PII redacted

SELECT COUNT(*) FROM morgancatalog.protocol_analysis.silver_calls_routed;
-- Expected: same as deidentified, with new_protocol_name added

-- Verify PII is redacted
SELECT caller_name, caller_phone, caller_address
FROM morgancatalog.protocol_analysis.silver_calls_deidentified LIMIT 5;
-- Expected: all show '[REDACTED]'
```

---

## Task 6: Narrative Generation Notebook

**Files:**
- Create: `notebooks/03_narrative_generation.py`

Convert grouped Q&A rows per call into a single narrative using `ai_query()`.

- [ ] **Step 1: Write the narrative generation notebook**

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Step 3: Generate Call Narratives
# MAGIC Converts de-identified Q&A rows into a single cohesive narrative per call.

# MAGIC %run ./00_config

# COMMAND ----------

# Use spark.sql with f-strings for variable interpolation
spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER_CALL_NARRATIVES} AS
WITH call_qa AS (
  SELECT
    call_id,
    complaint_type,
    actual_dispatch_type,
    on_scene_outcome,
    collect_list(
      struct(question_number, dispatcher_question, caller_answer)
      ORDER BY question_number
    ) AS qa_pairs
  FROM {SILVER_CALLS_DEIDENTIFIED}
  GROUP BY call_id, complaint_type, actual_dispatch_type, on_scene_outcome
)
SELECT
  call_id,
  complaint_type,
  actual_dispatch_type,
  on_scene_outcome,
  ai_query(
    '{LLM_MODEL}',
    CONCAT(
      'Convert the following dispatcher Q&A exchange into a single cohesive narrative. ',
      'Rules: preserve the callers exact wording, do not paraphrase, do not infer information not stated. ',
      'Write in past tense. Output only the narrative, no headers or labels.\\n\\n',
      to_json(qa_pairs)
    )
  ) AS narrative
FROM call_qa
""")
```

**Pattern note:** All notebooks use `spark.sql(f"...")` for SQL with Python config variables. This is the required pattern — do NOT use `# MAGIC %sql` with f-string interpolation.

- [ ] **Step 2: Upload and run the notebook**

Upload to workspace, attach to a SQL warehouse or cluster, run.

- [ ] **Step 3: Verify output**

```sql
SELECT COUNT(*) FROM morgancatalog.protocol_analysis.silver_call_narratives;
-- Expected: 500 rows (one per call)

SELECT call_id, LEFT(narrative, 200) FROM morgancatalog.protocol_analysis.silver_call_narratives LIMIT 3;
-- Verify narratives are coherent and preserve caller wording
```

---

## Task 7: Protocol Embedding & Vector Search Setup

**Files:**
- Create: `notebooks/04_protocol_embedding.py`

Generate embeddings for protocol rows and set up Vector Search index.

- [ ] **Step 1: Write the embedding notebook**

Use `@databricks-ai-dev-kit:databricks-vector-search` skill for correct Vector Search API.

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Step 4: Protocol Embedding & Vector Search

# MAGIC %run ./00_config

# COMMAND ----------

# Step 1: Create the protocol chunks table with embeddings
# NOTE: ai_query with returnType => 'ARRAY<FLOAT>' ensures Vector Search can sync embeddings
spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER_PROTOCOL_CHUNKS} AS
SELECT
  CONCAT(protocol_name, '_', row_number) AS chunk_id,
  protocol_name,
  CAST(row_number AS INT) AS row_number,
  module_name,
  CAST(disposition AS INT) AS disposition,
  stop_rule,
  CONCAT(
    'Protocol: ', protocol_name,
    ' | Module: ', module_name,
    ' | Question: ', question_text,
    ' | Condition: ', condition,
    ' | Disposition: ', disposition,
    ' | Stop Rule: ', stop_rule
  ) AS chunk_text,
  ai_query(
    '{EMBEDDING_MODEL}',
    CONCAT(question_text, ' ', condition, ' ', stop_rule),
    returnType => 'ARRAY<FLOAT>'
  ) AS embedding
FROM {BRONZE_PROTOCOLS}
""")

# COMMAND ----------

# Step 2: Create Vector Search endpoint and index
# Use MCP tools as primary approach: create_or_update_vs_endpoint, create_or_update_vs_index
# SDK fallback shown below:
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Create endpoint (if not exists)
try:
    w.vector_search_endpoints.create_endpoint(
        name=VS_ENDPOINT_NAME,
        endpoint_type="STANDARD"
    )
except Exception as e:
    print(f"Endpoint may already exist: {e}")

# Create delta sync index with pre-computed embeddings
w.vector_search_indexes.create_index(
    name=VS_INDEX_NAME,
    endpoint_name=VS_ENDPOINT_NAME,
    primary_key="chunk_id",
    index_type="DELTA_SYNC",
    delta_sync_index_spec={
        "source_table": SILVER_PROTOCOL_CHUNKS,
        "embedding_source_columns": [{"name": "embedding"}],
        "pipeline_type": "TRIGGERED",
        "columns_to_sync": [
            "chunk_id", "chunk_text", "protocol_name", "module_name",
            "row_number", "disposition", "stop_rule"
        ]
    }
)
```

**MCP tool alternative:** Use `create_or_update_vs_endpoint` and `create_or_update_vs_index` MCP tools as the primary approach. The SDK code above is a fallback.

- [ ] **Step 2: Upload and run the notebook**

- [ ] **Step 3: Verify the protocol chunks table**

```sql
SELECT COUNT(*) FROM morgancatalog.protocol_analysis.silver_protocol_chunks;
-- Expected: ~470 rows

SELECT chunk_id, protocol_name, row_number, LEFT(chunk_text, 100)
FROM morgancatalog.protocol_analysis.silver_protocol_chunks
WHERE protocol_name = 'Chest Pain'
ORDER BY row_number
LIMIT 5;
```

- [ ] **Step 4: Verify Vector Search index is synced**

Use `get_vs_index` MCP tool to check index status. Wait for sync to complete before proceeding to Task 8.

---

## Task 8: Protocol Execution Notebook

**Files:**
- Create: `notebooks/05_protocol_execution.py`

For each call: retrieve protocol rows from Vector Search, run LLM for step-by-step protocol execution.

- [ ] **Step 1: Write the protocol execution notebook**

This is the most complex notebook. The LLM receives:
- The call narrative
- The full ordered protocol rows for the routed protocol
- Instructions to walk through rows in order, apply stop rules, output disposition + audit trail

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Step 5: Protocol Execution
# MAGIC LLM walks through protocol rows against each call narrative.
# MAGIC Uses Vector Search for protocol retrieval (demo proof point for RAG).

# MAGIC %run ./00_config

# COMMAND ----------

# Step 1: Get routed calls with narratives (deduplicated to one row per call)
# silver_calls_routed has multiple rows per call (one per Q&A entry),
# so we aggregate to one row per call_id before joining narratives.
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW calls_with_narratives AS
SELECT
  r.call_id,
  r.complaint_type,
  r.new_protocol_name,
  r.actual_dispatch_type,
  r.on_scene_outcome,
  n.narrative
FROM (
  SELECT DISTINCT call_id, complaint_type, new_protocol_name,
         actual_dispatch_type, on_scene_outcome
  FROM {SILVER_CALLS_ROUTED}
) r
JOIN {SILVER_CALL_NARRATIVES} n ON r.call_id = n.call_id
""")

# COMMAND ----------

# Step 2: Retrieve protocol rows via Vector Search
# This demonstrates actual RAG — querying the Vector Search index
from databricks.sdk import WorkspaceClient
import json

w = WorkspaceClient()
vs_client = w.vector_search_indexes

# For each unique protocol, retrieve all rows ordered by row_number
protocols = spark.sql("SELECT DISTINCT new_protocol_name FROM calls_with_narratives").collect()

protocol_rows_map = {}
for row in protocols:
    protocol_name = row.new_protocol_name
    # Primary retrieval: filtered by protocol_name, return all rows
    results = vs_client.query_index(
        index_name=VS_INDEX_NAME,
        columns=["chunk_id", "chunk_text", "protocol_name", "module_name",
                 "row_number", "disposition", "stop_rule"],
        filters={"protocol_name": protocol_name},
        num_results=100  # more than max rows per protocol
    )
    # Sort by row_number for ordered execution
    sorted_rows = sorted(results.result.data_array,
                         key=lambda x: int(x[4]))  # row_number index
    protocol_rows_map[protocol_name] = sorted_rows

# Broadcast the map so it's available in SQL UDF or use it in Python processing
import pyspark.sql.functions as F

# COMMAND ----------

# Step 3: Execute protocol for each call via ai_query
# Parse JSON inline to avoid self-referential table overwrite
spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_PROTOCOL_EXECUTION} AS
WITH protocol_rows AS (
  SELECT
    protocol_name,
    collect_list(
      struct(row_number, module_name, chunk_text, disposition, stop_rule)
      ORDER BY row_number
    ) AS protocol_rows_json
  FROM {SILVER_PROTOCOL_CHUNKS}
  GROUP BY protocol_name
),
raw_execution AS (
  SELECT
    c.call_id,
    c.complaint_type,
    c.new_protocol_name AS protocol_name,
    ai_query(
      '{LLM_MODEL}',
      CONCAT(
        'You are a clinical protocol execution engine. Walk through the protocol rows below IN ORDER against the patient call narrative. For each row, determine if the condition is met based on the narrative. Apply stop rules exactly. Output ONLY valid JSON (no markdown, no code fences) with these fields:\\n',
        '- final_disposition: integer 0-5 or null if non-replayable\\n',
        '- disposition_set: array of integers if ambiguous (e.g. [0,1])\\n',
        '- is_ambiguous: boolean\\n',
        '- is_replayable: boolean\\n',
        '- non_replayable_reason: string or null\\n',
        '- triggering_row: integer row number that triggered final disposition\\n',
        '- triggering_module: string module name\\n',
        '- audit_trail: array of objects with row_number, result (pass/fail/skip), reasoning\\n\\n',
        'PATIENT CALL NARRATIVE:\\n', c.narrative, '\\n\\n',
        'PROTOCOL ROWS (execute in order):\\n', to_json(p.protocol_rows_json)
      )
    ) AS execution_result
  FROM calls_with_narratives c
  JOIN protocol_rows p ON c.new_protocol_name = p.protocol_name
)
SELECT
  call_id,
  complaint_type,
  protocol_name,
  execution_result:final_disposition::INT AS final_disposition,
  execution_result:disposition_set AS disposition_set,
  execution_result:is_ambiguous::BOOLEAN AS is_ambiguous,
  execution_result:is_replayable::BOOLEAN AS is_replayable,
  execution_result:non_replayable_reason::STRING AS non_replayable_reason,
  execution_result:triggering_row::INT AS triggering_row,
  execution_result:triggering_module::STRING AS triggering_module,
  execution_result:audit_trail AS audit_trail
FROM raw_execution
""")

# COMMAND ----------

# Step 4 (optional): Cross-protocol similarity check via Vector Search
# For ambiguous calls, query VS without protocol filter to find better-matching protocols
ambiguous_calls = spark.sql(f"""
  SELECT call_id, narrative, protocol_name
  FROM {GOLD_PROTOCOL_EXECUTION} e
  JOIN {SILVER_CALL_NARRATIVES} n USING (call_id)
  WHERE e.is_ambiguous = true
""").collect()

# This demonstrates genuine RAG similarity search (not just filtered lookup)
for call in ambiguous_calls[:5]:  # sample for demo
    results = vs_client.query_index(
        index_name=VS_INDEX_NAME,
        query_text=call.narrative,
        columns=["protocol_name", "chunk_text", "disposition"],
        num_results=10
        # No protocol filter — find best match across all protocols
    )
    print(f"Call {call.call_id} (routed to {call.protocol_name}): "
          f"VS top matches: {set(r[0] for r in results.result.data_array)}")
```

**Key design decisions:**
- Step 2 demonstrates Vector Search retrieval as a demo proof point
- Step 3 uses a CTE (`raw_execution`) to avoid self-referential table overwrite — JSON is parsed inline in the same CTAS
- `calls_with_narratives` deduplicates `silver_calls_routed` to one row per call via `DISTINCT`
- Step 4 shows genuine similarity-based cross-protocol discovery for ambiguous calls

- [ ] **Step 2: Upload and run the notebook**

This will take the longest — 500 calls x LLM execution. Monitor for rate limiting.

- [ ] **Step 3: Verify output**

```sql
SELECT COUNT(*) FROM morgancatalog.protocol_analysis.gold_protocol_execution;
-- Expected: 500 rows

SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN is_ambiguous THEN 1 ELSE 0 END) AS ambiguous,
  SUM(CASE WHEN NOT is_replayable THEN 1 ELSE 0 END) AS non_replayable
FROM morgancatalog.protocol_analysis.gold_protocol_execution;
-- Expected: ~75 ambiguous, ~25 non-replayable

SELECT call_id, final_disposition, is_ambiguous, triggering_row
FROM morgancatalog.protocol_analysis.gold_protocol_execution LIMIT 5;
```

---

## Task 9: Evaluation Summary Notebook

**Files:**
- Create: `notebooks/06_evaluation_summary.py`

- [ ] **Step 1: Write the evaluation notebook**

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Step 6: Clinical Evaluation Summaries

# MAGIC %run ./00_config

# COMMAND ----------

# Inline JSON parsing via CTE to avoid self-referential overwrite
spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_EVALUATION_SUMMARIES} AS
WITH raw_eval AS (
  SELECT
    e.call_id,
    ai_query(
      '{LLM_MODEL}',
      CONCAT(
        'You are a clinical quality reviewer. Based on the protocol execution results below, write a brief clinical evaluation. Output ONLY valid JSON with:\\n',
        '- disposition_reasoning: why this disposition was reached\\n',
        '- ambiguity_explanation: if ambiguous, what info was missing or conflicting (null if not ambiguous)\\n',
        '- cross_protocol_flag: boolean, true if a different protocol may have been more appropriate\\n',
        '- suggested_protocol: if cross_protocol_flag is true, which protocol and why (null otherwise)\\n',
        '- evaluation_text: 2-3 sentence clinical evaluation summary\\n\\n',
        'EXECUTION RESULTS:\\n',
        'Protocol: ', e.protocol_name, '\\n',
        'Disposition: ', CAST(e.final_disposition AS STRING), '\\n',
        'Ambiguous: ', CAST(e.is_ambiguous AS STRING), '\\n',
        'Audit Trail: ', e.audit_trail, '\\n',
        'Complaint Type: ', e.complaint_type
      )
    ) AS eval_result
  FROM {GOLD_PROTOCOL_EXECUTION} e
)
SELECT
  call_id,
  eval_result:disposition_reasoning::STRING AS disposition_reasoning,
  eval_result:ambiguity_explanation::STRING AS ambiguity_explanation,
  eval_result:cross_protocol_flag::BOOLEAN AS cross_protocol_flag,
  eval_result:suggested_protocol::STRING AS suggested_protocol,
  eval_result:evaluation_text::STRING AS evaluation_text
FROM raw_eval
""")
```

- [ ] **Step 2: Upload and run**

- [ ] **Step 3: Verify output**

```sql
SELECT COUNT(*) FROM morgancatalog.protocol_analysis.gold_evaluation_summaries;
-- Expected: 500

SELECT call_id, cross_protocol_flag, LEFT(evaluation_text, 150)
FROM morgancatalog.protocol_analysis.gold_evaluation_summaries
WHERE cross_protocol_flag = true
LIMIT 5;
```

---

## Task 10: Care Assessment Notebook

**Files:**
- Create: `notebooks/07_care_assessment.py`

- [ ] **Step 1: Write the care assessment notebook**

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Step 7: Care Assessment (Dispatch Necessity, Tiering, Quality)

# MAGIC %run ./00_config

# COMMAND ----------

# Inline JSON parsing via CTE
spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_CARE_ASSESSMENT} AS
WITH raw_assessment AS (
  SELECT
    e.call_id,
    ai_query(
      '{LLM_MODEL}',
      CONCAT(
        'You are a clinical operations analyst. Assess the following completed call. Output ONLY valid JSON with:\\n',
        '- dispatch_was_necessary: "yes", "no", or "borderline"\\n',
        '- dispatch_necessity_reasoning: why dispatch was or was not needed\\n',
        '- correct_vehicle_tier: "BLS", "ALS", "supervisor", or "no_dispatch" based on disposition\\n',
        '- actual_vehicle_tier: (provided below)\\n',
        '- vehicle_tier_match: "match", "over_dispatched", or "under_dispatched"\\n',
        '- quality_score: 1-5 (5=perfect alignment with protocol)\\n',
        '- quality_findings: what went right or wrong\\n',
        '- cost_impact_flag: "savings_possible", "correct_spend", or "under_resourced"\\n\\n',
        'CALL DATA:\\n',
        'Complaint: ', e.complaint_type, '\\n',
        'Protocol Disposition: ', CAST(e.final_disposition AS STRING), '\\n',
        'Actual Dispatch: ', n.actual_dispatch_type, '\\n',
        'On-Scene Outcome: ', n.on_scene_outcome, '\\n',
        'Evaluation: ', ev.evaluation_text, '\\n',
        'Ambiguous: ', CAST(e.is_ambiguous AS STRING), '\\n',
        'Audit Trail: ', e.audit_trail
      )
    ) AS assessment_result
  FROM {GOLD_PROTOCOL_EXECUTION} e
  JOIN {SILVER_CALL_NARRATIVES} n ON e.call_id = n.call_id
  JOIN {GOLD_EVALUATION_SUMMARIES} ev ON e.call_id = ev.call_id
)
SELECT
  call_id,
  assessment_result:dispatch_was_necessary::STRING AS dispatch_was_necessary,
  assessment_result:dispatch_necessity_reasoning::STRING AS dispatch_necessity_reasoning,
  assessment_result:correct_vehicle_tier::STRING AS correct_vehicle_tier,
  assessment_result:actual_vehicle_tier::STRING AS actual_vehicle_tier,
  assessment_result:vehicle_tier_match::STRING AS vehicle_tier_match,
  assessment_result:quality_score::INT AS quality_score,
  assessment_result:quality_findings::STRING AS quality_findings,
  assessment_result:cost_impact_flag::STRING AS cost_impact_flag
FROM raw_assessment
""")
```

- [ ] **Step 2: Upload and run**

- [ ] **Step 3: Verify output**

```sql
SELECT
  dispatch_was_necessary,
  COUNT(*) AS cnt
FROM morgancatalog.protocol_analysis.gold_care_assessment
GROUP BY dispatch_was_necessary;

SELECT
  vehicle_tier_match,
  COUNT(*) AS cnt
FROM morgancatalog.protocol_analysis.gold_care_assessment
GROUP BY vehicle_tier_match;

SELECT AVG(quality_score) AS avg_quality
FROM morgancatalog.protocol_analysis.gold_care_assessment;
```

---

## Task 11: Analytics Aggregation

**Files:**
- Create: `notebooks/08_analytics_aggregation.sql`

- [ ] **Step 1: Write the aggregation SQL notebook**

```sql
-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Step 8: Analytics Aggregation

CREATE OR REPLACE TABLE morgancatalog.protocol_analysis.gold_analytics_summary AS
SELECT
  e.complaint_type,
  e.protocol_name,
  COUNT(*) AS total_calls,
  -- Disposition distribution
  SUM(CASE WHEN e.final_disposition = 0 THEN 1 ELSE 0 END) AS disposition_0,
  SUM(CASE WHEN e.final_disposition = 1 THEN 1 ELSE 0 END) AS disposition_1,
  SUM(CASE WHEN e.final_disposition = 2 THEN 1 ELSE 0 END) AS disposition_2,
  SUM(CASE WHEN e.final_disposition = 3 THEN 1 ELSE 0 END) AS disposition_3,
  SUM(CASE WHEN e.final_disposition = 4 THEN 1 ELSE 0 END) AS disposition_4,
  SUM(CASE WHEN e.final_disposition = 5 THEN 1 ELSE 0 END) AS disposition_5,
  -- Ambiguity & replayability
  ROUND(100.0 * SUM(CASE WHEN e.is_ambiguous THEN 1 ELSE 0 END) / COUNT(*), 1) AS ambiguity_rate_pct,
  ROUND(100.0 * SUM(CASE WHEN e.is_replayable THEN 1 ELSE 0 END) / COUNT(*), 1) AS replayability_rate_pct,
  -- Dispatch necessity
  SUM(CASE WHEN a.dispatch_was_necessary = 'no' THEN 1 ELSE 0 END) AS unnecessary_dispatches,
  SUM(CASE WHEN a.dispatch_was_necessary = 'borderline' THEN 1 ELSE 0 END) AS borderline_dispatches,
  -- Vehicle tiering
  SUM(CASE WHEN a.vehicle_tier_match = 'over_dispatched' THEN 1 ELSE 0 END) AS over_dispatched,
  SUM(CASE WHEN a.vehicle_tier_match = 'under_dispatched' THEN 1 ELSE 0 END) AS under_dispatched,
  SUM(CASE WHEN a.vehicle_tier_match = 'match' THEN 1 ELSE 0 END) AS correct_tier,
  -- Quality
  ROUND(AVG(a.quality_score), 2) AS avg_quality_score,
  -- Cross-protocol
  SUM(CASE WHEN ev.cross_protocol_flag THEN 1 ELSE 0 END) AS cross_protocol_flags
FROM morgancatalog.protocol_analysis.gold_protocol_execution e
JOIN morgancatalog.protocol_analysis.gold_care_assessment a ON e.call_id = a.call_id
JOIN morgancatalog.protocol_analysis.gold_evaluation_summaries ev ON e.call_id = ev.call_id
GROUP BY e.complaint_type, e.protocol_name;
```

- [ ] **Step 2: Upload and run**

- [ ] **Step 3: Verify**

```sql
SELECT * FROM morgancatalog.protocol_analysis.gold_analytics_summary ORDER BY total_calls DESC;
-- Expected: 10 rows (one per protocol), totaling 500 calls
```

---

## Task 12: Export for Human Review

**Files:**
- Create: `notebooks/09_export_review.py`

- [ ] **Step 1: Write the export notebook**

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Step 9: Export Flagged Results for Clinical Review

# MAGIC %run ./00_config

from datetime import datetime

# Query flagged results
flagged_df = spark.sql(f"""
  SELECT
    e.call_id,
    e.complaint_type,
    e.protocol_name,
    e.final_disposition,
    e.is_ambiguous,
    e.is_replayable,
    e.non_replayable_reason,
    e.audit_trail,
    ev.evaluation_text,
    ev.cross_protocol_flag,
    ev.suggested_protocol,
    a.dispatch_was_necessary,
    a.vehicle_tier_match,
    a.quality_score,
    a.quality_findings
  FROM {GOLD_PROTOCOL_EXECUTION} e
  JOIN {GOLD_EVALUATION_SUMMARIES} ev ON e.call_id = ev.call_id
  JOIN {GOLD_CARE_ASSESSMENT} a ON e.call_id = a.call_id
  WHERE e.is_ambiguous = true
     OR ev.cross_protocol_flag = true
     OR a.quality_score <= 2
     OR a.dispatch_was_necessary = 'no'
""")

# Ensure exports directory exists
dbutils.fs.mkdirs(f"{VOLUME_PATH}/exports")

# Write to volume as CSV
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
export_path = f"{VOLUME_PATH}/exports/review_batch_{timestamp}.csv"
flagged_df.toPandas().to_csv(export_path.replace("dbfs:", ""), index=False)

print(f"Exported {flagged_df.count()} flagged records to {export_path}")
```

- [ ] **Step 2: Upload and run**

- [ ] **Step 3: Verify export file exists**

```sql
LIST '/Volumes/morgancatalog/protocol_analysis/source_data/exports/';
```

---

## Task 13: AI/BI Lakeview Dashboard

**Files:** Dashboard created via API (no local file)

Use `@databricks-ai-dev-kit:databricks-aibi-dashboards` skill. CRITICAL: Test ALL SQL queries via `execute_sql` BEFORE deploying.

- [ ] **Step 1: Test all dashboard queries**

Test each query against the gold tables to make sure they return valid data. Run all via `execute_sql`:

**Page 1 — Executive Overview:**
```sql
-- Total calls (counter)
SELECT COUNT(*) AS total_calls FROM morgancatalog.protocol_analysis.gold_protocol_execution;

-- Disposition distribution (bar chart)
SELECT final_disposition, COUNT(*) AS cnt
FROM morgancatalog.protocol_analysis.gold_protocol_execution
WHERE final_disposition IS NOT NULL
GROUP BY final_disposition ORDER BY final_disposition;

-- Ambiguity rate (gauge)
SELECT ROUND(100.0 * SUM(CASE WHEN is_ambiguous THEN 1 ELSE 0 END) / COUNT(*), 1) AS ambiguity_rate
FROM morgancatalog.protocol_analysis.gold_protocol_execution;

-- Replayability rate (gauge)
SELECT ROUND(100.0 * SUM(CASE WHEN is_replayable THEN 1 ELSE 0 END) / COUNT(*), 1) AS replayability_rate
FROM morgancatalog.protocol_analysis.gold_protocol_execution;

-- Non-replayable calls with top reasons (table)
SELECT non_replayable_reason, COUNT(*) AS cnt
FROM morgancatalog.protocol_analysis.gold_protocol_execution
WHERE is_replayable = false
GROUP BY non_replayable_reason ORDER BY cnt DESC;
```

**Page 2 — Dispatch Necessity:**
```sql
-- Dispatch necessity breakdown (pie chart)
SELECT dispatch_was_necessary, COUNT(*) AS cnt
FROM morgancatalog.protocol_analysis.gold_care_assessment
GROUP BY dispatch_was_necessary;

-- Unnecessary dispatch by complaint type (bar chart)
SELECT e.complaint_type, COUNT(*) AS unnecessary
FROM morgancatalog.protocol_analysis.gold_care_assessment a
JOIN morgancatalog.protocol_analysis.gold_protocol_execution e ON a.call_id = e.call_id
WHERE a.dispatch_was_necessary = 'no'
GROUP BY e.complaint_type ORDER BY unnecessary DESC;

-- Calls that could have been phone-resolved (drill-down table)
SELECT e.call_id, e.complaint_type, e.protocol_name, e.final_disposition,
       a.dispatch_necessity_reasoning, a.cost_impact_flag
FROM morgancatalog.protocol_analysis.gold_care_assessment a
JOIN morgancatalog.protocol_analysis.gold_protocol_execution e ON a.call_id = e.call_id
WHERE a.dispatch_was_necessary = 'no';

-- Estimated cost savings (counter) — assumes ~$500 avg cost per unnecessary dispatch
SELECT COUNT(*) AS unnecessary_dispatches,
       COUNT(*) * 500 AS estimated_savings_usd
FROM morgancatalog.protocol_analysis.gold_care_assessment
WHERE dispatch_was_necessary = 'no';
```

**Page 3 — Vehicle Tiering:**
```sql
-- Actual vs recommended tier (stacked bar)
SELECT vehicle_tier_match, COUNT(*) AS cnt
FROM morgancatalog.protocol_analysis.gold_care_assessment
GROUP BY vehicle_tier_match;

-- Over-dispatch rate by protocol (bar chart)
SELECT e.protocol_name, COUNT(*) AS over_dispatched
FROM morgancatalog.protocol_analysis.gold_care_assessment a
JOIN morgancatalog.protocol_analysis.gold_protocol_execution e ON a.call_id = e.call_id
WHERE a.vehicle_tier_match = 'over_dispatched'
GROUP BY e.protocol_name ORDER BY over_dispatched DESC;

-- Under-dispatch rate by protocol (bar chart — safety concern)
SELECT e.protocol_name, COUNT(*) AS under_dispatched
FROM morgancatalog.protocol_analysis.gold_care_assessment a
JOIN morgancatalog.protocol_analysis.gold_protocol_execution e ON a.call_id = e.call_id
WHERE a.vehicle_tier_match = 'under_dispatched'
GROUP BY e.protocol_name ORDER BY under_dispatched DESC;

-- Tier mismatch detail table (sortable)
SELECT e.call_id, e.complaint_type, e.protocol_name, e.final_disposition,
       a.correct_vehicle_tier, a.actual_vehicle_tier, a.vehicle_tier_match,
       a.quality_findings
FROM morgancatalog.protocol_analysis.gold_care_assessment a
JOIN morgancatalog.protocol_analysis.gold_protocol_execution e ON a.call_id = e.call_id
WHERE a.vehicle_tier_match != 'match';
```

**Page 4 — Quality of Care:**
```sql
-- Quality score distribution (histogram)
SELECT quality_score, COUNT(*) AS cnt
FROM morgancatalog.protocol_analysis.gold_care_assessment
GROUP BY quality_score ORDER BY quality_score;

-- Average quality score by protocol (bar chart)
SELECT e.protocol_name, ROUND(AVG(a.quality_score), 2) AS avg_quality
FROM morgancatalog.protocol_analysis.gold_care_assessment a
JOIN morgancatalog.protocol_analysis.gold_protocol_execution e ON a.call_id = e.call_id
GROUP BY e.protocol_name ORDER BY avg_quality;

-- Cross-protocol flags (table)
SELECT e.call_id, e.complaint_type, e.protocol_name, ev.suggested_protocol, ev.evaluation_text
FROM morgancatalog.protocol_analysis.gold_evaluation_summaries ev
JOIN morgancatalog.protocol_analysis.gold_protocol_execution e ON ev.call_id = e.call_id
WHERE ev.cross_protocol_flag = true;

-- Lowest quality calls with audit trails (detail table)
SELECT e.call_id, e.complaint_type, e.protocol_name, a.quality_score,
       a.quality_findings, e.audit_trail
FROM morgancatalog.protocol_analysis.gold_care_assessment a
JOIN morgancatalog.protocol_analysis.gold_protocol_execution e ON a.call_id = e.call_id
WHERE a.quality_score <= 2
ORDER BY a.quality_score;
```

**Dashboard filters (configure on all pages):** complaint_type, protocol_name, final_disposition, is_ambiguous, timestamp (date range)

- [ ] **Step 2: Create the dashboard**

Use the `create_or_update_dashboard` MCP tool with the spec from Section 7 of the design doc. 4 pages: Executive Overview, Dispatch Necessity, Vehicle Tiering, Quality of Care.

- [ ] **Step 3: Publish the dashboard**

Use `publish_dashboard` MCP tool.

- [ ] **Step 4: Verify dashboard renders correctly**

Open the dashboard URL and confirm all widgets show data.

---

## Task 14: AI/BI Genie Space

**Files:** Genie Space created via API (no local file)

Use `@databricks-ai-dev-kit:databricks-genie` skill.

- [ ] **Step 1: Create the Genie Space**

Use `create_or_update_genie` MCP tool. Configure:
- Name: `GMR Protocol Analysis Explorer`
- Tables: `gold_protocol_execution`, `gold_evaluation_summaries`, `gold_care_assessment`, `gold_analytics_summary`, `silver_call_narratives`
- Instructions: Include domain context from spec Section 8.3 (disposition scale, vehicle tiers, ambiguity/replayability definitions, quality score meaning)
- Sample questions from spec Section 8.2

- [ ] **Step 2: Test the Genie Space**

Use `ask_genie` MCP tool to test each sample question:
- "Which protocols have the highest ambiguity rate?"
- "Show me all chest pain calls where dispatch was unnecessary"
- "How many calls were over-dispatched with ALS?"
- "What's the average quality score for breathing problems?"

Verify responses make sense and return correct data.

---

## Task 15: Workflow Orchestration

**Files:** Workflow created via API (no local file)

- [ ] **Step 1: Create the Databricks Workflow**

Use `manage_jobs` MCP tool or `@databricks-ai-dev-kit:databricks-jobs` skill. Create a multi-task job:

```
Job name: gmr_protocol_analysis_workflow

Task dependencies:
  01_synthetic_data_gen → 02_ldp_pipeline_trigger
  02_ldp_pipeline_trigger → [03_narrative_generation, 04_protocol_embedding] (parallel)
  [03_narrative_generation, 04_protocol_embedding] → 05_protocol_execution
  05_protocol_execution → 06_evaluation_summary
  06_evaluation_summary → 07_care_assessment
  07_care_assessment → 08_analytics_aggregation
  08_analytics_aggregation → 09_export_review
```

Each task runs the corresponding notebook. The LDP pipeline trigger task should use the `pipeline_task` type to run the LDP pipeline.

- [ ] **Step 2: Run the full workflow end-to-end**

Trigger the job and monitor. This is the full demo run.

- [ ] **Step 3: Verify all outputs after workflow completes**

Run a final validation:
```sql
SELECT 'bronze_call_logs' AS tbl, COUNT(*) AS cnt FROM morgancatalog.protocol_analysis.bronze_call_logs
UNION ALL SELECT 'bronze_protocols', COUNT(*) FROM morgancatalog.protocol_analysis.bronze_protocols
UNION ALL SELECT 'silver_calls_deidentified', COUNT(*) FROM morgancatalog.protocol_analysis.silver_calls_deidentified
UNION ALL SELECT 'silver_call_narratives', COUNT(*) FROM morgancatalog.protocol_analysis.silver_call_narratives
UNION ALL SELECT 'silver_calls_routed', COUNT(*) FROM morgancatalog.protocol_analysis.silver_calls_routed
UNION ALL SELECT 'silver_protocol_chunks', COUNT(*) FROM morgancatalog.protocol_analysis.silver_protocol_chunks
UNION ALL SELECT 'gold_protocol_execution', COUNT(*) FROM morgancatalog.protocol_analysis.gold_protocol_execution
UNION ALL SELECT 'gold_evaluation_summaries', COUNT(*) FROM morgancatalog.protocol_analysis.gold_evaluation_summaries
UNION ALL SELECT 'gold_care_assessment', COUNT(*) FROM morgancatalog.protocol_analysis.gold_care_assessment
UNION ALL SELECT 'gold_analytics_summary', COUNT(*) FROM morgancatalog.protocol_analysis.gold_analytics_summary;
```

Confirm dashboard and Genie Space reflect the final data.
