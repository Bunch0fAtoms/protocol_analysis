# Gen AI Protocol Analysis Demo — Design Spec

**Customer:** Global Medical Response, Inc. (GMR)
**Use Case:** Gen AI Protocol Analysis (Clinical Data Abstraction)
**Demo Goal:** Prove to executive sponsor (Mukund, VP Data Science & AI) that Databricks can handle the full automated clinical protocol assessment pipeline end-to-end
**Date:** 2026-03-26

---

## 1. Overview

GMR dispatches ambulances using MPDS-style clinical protocols. They want to run historical patient call data through clinical protocols using an LLM + RAG system inside Databricks to assess:

1. **Quality of Care** — Were the right clinical decisions made?
2. **Dispatch Necessity** — Should an ambulance have been sent, or could the patient have been helped over the phone?
3. **Vehicle Tiering** — Was the correct level of response sent (BLS vs ALS vs supervisor)?

This demo builds a complete pipeline using synthetic data that mirrors GMR's real workflow, proving Databricks platform capability.

---

## 2. Architecture

**Approach:** Lakeflow Declarative Pipeline (LDP) with medallion architecture + notebooks for LLM steps + AI/BI Dashboard + Genie Space

**Key technology choices:**
- `ai_query()` for all LLM calls (narrative generation, protocol execution, evaluation)
- Managed Vector Search synced from Delta table for protocol RAG
- Lakeflow Declarative Pipeline for ingestion and non-LLM transformations
- Databricks Workflow to orchestrate LDP pipeline + notebook LLM tasks
- AI/BI Lakeview Dashboard for structured analytics
- AI/BI Genie Space for ad-hoc natural language exploration

**Division of labor:**
- **Built by Vibe:** Synthetic data, LDP pipeline, Vector Search, LLM transformation notebooks, Dashboard, Genie Space, Excel export
- **Built by Morgan:** Analysis notebook

---

## 3. Synthetic Data Generation

A standalone notebook generates all source data and uploads to a Unity Catalog volume as CSV files. (CSV is used instead of Excel because `read_files`/Auto Loader does not natively parse `.xlsx` format. For the demo, GMR's Excel workflow is simulated by generating CSVs that represent the same tabular structure.)

### 3.1 Patient Call Logs

**File:** `calls/call_logs_batch_001.csv`
**Volume:** 500 synthetic patient calls, each with 5-15 Q&A rows

**Fields per row:**
- `call_id` — unique identifier
- `timestamp` — call date/time
- `legacy_protocol_used` — which protocol the dispatcher originally followed
- `question_number` — sequence within the call
- `dispatcher_question` — what the dispatcher asked
- `caller_answer` — what the caller said (verbatim wording)
- `caller_name` — fake PII (for de-identification demo)
- `caller_phone` — fake PII
- `caller_address` — fake PII
- `actual_dispatch_type` — what was actually sent (BLS, ALS, supervisor, no dispatch)
- `complaint_type` — chief complaint category (e.g., "Chest Pain", "Breathing Problems") derived from `legacy_protocol_used`
- `on_scene_outcome` — transported to ER, treated on scene, refusal, phone-resolved, etc.

**Distribution by complaint type (weighted realistically):**
- Chest Pain / Heart Problems: ~20% (100 calls)
- Breathing Problems / Respiratory Distress: ~20% (100 calls)
- Falls / Traumatic Injuries: ~15% (75 calls)
- Abdominal Pain: ~10% (50 calls)
- Allergic Reaction / Anaphylaxis: ~8% (40 calls)
- Seizures: ~7% (35 calls)
- Overdose / Poisoning: ~7% (35 calls)
- Stroke / Neurological: ~5% (25 calls)
- Pregnancy / Childbirth Complications: ~4% (20 calls)
- Unconscious / Unresponsive: ~4% (20 calls)

**Severity variation:** Calls span the full disposition range (0-5) with realistic distribution — most calls cluster at moderate severity (2-3), with tails at both extremes. ~15% of calls are designed to produce ambiguous dispositions. ~5% are designed to be non-replayable (insufficient information).

### 3.2 Clinical Protocol Files

**10 Excel files** in `protocols/` directory, each structured as MPDS-style triage protocols with 40-80 rows.

**Protocol list:**
1. `chest_pain.csv` (~60 rows)
2. `breathing_problems.csv` (~55 rows)
3. `falls_traumatic_injury.csv` (~50 rows)
4. `abdominal_pain.csv` (~45 rows)
5. `allergic_reaction.csv` (~45 rows)
6. `seizures.csv` (~40 rows)
7. `overdose_poisoning.csv` (~40 rows)
8. `stroke_neurological.csv` (~45 rows)
9. `pregnancy_complications.csv` (~40 rows)
10. `unconscious_unresponsive.csv` (~50 rows)

**Fields per protocol row:**
- `row_number` — execution order
- `module_name` — Case Entry, Key Questions, Determinant Assignment, Pre-Arrival Instructions, Case Exit
- `question_text` — the scripted question or instruction
- `condition` — what triggers this row (e.g., "severe pain", "duration > 4 hours", "unconscious")
- `disposition` — numeric 0-5 (0 = most urgent / immediate dispatch, 5 = self-care)
- `stop_rule` — STOP (dispatch immediately), CONTINUE, ELEVATE (combine with other rows), conditional logic

**Protocol structure per file (MPDS-style):**
- **Case Entry module:** Verify location, callback number, "tell me exactly what happened"
- **Key Questions module:** Scripted triage questions specific to complaint type
- **Determinant Assignment module:** Decision logic mapping answers to Alpha/Bravo/Charlie/Delta/Echo severity with corresponding dispositions
- **Pre-Arrival Instructions module:** Word-for-word instructions for caller while units are en route
- **Case Exit module:** Standard closing instructions

### 3.3 Protocol Mapping Table

**File:** `mappings/protocol_mapping.csv`

| Field | Description |
|-------|-------------|
| `legacy_protocol_name` | Protocol name used in original call |
| `new_protocol_name` | Updated protocol to replay against |
| `protocol_file` | Filename of the new protocol CSV |

---

## 4. Unity Catalog Schema & Delta Tables

**Catalog/Schema:** Parameterized (e.g., `morgancatalog.protocol_analysis`)

### 4.1 Bronze Tables (Raw Ingestion)

| Table | Source | Description |
|-------|--------|-------------|
| `bronze_call_logs` | CSV from UC Volume | Raw Q&A rows — one row per question/answer per call |
| `bronze_protocols` | CSV from UC Volume | Raw protocol rows from all 10 files |
| `bronze_protocol_mapping` | CSV from UC Volume | Legacy to new protocol mapping |

### 4.2 Silver Tables (Transformed)

| Table | Description |
|-------|-------------|
| `silver_calls_deidentified` | PII stripped via regex (names, phones, addresses removed) |
| `silver_call_narratives` | One row per call — `ai_query()` converts Q&A into single narrative preserving exact caller wording |
| `silver_calls_routed` | Each call joined to new protocol via mapping table |
| `silver_protocol_chunks` | Protocol rows with text embeddings for Vector Search sync |

### 4.3 Gold Tables (Results)

| Table | Description |
|-------|-------------|
| `gold_protocol_execution` | Per-call: final disposition, ambiguity flag, replayability flag, audit trail of rows triggered/failed |
| `gold_evaluation_summaries` | Per-call: LLM clinical evaluation — disposition reasoning, ambiguity explanation, cross-protocol flags |
| `gold_care_assessment` | Per-call: dispatch necessity, vehicle tier assessment, quality score, cost impact flag |
| `gold_analytics_summary` | Aggregated metrics — disposition distributions, ambiguity rates, replayability rates by protocol |

### 4.4 Gold Protocol Execution Fields

| Field | Description |
|-------|-------------|
| `call_id` | Reference |
| `complaint_type` | Chief complaint category |
| `protocol_name` | New protocol used for replay |
| `final_disposition` | Numeric 0-5, or NULL if non-replayable |
| `disposition_set` | Array of possible dispositions if ambiguous (e.g., [0,1]) |
| `is_ambiguous` | Boolean — could not resolve to single disposition |
| `is_replayable` | Boolean — enough info in call to execute protocol |
| `non_replayable_reason` | Why the call couldn't be replayed (if applicable) |
| `triggering_row` | Row number that triggered the final disposition |
| `triggering_module` | Module name of the triggering row |
| `audit_trail` | JSON array — per-row trace of pass/fail/skip with reasoning |

### 4.5 Gold Evaluation Summary Fields

| Field | Description |
|-------|-------------|
| `call_id` | Reference |
| `disposition_reasoning` | Why the disposition triggered |
| `ambiguity_explanation` | If ambiguous, what information was missing or conflicting |
| `cross_protocol_flag` | Boolean — LLM suggests a different protocol may have been more appropriate |
| `suggested_protocol` | If cross-protocol flagged, which protocol and why |
| `evaluation_text` | Full clinical evaluation narrative |

### 4.6 Gold Care Assessment Fields

| Field | Description |
|-------|-------------|
| `call_id` | Reference |
| `dispatch_was_necessary` | LLM assessment: yes / no / borderline |
| `dispatch_necessity_reasoning` | Why dispatch was/wasn't needed |
| `correct_vehicle_tier` | LLM-recommended tier based on protocol disposition |
| `actual_vehicle_tier` | What was actually sent |
| `vehicle_tier_match` | match / over-dispatched / under-dispatched |
| `quality_score` | 1-5 rating of care delivery alignment with protocol |
| `quality_findings` | What went right/wrong in clinical decision chain |
| `cost_impact_flag` | Potential savings if correct tier had been dispatched |

---

## 5. Lakeflow Declarative Pipeline

A single LDP pipeline definition with SQL transformations for bronze and non-LLM silver tables. LLM-dependent silver/gold steps run as notebook tasks in the same Databricks Workflow after the LDP pipeline completes (due to potential `ai_query()` limitations in materialized views).

### 5.1 Pipeline Flow

```
=== LDP PIPELINE (Lakeflow Declarative Pipeline) ===

UC Volume (CSV files)
  |
  | Auto Loader / read_files
  v
bronze_call_logs    bronze_protocols    bronze_protocol_mapping
  |                       |
  v                       |
silver_calls_deidentified (regex PII removal)
  |                       |
  v                       |
silver_calls_routed       |
(JOIN deidentified calls  |
 + mapping table)         |

=== NOTEBOOK TASKS (Databricks Workflow, after LDP) ===

silver_calls_deidentified                bronze_protocols
  |                                           |
  v                                           v
silver_call_narratives                   silver_protocol_chunks
(ai_query: grouped Q&A -> narrative)     (ai_query: generate embeddings)
                                              |
                                              v
                                         Vector Search Index
                                         (managed sync)

silver_calls_routed + silver_call_narratives + Vector Search
  |                          |                      |
  +----------+---------------+----------------------+
             |
             v
  gold_protocol_execution
  (ai_query: narrative + ordered protocol rows -> disposition + audit)
             |
             v
  gold_evaluation_summaries
  (ai_query: execution results -> clinical evaluation)
             |
             v
  gold_care_assessment
  (ai_query: assess dispatch necessity, tiering, quality)
             |
             v
  gold_analytics_summary
  (SQL aggregations across all results)
             |
             v
  Excel Export (to UC Volume for human reviewers)
```

### 5.2 LDP Pipeline (Bronze + Non-LLM Silver)

- **bronze_call_logs:** Streaming table using `read_files` from UC Volume, parsing CSV
- **bronze_protocols:** Streaming table using `read_files` from UC Volume, parsing CSV
- **bronze_protocol_mapping:** Streaming table using `read_files` from UC Volume, parsing CSV
- **silver_calls_deidentified:** Materialized view with `regexp_replace` to strip PII from bronze_call_logs
- **silver_calls_routed:** Materialized view joining `silver_calls_deidentified` (not bronze) to new protocols via `bronze_protocol_mapping` — ensures PII-free data flows downstream

### 5.3 Notebook Tasks (LLM Steps)

Run as tasks in the Databricks Workflow, sequenced after the LDP pipeline:

1. **Narrative Generation Notebook** — reads `silver_calls_deidentified`, uses `ai_query()` to convert grouped Q&A rows per call into a single narrative, writes `silver_call_narratives`
2. **Protocol Embedding Notebook** — reads `bronze_protocols`, uses `ai_query()` with embedding model to generate embeddings per row, writes `silver_protocol_chunks`, triggers Vector Search sync
3. **Protocol Execution Notebook** — joins `silver_calls_routed` with `silver_call_narratives` on `call_id` to get both routing info and narrative text, then queries Vector Search index for ordered protocol rows, uses `ai_query()` for step-by-step execution, writes `gold_protocol_execution`
4. **Evaluation Notebook** — reads execution results, uses `ai_query()` to write clinical evaluations, writes `gold_evaluation_summaries`
5. **Care Assessment Notebook** — reads execution + evaluation + original call data, uses `ai_query()` to assess dispatch necessity / tiering / quality, writes `gold_care_assessment`
6. **Analytics Aggregation** — SQL notebook that builds `gold_analytics_summary` from gold tables
7. **Excel Export Notebook** — exports results for human clinical review to UC Volume:
   - **Source tables:** joins `gold_protocol_execution`, `gold_evaluation_summaries`, `gold_care_assessment`
   - **Filter criteria:** `is_ambiguous = true` OR `cross_protocol_flag = true` OR `quality_score <= 2` OR `dispatch_was_necessary = 'no'`
   - **Output path:** `{volume_path}/exports/review_batch_{timestamp}.csv`
   - **Behavior:** Creates a new file per run (does not overwrite previous exports)

---

## 6. Vector Search Setup

### 6.1 Endpoint

Managed Vector Search endpoint (serverless)

### 6.2 Index: `protocol_chunks_index`

Synced from `silver_protocol_chunks` delta table.

**Source table columns:**

| Column | Purpose |
|--------|---------|
| `chunk_id` | Primary key — `{protocol_name}_{row_number}` |
| `chunk_text` | Concatenated: question_text + condition + disposition + stop_rule for that row |
| `embedding` | Generated via `ai_query()` using Databricks embedding model (e.g., `databricks-bge-large-en`) |
| `protocol_name` | Metadata filter — target a specific protocol |
| `module_name` | Metadata — triage, key questions, determinant, pre-arrival |
| `row_number` | Metadata — preserves execution order |
| `disposition` | Metadata — numeric 0-5 |
| `stop_rule` | Metadata — stop logic for that row |

### 6.3 Retrieval Pattern

Vector Search serves two purposes in this pipeline:

**Primary use — Protocol retrieval by name:** For most calls, the routing step (Step 4) already identifies the correct protocol. We query Vector Search with filter `protocol_name = '{routed_protocol}'`, return all rows for that protocol, and order by `row_number` in the application layer. This is functionally a filtered lookup, but uses Vector Search to demonstrate the platform capability and keep the protocol store in a single managed index.

**Secondary use — Similarity-based protocol discovery:** For calls that are ambiguous or where the routing is uncertain, we use the narrative as a similarity query against the full index (no protocol filter) to find the most relevant protocol rows. This demonstrates genuine RAG value — the LLM can discover that a "breathing problems" call might actually match "chest pain" protocol rows better, supporting the cross-protocol flagging in the evaluation step.

**Execution flow:**
1. Filter by `protocol_name` to get the routed protocol's rows
2. Order by `row_number` to preserve execution sequence
3. Pass full ordered protocol + call narrative to `ai_query()` for step-by-step execution
4. Optionally run a similarity query (no filter) to check for cross-protocol matches

**Key constraint:** Protocols go into RAG. Narratives do NOT go into RAG. RAG is only for protocol retrieval.

**Embedding model note:** The spec references `databricks-bge-large-en`. Verify availability on the target workspace before implementation — `databricks-gte-large-en` is an alternative if BGE is not available.

---

## 7. AI/BI Dashboard (Lakeview)

4-page dashboard targeting Mukund's three assessment dimensions.

### Page 1 — Executive Overview
- Total calls processed (counter)
- Disposition distribution (bar chart — 0 through 5)
- Ambiguity rate (gauge — % of calls with ambiguous disposition)
- Replayability rate (gauge — % successfully replayed)
- Non-replayable calls count with top reasons

### Page 2 — Dispatch Necessity Analysis
- Dispatch necessary vs unnecessary vs borderline (pie chart)
- Unnecessary dispatch rate by complaint type (bar chart)
- Calls that could have been phone-resolved (table with drill-down)
- Estimated cost savings from avoided dispatches (counter)

### Page 3 — Vehicle Tiering / Resource Allocation
- Actual vs recommended vehicle tier (stacked bar — match / over-dispatched / under-dispatched)
- Over-dispatch rate by protocol (bar chart)
- Under-dispatch rate by protocol (safety concern)
- Tier mismatch detail table (sortable, filterable)

### Page 4 — Quality of Care
- Quality score distribution (histogram 1-5)
- Average quality score by protocol (bar chart)
- Cross-protocol flags — calls where different protocol may have been more appropriate (table)
- Lowest quality score calls with audit trail summaries (detail table)

**Filters across all pages:** complaint type, protocol name, disposition, ambiguity flag, date range

---

## 8. AI/BI Genie Space

### 8.1 Tables Exposed

- `gold_protocol_execution`
- `gold_evaluation_summaries`
- `gold_care_assessment`
- `gold_analytics_summary`
- `silver_call_narratives`

### 8.2 Sample Seed Questions

- "Which protocols have the highest ambiguity rate?"
- "Show me all chest pain calls where dispatch was unnecessary"
- "How many calls were over-dispatched with ALS?"
- "What's the average quality score for breathing problems?"
- "Show me calls where a different protocol was recommended"
- "What are the top reasons calls are non-replayable?"
- "How much could we save if we right-sized every dispatch?"
- "Show me the audit trail for call ID 247"

### 8.3 Genie Instructions Context

- Disposition scale: 0 = most urgent (immediate dispatch) through 5 = self-care
- Vehicle tiers: BLS (Basic Life Support), ALS (Advanced Life Support), Supervisor
- Ambiguous = call could not be cleanly resolved to single disposition
- Non-replayable = insufficient information in call record to execute protocol
- Quality score: 1 = poor alignment with protocol, 5 = perfect alignment
- Column descriptions for all exposed tables

---

## 9. Workflow Orchestration

A single Databricks Workflow with task dependencies:

```
[Synthetic Data Generation]
        |
        v
[LDP Pipeline - Bronze + Silver non-LLM]
        |
        +---> [Protocol Embedding + Vector Search Setup]
        |
        +---> [Narrative Generation]
                    |
                    v
              [Protocol Execution] (depends on both embedding + narratives)
                    |
                    v
              [Evaluation Summary]
                    |
                    v
              [Care Assessment]
                    |
                    v
              [Analytics Aggregation]
                    |
                    v
              [Excel Export]
```

---

## 10. What This Demo Proves to Mukund

1. **Databricks handles the full pipeline** — ingestion, de-identification, LLM processing, RAG, storage, analytics, all on one platform
2. **Production-grade architecture** — Lakeflow Declarative Pipeline with medallion architecture, not just notebooks
3. **Scalable LLM processing** — `ai_query()` processes hundreds of calls through protocols
4. **RAG done right** — Vector Search with managed sync, protocol rows as individual documents with metadata
5. **Actionable analytics** — dashboard answers the three questions Mukund cares about: quality of care, dispatch necessity, vehicle tiering
6. **Ad-hoc exploration** — Genie lets execs ask their own questions without waiting for a dashboard update
7. **Human-in-the-loop** — Excel export for clinical reviewers to validate ambiguous cases
8. **Data governance** — Unity Catalog for access control, PII removal in the pipeline, audit trail for every decision
