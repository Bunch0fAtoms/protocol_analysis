# Databricks notebook source
# MAGIC %md
# MAGIC # Step 5: Protocol Execution
# MAGIC
# MAGIC The core step — the LLM walks through each protocol's rows IN ORDER against the patient call narrative.
# MAGIC For each row, it determines if the condition is met, applies stop rules, and outputs:
# MAGIC - Final disposition (0-5)
# MAGIC - Whether the outcome is ambiguous or non-replayable
# MAGIC - A full audit trail of which rows triggered or failed

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare: Join routed calls with narratives (deduplicated to 1 row per call)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW calls_with_narratives AS
SELECT DISTINCT
  r.call_id,
  r.complaint_type,
  r.new_protocol_name,
  r.actual_dispatch_type,
  r.on_scene_outcome,
  n.narrative
FROM {SILVER_CALLS_ROUTED} r
JOIN {SILVER_CALL_NARRATIVES} n ON r.call_id = n.call_id
""")

print(f"Calls ready for execution: {spark.sql('SELECT COUNT(*) FROM calls_with_narratives').collect()[0][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare: Collect protocol rows in order per protocol

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW protocol_rows AS
SELECT
  protocol_name,
  array_join(
    collect_list(
      CONCAT('Row ', row_number, ' [', module_name, '] Question: ', chunk_text,
             ' | Disposition: ', COALESCE(CAST(disposition AS STRING), 'N/A'),
             ' | Stop Rule: ', stop_rule)
    ),
    '\\n'
  ) AS protocol_text
FROM (
  SELECT * FROM {SILVER_PROTOCOL_CHUNKS} ORDER BY row_number
)
GROUP BY protocol_name
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute: LLM walks through each protocol against the narrative
# MAGIC This processes all 500 calls. Each call gets the full ordered protocol + narrative sent to the LLM.
# MAGIC The LLM outputs structured JSON with disposition, audit trail, and ambiguity flags.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_PROTOCOL_EXECUTION} AS
WITH raw_execution AS (
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
        'PROTOCOL ROWS (execute in order):\\n', p.protocol_text
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

# MAGIC %md
# MAGIC ### Verify: Protocol execution results

# COMMAND ----------

display(spark.sql(f"""
SELECT
  COUNT(*) as total,
  SUM(CASE WHEN is_ambiguous THEN 1 ELSE 0 END) AS ambiguous,
  SUM(CASE WHEN NOT is_replayable THEN 1 ELSE 0 END) AS non_replayable,
  ROUND(AVG(final_disposition), 2) AS avg_disposition
FROM {GOLD_PROTOCOL_EXECUTION}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample: Execution result with audit trail

# COMMAND ----------

display(spark.sql(f"""
SELECT call_id, complaint_type, protocol_name, final_disposition,
       triggering_row, triggering_module, audit_trail
FROM {GOLD_PROTOCOL_EXECUTION}
LIMIT 3
"""))
