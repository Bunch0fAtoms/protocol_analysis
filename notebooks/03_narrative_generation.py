# Databricks notebook source
# MAGIC %md
# MAGIC # Step 3: Generate Call Narratives
# MAGIC
# MAGIC Converts de-identified Q&A rows into a single cohesive narrative per call using `ai_query()`.
# MAGIC
# MAGIC **Rules:**
# MAGIC - Preserve the caller's exact wording
# MAGIC - No paraphrasing
# MAGIC - No inference of information not stated
# MAGIC - Write in past tense

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build narratives from Q&A rows
# MAGIC Each call has 5-15 Q&A rows. We group them by call_id, order by question number, and ask the LLM to produce a single cohesive narrative.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {SILVER_CALL_NARRATIVES} AS
WITH ordered_qa AS (
  SELECT
    call_id,
    complaint_type,
    actual_dispatch_type,
    on_scene_outcome,
    CONCAT('Q: ', dispatcher_question, ' A: ', caller_answer) AS qa_text,
    ROW_NUMBER() OVER (PARTITION BY call_id ORDER BY CAST(question_number AS INT)) AS rn
  FROM {SILVER_CALLS_DEIDENTIFIED}
),
call_qa AS (
  SELECT
    call_id,
    complaint_type,
    actual_dispatch_type,
    on_scene_outcome,
    array_join(collect_list(qa_text), '\\n') AS qa_text_joined
  FROM ordered_qa
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
      qa_text_joined
    )
  ) AS narrative
FROM call_qa
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify: Sample narratives

# COMMAND ----------

display(spark.sql(f"SELECT call_id, complaint_type, narrative FROM {SILVER_CALL_NARRATIVES} LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stats

# COMMAND ----------

display(spark.sql(f"""
SELECT
  COUNT(*) as total_narratives,
  COUNT(DISTINCT complaint_type) as complaint_types,
  AVG(LENGTH(narrative)) as avg_narrative_length
FROM {SILVER_CALL_NARRATIVES}
"""))
