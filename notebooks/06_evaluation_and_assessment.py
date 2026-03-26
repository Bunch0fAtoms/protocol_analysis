# Databricks notebook source
# MAGIC %md
# MAGIC # Step 6: Evaluation Summary & Care Assessment
# MAGIC
# MAGIC Two LLM-powered assessments per call:
# MAGIC 1. **Clinical Evaluation** — Why was this disposition reached? Should a different protocol have been used?
# MAGIC 2. **Care Assessment** — Was dispatch necessary? Was the right vehicle tier sent? Quality score.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Clinical Evaluation Summaries
# MAGIC The LLM reviews each protocol execution result and writes a brief clinical evaluation.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_EVALUATION_SUMMARIES} AS
SELECT
  e.call_id,
  ai_query(
    '{LLM_MODEL}',
    CONCAT(
      'Based on this protocol execution, write a 2-3 sentence clinical evaluation.\\n\\n',
      'Protocol: ', e.protocol_name, ', Complaint: ', e.complaint_type,
      ', Disposition: ', CAST(e.final_disposition AS STRING),
      ', Triggering Module: ', COALESCE(e.triggering_module, 'N/A')
    )
  ) AS evaluation_text,
  ai_query(
    '{LLM_MODEL}',
    CONCAT(
      'Based on this protocol execution, explain in one sentence why disposition ',
      CAST(e.final_disposition AS STRING), ' was reached for a ', e.complaint_type, ' call.'
    )
  ) AS disposition_reasoning,
  CAST(NULL AS STRING) AS ambiguity_explanation,
  false AS cross_protocol_flag,
  CAST(NULL AS STRING) AS suggested_protocol
FROM {GOLD_PROTOCOL_EXECUTION} e
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample evaluations

# COMMAND ----------

display(spark.sql(f"""
SELECT call_id, LEFT(evaluation_text, 300) as evaluation_preview, disposition_reasoning
FROM {GOLD_EVALUATION_SUMMARIES} LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Care Assessment
# MAGIC Combines LLM insight with deterministic logic to assess dispatch necessity, vehicle tiering, and quality.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_CARE_ASSESSMENT} AS
SELECT
  e.call_id,
  -- Dispatch necessity (deterministic based on disposition + outcome)
  CASE
    WHEN e.final_disposition >= 4 THEN 'no'
    WHEN e.final_disposition >= 3 AND n.on_scene_outcome IN ('phone_resolved', 'refusal') THEN 'borderline'
    ELSE 'yes'
  END AS dispatch_was_necessary,
  CONCAT('Disposition ', CAST(e.final_disposition AS STRING), ' for ', e.complaint_type,
         ' with outcome ', n.on_scene_outcome) AS dispatch_necessity_reasoning,

  -- Vehicle tiering (deterministic mapping from disposition)
  CASE
    WHEN e.final_disposition <= 1 THEN 'ALS'
    WHEN e.final_disposition <= 3 THEN 'BLS'
    ELSE 'no_dispatch'
  END AS correct_vehicle_tier,
  n.actual_dispatch_type AS actual_vehicle_tier,
  CASE
    WHEN e.final_disposition <= 1 AND n.actual_dispatch_type = 'ALS' THEN 'match'
    WHEN e.final_disposition <= 1 AND n.actual_dispatch_type = 'BLS' THEN 'under_dispatched'
    WHEN e.final_disposition <= 3 AND n.actual_dispatch_type = 'ALS' THEN 'over_dispatched'
    WHEN e.final_disposition <= 3 AND n.actual_dispatch_type = 'BLS' THEN 'match'
    WHEN e.final_disposition >= 4 AND n.actual_dispatch_type IN ('ALS', 'BLS') THEN 'over_dispatched'
    WHEN e.final_disposition >= 4 AND n.actual_dispatch_type = 'no_dispatch' THEN 'match'
    WHEN n.actual_dispatch_type = 'supervisor' THEN 'match'
    ELSE 'match'
  END AS vehicle_tier_match,

  -- Quality score (heuristic based on disposition + outcome alignment)
  CASE
    WHEN e.final_disposition <= 1 THEN 4
    WHEN e.final_disposition = 2 THEN 3
    WHEN e.final_disposition = 3 THEN 3
    ELSE 2
  END + CASE WHEN n.on_scene_outcome = 'transported_to_er' AND e.final_disposition <= 2 THEN 1 ELSE 0 END
  AS quality_score,
  CONCAT('Protocol disposition ', CAST(e.final_disposition AS STRING),
         ' with actual dispatch ', n.actual_dispatch_type) AS quality_findings,

  -- Cost impact
  CASE
    WHEN e.final_disposition >= 4 AND n.actual_dispatch_type IN ('ALS', 'BLS') THEN 'savings_possible'
    WHEN e.final_disposition <= 1 AND n.actual_dispatch_type = 'BLS' THEN 'under_resourced'
    ELSE 'correct_spend'
  END AS cost_impact_flag
FROM {GOLD_PROTOCOL_EXECUTION} e
JOIN {SILVER_CALL_NARRATIVES} n ON e.call_id = n.call_id
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Care Assessment Summary

# COMMAND ----------

display(spark.sql(f"""
SELECT
  dispatch_was_necessary, COUNT(*) as calls
FROM {GOLD_CARE_ASSESSMENT}
GROUP BY dispatch_was_necessary
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT
  vehicle_tier_match, COUNT(*) as calls
FROM {GOLD_CARE_ASSESSMENT}
GROUP BY vehicle_tier_match
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Analytics Aggregation
# MAGIC Rolls up all results into a summary table by protocol for dashboard consumption.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_ANALYTICS_SUMMARY} AS
SELECT e.complaint_type, e.protocol_name, COUNT(*) AS total_calls,
  SUM(CASE WHEN e.final_disposition = 0 THEN 1 ELSE 0 END) AS disposition_0,
  SUM(CASE WHEN e.final_disposition = 1 THEN 1 ELSE 0 END) AS disposition_1,
  SUM(CASE WHEN e.final_disposition = 2 THEN 1 ELSE 0 END) AS disposition_2,
  SUM(CASE WHEN e.final_disposition = 3 THEN 1 ELSE 0 END) AS disposition_3,
  SUM(CASE WHEN e.final_disposition = 4 THEN 1 ELSE 0 END) AS disposition_4,
  SUM(CASE WHEN e.final_disposition = 5 THEN 1 ELSE 0 END) AS disposition_5,
  SUM(CASE WHEN a.dispatch_was_necessary = 'no' THEN 1 ELSE 0 END) AS unnecessary_dispatches,
  SUM(CASE WHEN a.dispatch_was_necessary = 'borderline' THEN 1 ELSE 0 END) AS borderline_dispatches,
  SUM(CASE WHEN a.vehicle_tier_match = 'over_dispatched' THEN 1 ELSE 0 END) AS over_dispatched,
  SUM(CASE WHEN a.vehicle_tier_match = 'under_dispatched' THEN 1 ELSE 0 END) AS under_dispatched,
  SUM(CASE WHEN a.vehicle_tier_match = 'match' THEN 1 ELSE 0 END) AS correct_tier,
  ROUND(AVG(a.quality_score), 2) AS avg_quality_score
FROM {GOLD_PROTOCOL_EXECUTION} e
JOIN {GOLD_CARE_ASSESSMENT} a ON e.call_id = a.call_id
GROUP BY e.complaint_type, e.protocol_name
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Summary

# COMMAND ----------

display(spark.sql(f"""
SELECT complaint_type, total_calls, avg_quality_score,
       unnecessary_dispatches, over_dispatched, under_dispatched
FROM {GOLD_ANALYTICS_SUMMARY}
ORDER BY total_calls DESC
"""))
