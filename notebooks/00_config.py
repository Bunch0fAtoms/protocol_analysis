# Databricks notebook source
# MAGIC %md
# MAGIC # GMR Protocol Analysis Demo — Configuration
# MAGIC Shared configuration for all demo notebooks.

# COMMAND ----------

# Catalog and schema — change these to match your workspace
CATALOG = "morgancatalog"
SCHEMA = "protocol_analysis"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/source_data"

# Model endpoints for ai_query
LLM_MODEL = "databricks-meta-llama-3-3-70b-instruct"
EMBEDDING_MODEL = "databricks-bge-large-en"

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
