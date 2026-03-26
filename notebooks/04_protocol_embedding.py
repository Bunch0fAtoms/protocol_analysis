# Databricks notebook source
# MAGIC %md
# MAGIC # Step 4: Protocol Embedding & Vector Search
# MAGIC
# MAGIC Embeds each protocol row as a separate document and syncs to a managed Vector Search index.
# MAGIC Each row becomes a retrievable chunk with metadata (protocol name, module, row number, disposition, stop rule).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate embeddings for protocol rows
# MAGIC Using `databricks-bge-large-en` embedding model. Each protocol row's question text + condition + stop rule is embedded as a 1024-dimensional vector.

# COMMAND ----------

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
    ' | Disposition: ', COALESCE(disposition, ''),
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

# MAGIC %md
# MAGIC ### Enable Change Data Feed (required for Vector Search delta sync)

# COMMAND ----------

spark.sql(f"ALTER TABLE {SILVER_PROTOCOL_CHUNKS} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Vector Search endpoint and index
# MAGIC The index syncs from the delta table automatically. Protocol rows are retrievable by protocol name and similarity search.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Create endpoint (idempotent)
try:
    w.vector_search_endpoints.create_endpoint(name=VS_ENDPOINT_NAME, endpoint_type="STANDARD")
    print(f"Created endpoint: {VS_ENDPOINT_NAME}")
except Exception as e:
    print(f"Endpoint exists: {e}")

# COMMAND ----------

import requests, json

# Create delta sync index via REST API
host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

response = requests.post(
    f"https://{host}/api/2.0/vector-search/indexes",
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    json={
        "name": VS_INDEX_NAME,
        "endpoint_name": VS_ENDPOINT_NAME,
        "primary_key": "chunk_id",
        "index_type": "DELTA_SYNC",
        "delta_sync_index_spec": {
            "source_table": SILVER_PROTOCOL_CHUNKS,
            "embedding_vector_columns": [{"name": "embedding", "embedding_dimension": 1024}],
            "pipeline_type": "TRIGGERED",
            "columns_to_sync": ["chunk_id", "chunk_text", "protocol_name", "module_name", "row_number", "disposition", "stop_rule"]
        }
    }
)
print(response.json())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify: Protocol chunks

# COMMAND ----------

display(spark.sql(f"""
SELECT protocol_name, COUNT(*) as rows, MIN(row_number) as min_row, MAX(row_number) as max_row
FROM {SILVER_PROTOCOL_CHUNKS}
GROUP BY protocol_name
ORDER BY rows DESC
"""))
