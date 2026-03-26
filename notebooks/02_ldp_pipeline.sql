-- Databricks notebook source
-- GMR Protocol Analysis — Lakeflow Declarative Pipeline
-- Bronze ingestion from UC Volume + Silver transformations (non-LLM)

-- ============================================================
-- BRONZE LAYER: Raw ingestion from CSV files in UC Volume
-- ============================================================

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

-- ============================================================
-- SILVER LAYER: De-identification and routing (non-LLM)
-- ============================================================

-- Silver: De-identify call records (strip PII)
CREATE OR REFRESH MATERIALIZED VIEW silver_calls_deidentified AS
SELECT
  call_id,
  timestamp,
  legacy_protocol_used,
  complaint_type,
  question_number,
  dispatcher_question,
  -- PII redaction: replace phone numbers, addresses, and names with title prefixes
  regexp_replace(
    regexp_replace(
      regexp_replace(caller_answer,
        '\\b\\d{3}[-.]?\\d{3}[-.]?\\d{4}\\b', '[PHONE_REDACTED]'),
      '\\b\\d+\\s+[A-Z][a-z]+\\s+(St|Ave|Blvd|Dr|Rd|Ln|Ct|Way)\\b', '[ADDRESS_REDACTED]'),
    '\\b(Mr|Mrs|Ms|Dr)\\.?\\s+[A-Z][a-z]+\\b', '[NAME_REDACTED]')
  AS caller_answer,
  actual_dispatch_type,
  on_scene_outcome,
  '[REDACTED]' AS caller_name,
  '[REDACTED]' AS caller_phone,
  '[REDACTED]' AS caller_address
FROM bronze_call_logs;

-- Silver: Route calls to new protocols (joins deidentified calls with mapping)
CREATE OR REFRESH MATERIALIZED VIEW silver_calls_routed AS
SELECT
  d.*,
  m.new_protocol_name,
  m.protocol_file
FROM silver_calls_deidentified d
JOIN bronze_protocol_mapping m
  ON d.legacy_protocol_used = m.legacy_protocol_name;
