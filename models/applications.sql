-- models/extract_airbyte_data.sql
{{ config(materialized='table') }}

WITH clickhouse_applications AS (
    SELECT
        JSONExtractString(_airbyte_data, 'id') AS id,
        JSONExtractUInt(_airbyte_data, 'state') AS state,
        JSONExtractUInt(_airbyte_data, 'version') AS version,
        JSONExtractString(_airbyte_data, 'application_number') AS application_number,
        JSONExtractString(_airbyte_data, 'application_state') AS application_state,
        JSONExtractString(_airbyte_data, 'application_type') AS application_type,
        JSONExtractString(_airbyte_data, 'application_category') AS application_category,
        JSONExtractString(_airbyte_data, 'comment') AS comment,
        JSONExtractString(_airbyte_data, 'date_created') AS date_created,
        JSONExtractString(_airbyte_data, 'date_last_modified') AS date_last_modified,
        JSONExtractString(_airbyte_data, 'date_submitted') AS date_submitted,
        JSONExtractString(_airbyte_data, 'applicant_email_address') AS applicant_email_address,
        JSONExtractString(_airbyte_data, 'applicant_phone_number') AS applicant_phone_number,
        JSONExtractString(_airbyte_data, 'locale') AS locale,
        JSONExtractString(_airbyte_data, 'payment_expiration_time') AS payment_expiration_time,
        JSONExtractUInt(_airbyte_data, 'price') AS price,
        JSONExtractString(_airbyte_data, 'price_type') AS price_type,
        JSONExtractString(_airbyte_data, 'currency_code') AS currency_code,
        JSONExtractString(_airbyte_data, 'processing_level') AS processing_level,
        JSONExtractString(_airbyte_data, 'applicant_id') AS applicant_id,
        JSONExtractString(_airbyte_data, 'creator_id') AS creator_id,
        JSONExtractString(_airbyte_data, 'office_id') AS office_id,
        JSONExtractString(_airbyte_data, 'officer_id') AS officer_id,
        JSONExtractString(_airbyte_data, 'reason_id') AS reason_id,
        JSONExtractString(_airbyte_data, 'requester_id') AS requester_id,
        JSONExtractString(_airbyte_data, 'irembo_service_id') AS irembo_service_id,
        JSONExtractString(_airbyte_data, 'creator_type') AS creator_type,
        JSONExtractString(_airbyte_data, 'application_channel') AS application_channel,
        JSONExtractString(_airbyte_data, 'payment_account_id') AS payment_account_id,
        JSONExtractString(_airbyte_data, 'pagination_key') AS pagination_key,
        JSONExtractString(_airbyte_data, 'generated_document_id') AS generated_document_id
    FROM 
        irembo_clickhouse_raw__stream_application
)

SELECT * FROM clickhouse_applications
