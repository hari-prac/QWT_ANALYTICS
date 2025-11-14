{{ config(
    materialized='table',
    schema=env_var('DBT_DBTSCHEMA_NAME', 'STAGING_DEV')
) }}
select * from {{ source("QWT_RAW","raw_office")}}