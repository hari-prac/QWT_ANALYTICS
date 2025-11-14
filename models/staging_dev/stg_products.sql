{{ 
    config(materialized='table',transient=false,schema= env_var('DBT_STGSCHEMA_NAME','STAGING_DEV'))
}}

SELECT *
from
{{ source('QWT_RAW','raw_products')}}