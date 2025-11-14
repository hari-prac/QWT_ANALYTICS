{{ 
    config(materialized='table',transient=false,schema= env_var('DBT_DBTSCHEMA_NAME','STAGING_DEV'))
}}

SELECT *
from
{{ source('QWT_RAW','raw_products')}}