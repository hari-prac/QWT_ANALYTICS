{{ config (materialized = 'table',schema= env_var('DBT_STGSCHEMA_NAME','STAGING_DEV')) }}

select
EMPID,
LASTNAME,
FIRSTNAME,
TITLE,
HIREDATE,
OFFICE,
EXTENSION,
REPORTSTO,
YEARSALARY
from {{ source("QWT_RAW","raw_employee")}}
