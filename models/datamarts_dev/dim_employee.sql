{{ config(materialzied = 'view',schema='datamarts_dev')}}

select * from {{ ref("stg_employee")}}