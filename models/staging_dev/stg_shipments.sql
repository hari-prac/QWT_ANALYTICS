{{ config(materialized='table',
          schema= env_var('DBT_DBTSCHEMA_NAME','STAGING_DEV')
            )
             }}
select 
orderid,
lineno,
shipperid,
customerid,
productid,
employeeid,
split_part(shipmentdate,' ',1) as shipmentdate,
status
from {{ source("QWT_RAW",'raw_shipments')}}