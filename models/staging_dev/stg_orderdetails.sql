{{ config(
    materialized='incremental',
    unique_key = 'OrderID',
    schema=env_var('DBT_DBTSCHEMA_NAME', 'STAGING_DEV')
) }}
 
select
od.orderid,
od.lineno,
od.productid,
od.quantity,
od.unitprice,
od.discount,
o.orderdate
from
{{ source("QWT_RAW", "raw_orderdetails")}}  as od
inner join
{{source("QWT_RAW", "raw_orders")}} as o
on od.orderid = o.orderid
 
{% if is_incremental() %}
 
where o.orderdate > (select max(orderdate) from {{this}} )
 
{% endif %}