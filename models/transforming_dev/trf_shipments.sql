{{ config(materialized='table',schema='transforming_dev')}}

select 
ss.orderid,
ss.lineno,
ss.shipmentdate,
ss.status,
sh.companyname as shipmentcompany

from 

{{ ref("shipments_snapshot")}} as ss
inner join
{{ ref('lkp_shipments') }} as sh
on ss.shipperid = sh.shipperid
where ss.dbt_valid_to is null