{{ config(materialized ='view',schema='reporting_dev') }}

select 
c.contactname,
c.city,
c.country,
c.divisionname,
count(distinct o.orderid) as totalorderid,
sum(o.quantity) as totalquantity,
sum(o.linesalesamount) as totalsales,
avg(o.margin) as avgmargin
from 
{{ ref("dim_customers") }}  c
inner join
{{ ref("fct_orders") }} o
on c.customerid = o.customerid
where c.divisionname = '{{var('v_division','Europe') }}'
group by c.contactname, c.city,
c.country,c.divisionname
