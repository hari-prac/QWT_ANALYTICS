{{ config(materialized='table',schema='transforming_dev')}}

select  
p.productid,
p.productname,
c.categoryname,
s.CompanyName as suppliercompany,
s.ContactName as suppliercontact,
s.city as suppliercity,
s.country as suppliercountry,
p.QUANTITYPERUNIT,
p.UNITCOST,
p.UNITPRICE,
p.UNITSINSTOCK,
p.UNITSINORDER,
iff(p.UNITSINSTOCK > p.UNITSINORDER, 'ProductAvailable','ProductNotAvailable') as ProductAvailability
from
{{ ref('stg_products') }} as p
inner join
{{ ref('stg_suppliers') }} as s
on p.SupplierID = s.SupplierID

inner join
{{ ref("lkp_categories") }} as c
on p.categoryid = c.categoryid