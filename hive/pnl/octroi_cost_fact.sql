insert overwrite table octroi_cost_fact

select 
j.vendor_tracking_id, 
sum(if(e.octroi_flag=0 or j.vendor_tracking_id='not_assigned',0,
    j.item_value*j.item_quantity*j.part)) as octroi_cost
from
(select h.vendor_tracking_id,
  h.item_quantity,
  h.item_value,
  h.analytic_category,
  if(i.part is not null, i.part, 0.055) as part from 
(select f.vendor_tracking_id, 
  f.item_quantity,
  f.item_value,
  g.analytic_category as analytic_category
from bigfoot_external_neo.scp_ekl__shipment_item_l1_90_fact f left join 
bigfoot_external_neo.sp_product__product_categorization_hive_dim g
on lookupkey('product_id',f.product_id)=g.product_categorization_hive_dim_key) h
left join bigfoot_common.category_wise_octroi i
on h.analytic_category=i.category) j
left join
(select tracking_id, 
  if(source_pincode_octroi is null and dest_pincode_octroi is not null, 1, 0) as octroi_flag
from
(select 
  a.tracking_id, 
  a.shipment_value, 
  b.pincode as source_pincode_octroi, 
  c.pincode as dest_pincode_octroi 
  from 
(select 
   vendor_tracking_id as tracking_id,
   shipment_value, 
   source_address_pincode as source_pincode,
   destination_address_pincode as destination_pincode
from bigfoot_external_neo.scp_ekl__shipment_l1_90_fact) a 
left join bigfoot_common.tax_type_pincode b
on a.source_pincode=b.pincode
left join bigfoot_common.tax_type_pincode c
on a.destination_pincode=c.pincode) d) e
on j.vendor_tracking_id=e.tracking_id
group by j.vendor_tracking_id ;
insert overwrite table octroi_cost_fact

select 
j.vendor_tracking_id, 
sum(if(e.octroi_flag=0 or j.vendor_tracking_id='not_assigned',0,
    j.item_value*j.item_quantity*j.part)) as octroi_cost
from
(select h.vendor_tracking_id,
  h.item_quantity,
  h.item_value,
  h.analytic_category,
  if(i.part is not null, i.part, 0.055) as part from 
(select f.vendor_tracking_id, 
  f.item_quantity,
  f.item_value,
  g.analytic_category as analytic_category
from bigfoot_external_neo.scp_ekl__shipment_item_l1_90_fact f left join 
bigfoot_external_neo.sp_product__product_categorization_hive_dim g
on lookupkey('product_id',f.product_id)=g.product_categorization_hive_dim_key) h
left join bigfoot_common.category_wise_octroi i
on h.analytic_category=i.category) j
left join
(select tracking_id, 
  if(source_pincode_octroi is null and dest_pincode_octroi is not null, 1, 0) as octroi_flag
from
(select 
  a.tracking_id, 
  a.shipment_value, 
  b.pincode as source_pincode_octroi, 
  c.pincode as dest_pincode_octroi 
  from 
(select 
   vendor_tracking_id as tracking_id,
   shipment_value, 
   source_address_pincode as source_pincode,
   destination_address_pincode as destination_pincode
from bigfoot_external_neo.scp_ekl__shipment_l1_90_fact) a 
left join bigfoot_common.tax_type_pincode b
on a.source_pincode=b.pincode
left join bigfoot_common.tax_type_pincode c
on a.destination_pincode=c.pincode) d) e
on j.vendor_tracking_id=e.tracking_id
group by j.vendor_tracking_id ;
