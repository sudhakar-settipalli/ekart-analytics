INSERT OVERWRITE TABLE fc_single_shipment_product_vol_l2_hive_fact
select
d.product_detail_fsn as product_detail_fsn,
if(d.single_shipment_volume is not null and d.single_shipment_volume>0,d.single_shipment_volume,e.single_shipment_volume) as single_shipment_volume,
if(d.product_detail_volume is not null and d.product_detail_volume>0,d.product_detail_volume,e.product_detail_volume) as product_detail_volume
from
(
select
c.product_detail_fsn as product_detail_fsn,
percentile_approx(if(b.shipment_packing_box_used_bucket='SecurityBag' and  c.product_detail_cms_vertical  in ('mobile','tablet'),
1.2*shipment_packing_box_suggested_volume,if(b.shipment_packing_box_used_bucket='SecurityBag',1.2*c.product_detail_length*c.product_detail_breadth*c.product_detail_height,b.shipment_volume)),0.5) as single_shipment_volume,
percentile_approx(c.product_detail_length*c.product_detail_breadth*c.product_detail_height,0.5) as product_detail_volume,
max(c.product_detail_cms_vertical) as product_detail_cms_vertical
from
bigfoot_external_neo.scp_warehouse__fc_shipment_item_l0_hive_fact as a 
join bigfoot_external_neo.scp_warehouse__fc_shipment_l1_hive_fact as b on a.shipment_display_id=b.shipment_display_id 
left outer join  bigfoot_external_neo.scp_warehouse__fc_product_detail_hive_dim as c on  concat(a.shipment_item_product_id,a.warehouse_company)=c.product_detail_product_id
where a.shipment_dispatched_timestamp is not null  and b.shipment_quantity=1 and b.shipment_destination_type='customer'
group by c.product_detail_fsn
) d
left join
(
select
c.product_detail_cms_vertical as product_detail_cms_vertical ,
percentile_approx(if(b.shipment_packing_box_used_bucket='SecurityBag' and  c.product_detail_cms_vertical  in ('mobile','tablet'),
1.2*shipment_packing_box_suggested_volume,if(b.shipment_packing_box_used_bucket='SecurityBag',1.2*c.product_detail_length*c.product_detail_breadth*c.product_detail_height,b.shipment_volume)),0.5) as single_shipment_volume,
percentile_approx(c.product_detail_length*c.product_detail_breadth*c.product_detail_height,0.5) as product_detail_volume
from
bigfoot_external_neo.scp_warehouse__fc_shipment_item_l0_hive_fact as a 
join bigfoot_external_neo.scp_warehouse__fc_shipment_l1_hive_fact as b on a.shipment_display_id=b.shipment_display_id 
left outer join  bigfoot_external_neo.scp_warehouse__fc_product_detail_hive_dim as c on  concat(a.shipment_item_product_id,a.warehouse_company)=c.product_detail_product_id
where a.shipment_dispatched_timestamp is not null  and b.shipment_quantity=1 and b.shipment_destination_type='customer'
group by c.product_detail_cms_vertical
) e
on d.product_detail_cms_vertical=e.product_detail_cms_vertical
