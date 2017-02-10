INSERT OVERWRITE TABLE fc_product_detail_hive_dim 
select 
final.fc_product_detail_hive_dim_key as fc_product_detail_hive_dim_key , 
final.product_detail_product_id as product_detail_product_id , 
final.product_detail_id as product_detail_id , 
final.product_detail_binding_attribute as product_detail_binding_attribute , 
final.product_detail_bold_title as product_detail_bold_title , 
final.product_detail_breadth as product_detail_breadth , 
final.product_detail_cms_brand as product_detail_cms_brand , 
final.product_detail_cms_vertical as product_detail_cms_vertical , 
final.product_detail_created_at_timestamp as product_detail_created_at_timestamp , 
final.product_detail_extra_inwarding_info as product_detail_extra_inwarding_info , 
final.product_detail_fsn as product_detail_fsn , 
final.product_detail_height as product_detail_height , 
final.product_detail_ideal_for as product_detail_ideal_for , 
final.product_detail_is_dangerous as product_detail_is_dangerous , 
final.product_detail_is_fragile as product_detail_is_fragile , 
final.product_detail_is_large as product_detail_is_large , 
final.product_detail_is_medium as product_detail_is_medium , 
final.product_detail_is_serializable as product_detail_is_serializable , 
final.product_detail_length as product_detail_length , 
final.product_detail_listing_id as product_detail_listing_id , 
final.product_detail_packing_type_id as product_detail_packing_type_id , 
final.product_detail_price as product_detail_price , 
final.product_detail_product_title as product_detail_product_title , 
final.product_detail_seller_id as product_detail_seller_id , 
final.product_detail_serial_number_count as product_detail_serial_number_count , 
final.product_detail_weight as product_detail_weight , 
final.product_detail_sku as product_detail_sku , 
final.product_detail_updated_at_timestamp as product_detail_updated_at_timestamp , 
final.product_detail_wid as product_detail_wid , 
final.product_detail_warehouse_company as product_detail_warehouse_company , 
final.product_detail_is_wid_lbhw_captured as product_detail_is_wid_lbhw_captured , 
final.product_vertical_attribute_breadth as product_vertical_attribute_breadth , 
final.product_vertical_attribute_height as product_vertical_attribute_height , 
final.product_vertical_attribute_length as product_vertical_attribute_length , 
final.product_vertical_attribute_tolerance as product_vertical_attribute_tolerance , 
final.seller_dim_display_name as seller_dim_display_name , 
final.product_listing_dim_fsp as product_listing_dim_fsp , 
final.product_listing_dim_mrp as product_listing_dim_mrp , 
final.product_categorization_dim_super_category as product_categorization_dim_super_category , 
final.product_categorization_dim_vertical as product_categorization_dim_vertical , 
final.product_categorization_dim_category as product_categorization_dim_category , 
final.product_categorization_dim_sub_category as product_categorization_dim_sub_category , 
final.product_categorization_dim_business_unit as product_categorization_dim_business_unit , 
final.product_categorization_dim_node_id as product_categorization_dim_node_id , 
final.product_categorization_dim_is_large as product_categorization_dim_is_large , 
final.product_detail_is_fbf as product_detail_is_fbf , 
final.product_detail_volume as product_detail_volume , 
final.single_shipment_volume as single_shipment_volume , 
final.product_detail_lbh_captured_at as product_detail_lbh_captured_at , 
final.product_detail_lbh_captured_at_date_key as product_detail_lbh_captured_at_date_key , 
final.product_detail_lbh_captured_at_time_key as product_detail_lbh_captured_at_time_key , 
final.product_detail_lbh_machine_id as product_detail_lbh_machine_id , 
final.product_detail_lbh_machine_warehouse_id as product_detail_lbh_machine_warehouse_id , 
final.product_detail_lbh_updated_by as product_detail_lbh_updated_by , 
final.is_first_party_seller as is_first_party_seller ,
final.product_attribute_importance_type as product_attribute_importance_type,
final.product_attribute_core_flag as product_attribute_core_flag
from
(
SELECT 
lookupkey('product_detail_product_id',concat(pd.entityid,'fki')) AS fc_product_detail_hive_dim_key ,
concat(pd.entityid,'fki') as product_detail_product_id ,
pd.entityid as product_detail_id ,
pd.`data`.binding_attribute as  product_detail_binding_attribute,
pd.`data`.bold_title as product_detail_bold_title,
pd.`data`.breadth as product_detail_breadth,
pd.`data`.cms_brand as product_detail_cms_brand,
pd.`data`.cms_vertical as product_detail_cms_vertical,
from_unixtime(unix_timestamp(pd.`data`.created_at))  as product_detail_created_at_timestamp,
pd.`data`.extra_inwarding_info as product_detail_extra_inwarding_info,
pd.`data`.fsn as product_detail_fsn,
pd.`data`.height as product_detail_height,
pd.`data`.ideal_for as product_detail_ideal_for,
if(pd.`data`.is_dangerous=TRUE,1,0)  as product_detail_is_dangerous,
if(pd.`data`.is_fragile=TRUE,1,0) as product_detail_is_fragile,
if(pd.`data`.is_large=TRUE,1,0)  as product_detail_is_large,
if(pd.`data`.is_medium=TRUE,1,0)  as product_detail_is_medium,
if(pd.`data`.is_serializable=TRUE,1,0) as product_detail_is_serializable,
pd.`data`.length as product_detail_length,
pd.`data`.listing_id as product_detail_listing_id,
pd.`data`.packing_type_id as product_detail_packing_type_id,
pd.`data`.price as product_detail_price,
pd.`data`.product_title as product_detail_product_title,
pd.`data`.seller_id as product_detail_seller_id,
pd.`data`.serial_number_count as product_detail_serial_number_count,
pd.`data`.shipping_weight as product_detail_weight,
pd.`data`.sku as product_detail_sku,
from_unixtime(unix_timestamp(pd.`data`.updated_at)) as product_detail_updated_at_timestamp,
pd.`data`.wid as product_detail_wid,
'fki' as  product_detail_warehouse_company,
if( pd.`data`.length is null or pd.`data`.length=0 or      pd.`data`.breadth is null or pd.`data`.breadth=0 or pd.`data`.height is null or pd.`data`.height=0 or    pd.`data`.shipping_weight is null or pd.`data`.shipping_weight=0,0,1)  AS product_detail_is_wid_lbhw_captured,
 pva.breadth as product_vertical_attribute_breadth,
pva.height as product_vertical_attribute_height,
pva.length as product_vertical_attribute_length,
pva.tolerance   as product_vertical_attribute_tolerance,
(
Case 
when pd.`data`.seller_id='fki' then 'fki'
when pd.`data`.seller_id='wsr' then 'wsr'
else shd.display_name
end
) as seller_dim_display_name ,
plhv.flipkart_selling_price as product_listing_dim_fsp,
plhv.mrp as product_listing_dim_mrp,
pahd.analytic_super_category as product_categorization_dim_super_category,
pahd.analytic_vertical as product_categorization_dim_vertical,
pahd.analytic_category as product_categorization_dim_category,
pahd.analytic_sub_category as product_categorization_dim_sub_category,
pahd.analytic_business_unit as product_categorization_dim_business_unit,
pahd.node_id as product_categorization_dim_node_id,
pahd.is_large as product_categorization_dim_is_large,
null AS product_detail_is_fbf,
if((pd.`data`.length * pd.`data`.breadth * pd.`data`.height) is not null and 
   (pd.`data`.length * pd.`data`.breadth * pd.`data`.height)>0,
   (pd.`data`.length * pd.`data`.breadth * pd.`data`.height),
   if(sspv.product_detail_volume is not null and sspv.product_detail_volume>0,sspv.product_detail_volume,
   pdhd.product_detail_volume)
   ) as product_detail_volume,
if(sspv.single_shipment_volume is not null and sspv.single_shipment_volume>0,sspv.single_shipment_volume,if (pdhd.single_shipment_volume is not null
and pdhd.single_shipment_volume>0,pdhd.single_shipment_volume,2*(pd.`data`.length * pd.`data`.breadth * pd.`data`.height))) as single_shipment_volume,
pd.`data`.lbh_captured_at as product_detail_lbh_captured_at,
lookup_date(pd.`data`.lbh_captured_at) as product_detail_lbh_captured_at_date_key,
lookup_time(pd.`data`.lbh_captured_at) as product_detail_lbh_captured_at_time_key,
pd.`data`.lbh_machine_id as product_detail_lbh_machine_id,
pd.`data`.lbh_machine_warehouse_id as product_detail_lbh_machine_warehouse_id,
pd.`data`.lbh_updated_by as product_detail_lbh_updated_by,
(
Case 
when (pd.`data`.seller_id='fki' or pd.`data`.seller_id='wsr' or shd.is_first_party_seller=TRUE) then 1
else 0
end
) as is_first_party_seller,
pahd.importance_type as product_attribute_importance_type,
pahd.core_flag as product_attribute_core_flag
from 
bigfoot_snapshot.dart_fki_scp_warehouse_product_detail_2_view_total pd
left join 
(
select 
pva1.`data`.cms_vertical as cms_vertical,
max(pva1.`data`.breadth) as breadth,
max(pva1.`data`.height) as height,
max(pva1.`data`.length) as length,
max(pva1.`data`.tolerance)   as tolerance
from
bigfoot_snapshot.dart_fki_scp_warehouse_product_vertical_attribute_1_view_total pva1
group by pva1.`data`.cms_vertical
) pva on pd.`data`.cms_vertical= pva.cms_vertical
left join  
(
select
shd1.seller_id as seller_id,
max(shd1.display_name) as display_name,
max(is_first_party_seller) as is_first_party_seller
from
bigfoot_external_neo.sp_seller__seller_hive_dim shd1
group by shd1.seller_id
) shd
on pd.`data`.seller_id=shd.seller_id
left join bigfoot_external_neo.sp_product__listing_hive_dim plhv on pd.`data`.listing_id=plhv.listing_id
left join bigfoot_external_neo.sp_product__product_attribute_hive_dim pahd on pd.`data`.fsn=pahd.product_id	
left join bigfoot_external_neo.scp_warehouse__fc_single_shipment_product_vol_l2_hive_fact sspv on pd.`data`.fsn=sspv.product_detail_fsn
left join 
(
select
sub.product_detail_cms_vertical as product_detail_cms_vertical ,
percentile_approx(sub.single_shipment_volume,0.5) as single_shipment_volume,
percentile_approx(sub.product_detail_volume,0.5) as product_detail_volume
from
bigfoot_external_neo.scp_warehouse__fc_product_detail_hive_dim sub
group by sub.product_detail_cms_vertical
) pdhd
on pd.`data`.cms_vertical = pdhd.product_detail_cms_vertical

UNION ALL

SELECT 
lookupkey('product_detail_product_id',concat(pd.entityid,'wsr')) AS fc_product_detail_hive_dim_key ,
concat(pd.entityid,'wsr') as product_detail_product_id ,
pd.entityid as product_detail_id ,
pd.`data`.binding_attribute as  product_detail_binding_attribute,
pd.`data`.bold_title as product_detail_bold_title,
pd.`data`.breadth as product_detail_breadth,
pd.`data`.cms_brand as product_detail_cms_brand,
pd.`data`.cms_vertical as product_detail_cms_vertical,
from_unixtime(unix_timestamp(pd.`data`.created_at)) as product_detail_created_at_timestamp,
pd.`data`.extra_inwarding_info as product_detail_extra_inwarding_info,
pd.`data`.fsn as product_detail_fsn,
pd.`data`.height as product_detail_height,
pd.`data`.ideal_for as product_detail_ideal_for,
if(pd.`data`.is_dangerous=TRUE,1,0)  as product_detail_is_dangerous,
if(pd.`data`.is_fragile=TRUE,1,0) as product_detail_is_fragile,
if(pd.`data`.is_large=TRUE,1,0)  as product_detail_is_large,
if(pd.`data`.is_medium=TRUE,1,0)  as product_detail_is_medium,
if(pd.`data`.is_serializable=TRUE,1,0) as product_detail_is_serializable,
pd.`data`.length as product_detail_length,
pd.`data`.listing_id as product_detail_listing_id,
pd.`data`.packing_type_id as product_detail_packing_type_id,
pd.`data`.price as product_detail_price,
pd.`data`.product_title as product_detail_product_title,
pd.`data`.seller_id as product_detail_seller_id,
pd.`data`.serial_number_count as product_detail_serial_number_count,
pd.`data`.shipping_weight as product_detail_weight,
pd.`data`.sku as product_detail_sku,
from_unixtime(unix_timestamp(pd.`data`.updated_at)) as product_detail_updated_at_timestamp,
pd.`data`.wid as product_detail_wid,
'wsr' as  product_detail_warehouse_company,
if( pd.`data`.length is null or pd.`data`.length=0 or      pd.`data`.breadth is null or pd.`data`.breadth=0 or pd.`data`.height is null or pd.`data`.height=0 or    pd.`data`.shipping_weight is null or pd.`data`.shipping_weight=0,0,1)  AS product_detail_is_wid_lbhw_captured,
 pva.breadth as product_vertical_attribute_breadth,
pva.height as product_vertical_attribute_height,
pva.length as product_vertical_attribute_length,
pva.tolerance   as product_vertical_attribute_tolerance,
(
Case 
when pd.`data`.seller_id='fki' then 'fki'
when pd.`data`.seller_id='wsr' then 'wsr'
else shd.display_name
end
) as seller_dim_display_name ,
plhv.flipkart_selling_price as product_listing_dim_fsp,
plhv.mrp as product_listing_dim_mrp,
pahd.analytic_super_category as product_categorization_dim_super_category,
pahd.analytic_vertical as product_categorization_dim_vertical,
pahd.analytic_category as product_categorization_dim_category,
pahd.analytic_sub_category as product_categorization_dim_sub_category,
pahd.analytic_business_unit as product_categorization_dim_business_unit,
pahd.node_id as product_categorization_dim_node_id,
pahd.is_large as product_categorization_dim_is_large,
null AS product_detail_is_fbf,
if((pd.`data`.length * pd.`data`.breadth * pd.`data`.height) is not null and 
   (pd.`data`.length * pd.`data`.breadth * pd.`data`.height)>0,
   (pd.`data`.length * pd.`data`.breadth * pd.`data`.height),
   if(sspv.product_detail_volume is not null and sspv.product_detail_volume>0,sspv.product_detail_volume,
   pdhd.product_detail_volume)
   ) as product_detail_volume,
if(sspv.single_shipment_volume is not null and sspv.single_shipment_volume>0,sspv.single_shipment_volume,if (pdhd.single_shipment_volume is not null
and pdhd.single_shipment_volume>0,pdhd.single_shipment_volume,2*(pd.`data`.length * pd.`data`.breadth * pd.`data`.height))) as single_shipment_volume,
pd.`data`.lbh_captured_at as product_detail_lbh_captured_at,
lookup_date(pd.`data`.lbh_captured_at) as product_detail_lbh_captured_at_date_key,
lookup_time(pd.`data`.lbh_captured_at) as product_detail_lbh_captured_at_time_key,
pd.`data`.lbh_machine_id as product_detail_lbh_machine_id,
pd.`data`.lbh_machine_warehouse_id as product_detail_lbh_machine_warehouse_id,
pd.`data`.lbh_updated_by as product_detail_lbh_updated_by,
(
Case 
when (pd.`data`.seller_id='fki' or pd.`data`.seller_id='wsr' or shd.is_first_party_seller=TRUE) then 1
else 0
end
) as is_first_party_seller,
pahd.importance_type as product_attribute_importance_type,
pahd.core_flag as product_attribute_core_flag
from 
bigfoot_snapshot.dart_wsr_scp_warehouse_product_detail_3_view_total pd
left join 
(
select 
pva1.`data`.cms_vertical as cms_vertical,
max(pva1.`data`.breadth) as breadth,
max(pva1.`data`.height) as height,
max(pva1.`data`.length) as length,
max(pva1.`data`.tolerance)   as tolerance
from
bigfoot_snapshot.dart_wsr_scp_warehouse_product_vertical_attribute_1_view_total pva1
group by pva1.`data`.cms_vertical
) pva on pd.`data`.cms_vertical= pva.cms_vertical
left join 
(
select
shd1.seller_id as seller_id,
max(shd1.display_name) as display_name,
max(is_first_party_seller) as is_first_party_seller
from
bigfoot_external_neo.sp_seller__seller_hive_dim shd1
group by shd1.seller_id
) shd
 on pd.`data`.seller_id=shd.seller_id
left join bigfoot_external_neo.sp_product__listing_hive_dim plhv on pd.`data`.listing_id=plhv.listing_id
left join bigfoot_external_neo.sp_product__product_attribute_hive_dim pahd on pd.`data`.fsn=pahd.product_id	
left join bigfoot_external_neo.scp_warehouse__fc_single_shipment_product_vol_l2_hive_fact sspv on pd.`data`.fsn=sspv.product_detail_fsn
left join 
(
select
sub.product_detail_cms_vertical as product_detail_cms_vertical ,
percentile_approx(sub.single_shipment_volume,0.5) as single_shipment_volume,
percentile_approx(sub.product_detail_volume,0.5) as product_detail_volume
from
bigfoot_external_neo.scp_warehouse__fc_product_detail_hive_dim sub
group by sub.product_detail_cms_vertical
) pdhd
on pd.`data`.cms_vertical = pdhd.product_detail_cms_vertical
) final
