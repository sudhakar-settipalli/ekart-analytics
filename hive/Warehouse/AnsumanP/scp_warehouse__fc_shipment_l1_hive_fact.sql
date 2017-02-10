INSERT OVERWRITE TABLE fc_shipment_l1_hive_fact 
select 
final_query.shipment_display_id as shipment_display_id,
final_query.warehouse_company as warehouse_company,
final_query.warehouse_id as warehouse_id,
final_query.shipment_id as shipment_id,
final_query.shipment_tracking_id as shipment_tracking_id,
final_query.shipment_status as shipment_status,
final_query.shipment_packing_box_used as shipment_packing_box_used,
final_query.shipment_volume as shipment_volume,
final_query.shipment_packing_box_used_bucket as shipment_packing_box_used_bucket,
final_query.distinct_fsn_count as distinct_fsn_count,
final_query.distinct_cms_vertical_count as distinct_cms_vertical_count,
final_query.distinct_wid_count as distinct_wid_count,
final_query.shipment_is_fragile as shipment_is_fragile,
final_query.shipment_quantity as shipment_quantity,
final_query.shipment_packing_box_suggested_name as shipment_packing_box_suggested_name,
final_query.shipment_packing_box_suggested_volume as shipment_packing_box_suggested_volume,
final_query.shipment_destination_type as shipment_destination_type,
final_query.shipment_packing_box_suggested_bucket as shipment_packing_box_suggested_bucket,
final_query.shipment_box_used_packing_box_id as shipment_box_used_packing_box_id,
final_query.shipment_box_suggested_packing_box_id as shipment_box_suggested_packing_box_id,
final_query.is_packing_box_suggested as is_packing_box_suggested,
final_query.is_packing_box_used as is_packing_box_used,
final_query.is_suggested_packing_box_valid as is_suggested_packing_box_valid,
final_query.is_used_packing_box_valid as is_used_packing_box_valid,
final_query.used_packing_box_volume_order as used_packing_box_volume_order,
final_query.suggested_packing_box_volume_order as suggested_packing_box_volume_order,
final_query.used_packing_bucket_volume_order as used_packing_bucket_volume_order,
final_query.suggested_packing_bucket_volume_order as suggested_packing_bucket_volume_order,
final_query.is_box_suggested_new as is_box_suggested_new,
final_query.is_box_used_new as is_box_used_new,
final_query.is_exact_adhered as is_exact_adhered,
final_query.is_mobile_tablet_category as is_mobile_tablet_category,
final_query.is_mobile_adherence as is_mobile_adherence,
final_query.is_packed_in_lower_volume as is_packed_in_lower_volume,
final_query.is_bucket_adhered as is_bucket_adhered,
final_query.shipment_item_order_item_id as shipment_item_order_item_id,
final_query.shipment_item_is_excess as shipment_item_is_excess,
final_query.shipment_return_warehouse_id as shipment_return_warehouse_id,
final_query.shipment_is_tampered as shipment_is_tampered,
--new columns added for outer  packaging
final_query.box_suggested_outer_packing_box_id as box_suggested_outer_packing_box_id,
final_query.packing_box_suggested_outer_name as packing_box_suggested_outer_name,
final_query.packing_box_suggested_outer_bucket as packing_box_suggested_outer_bucket,
final_query.is_outer_packing_box_suggested  as is_outer_packing_box_suggested,
final_query.is_outer_suggested_packing_box_valid as is_outer_suggested_packing_box_valid,
final_query.suggested_outer_packing_box_volume_order as suggested_outer_packing_box_volume_order,
final_query.suggested_outer_packing_bucket_volume_order as suggested_outer_packing_bucket_volume_order,
final_query.is_box_outer_suggested_new as is_box_outer_suggested_new,
final_query.box_outer_used_packing_box_id as box_outer_used_packing_box_id,
final_query.packing_box_used_outer_name as packing_box_used_outer_name,
final_query.packing_box_used_outer_bucket as packing_box_used_outer_bucket,
final_query.is_outer_packing_box_used as is_outer_packing_box_used,
final_query.is_outer_used_packing_box_valid as is_outer_used_packing_box_valid,
final_query.used_outer_packing_box_volume_order as used_outer_packing_box_volume_order,
final_query.used_outer_packing_bucket_volume_order as used_outer_packing_bucket_volume_order,
final_query.is_box_outer_used_new as is_box_outer_used_new,
final_query.shipment_dispatched_date_key as shipment_dispatched_date_key,
final_query.is_exact_adhered_outer as is_exact_adhered_outer,
final_query.is_mobile_adherence_outer as is_mobile_adherence_outer,
final_query.is_packed_in_lower_volume_outer as is_packed_in_lower_volume_outer,
final_query.is_bucket_adhered_outer as is_bucket_adhered_outer,
final_query.shipment_outer_packing_box_suggested_volume as shipment_outer_packing_box_suggested_volume,
final_query.Box_Usage as Box_Usage,
final_query.SB_Usage as SB_Usage,
final_query.Label_Usage as Label_Usage,
final_query.Tape_Usage as Tape_Usage,
final_query.Invoice_Pouch_Usage as Invoice_Pouch_Usage,
final_query.2PL_Usage as 2PL_Usage,
final_query.Gift_Paper_Usage as Gift_Paper_Usage,
final_query.Extra_Tape_Usage as Extra_Tape_Usage,
final_query.Bubble_Usage as Bubble_Usage,
final_query.Void_Filler_Usage as Void_Filler_Usage,
final_query.Shrink_Wrap_Usage as  Shrink_Wrap_Usage,
(box_cost_mapping.rate_rs) as Box_Usage_Value,
(SB_cost_mapping.rate_rs)  as SB_Usage_Value,
(final_query.Label_Usage * other_pk_cost_mapping.Label_rate ) as Label_Usage_Value,
(final_query.Tape_Usage * other_pk_cost_mapping.Tape_rate)  as Tape_Usage_Value,
(final_query.Invoice_Pouch_Usage * other_pk_cost_mapping.Invoice_Pouch_rate)   as Invoice_Pouch_Usage_Value,
(final_query.2PL_Usage * other_pk_cost_mapping.2PL_rate ) as 2PL_Usage_Value,
(final_query.Gift_Paper_Usage * other_pk_cost_mapping.Gift_Paper_rate ) as Gift_Paper_Usage_Value,
(final_query.Extra_Tape_Usage * other_pk_cost_mapping.Tape_rate )  as Extra_Tape_Usage_Value,
(final_query.Bubble_Usage * other_pk_cost_mapping.Bubble_rate)  as Bubble_Usage_Value,
(final_query.Void_Filler_Usage * other_pk_cost_mapping.Void_Filler_rate)  as Void_Filler_Usage_Value,
(final_query.Shrink_Wrap_Usage * other_pk_cost_mapping.Shrink_Wrap_rate ) as  Shrink_Wrap_Usage_Value,
(nvl(box_cost_mapping.rate_rs,0) + nvl(SB_cost_mapping.rate_rs,0) + 
nvl((final_query.Label_Usage * other_pk_cost_mapping.Label_rate),0) + nvl((final_query.Tape_Usage * other_pk_cost_mapping.Tape_rate),0)  +
nvl((final_query.Invoice_Pouch_Usage * other_pk_cost_mapping.Invoice_Pouch_rate),0) + nvl((final_query.2PL_Usage * other_pk_cost_mapping.2PL_rate ),0) + 
nvl((final_query.Gift_Paper_Usage * other_pk_cost_mapping.Gift_Paper_rate ),0) + nvl((final_query.Extra_Tape_Usage * other_pk_cost_mapping.Tape_rate ),0) +
nvl((final_query.Bubble_Usage * other_pk_cost_mapping.Bubble_rate),0) + nvl((final_query.Void_Filler_Usage * other_pk_cost_mapping.Void_Filler_rate),0) + 
nvl((final_query.Shrink_Wrap_Usage  * other_pk_cost_mapping.Shrink_Wrap_rate ),0)) as  Total_Packaging_Usage_Value,
final_query.shipment_product_volume as shipment_product_volume,
lookupkey('warehouse_id',final_query.warehouse_id) as warehouse_id_key

from

(select 
inner_query.shipment_display_id as shipment_display_id,
inner_query.warehouse_company as warehouse_company,
inner_query.warehouse_id as warehouse_id,
inner_query.shipment_id as shipment_id,
inner_query.shipment_tracking_id as shipment_tracking_id,
inner_query.shipment_status as shipment_status,
inner_query.shipment_packing_box_used as shipment_packing_box_used,
inner_query.shipment_volume as shipment_volume,
inner_query.shipment_packing_box_used_bucket as shipment_packing_box_used_bucket,
inner_query.distinct_fsn_count as distinct_fsn_count,
inner_query.distinct_cms_vertical_count as distinct_cms_vertical_count,
inner_query.distinct_wid_count as distinct_wid_count,
inner_query.shipment_is_fragile as shipment_is_fragile,
inner_query.shipment_quantity as shipment_quantity,
inner_query.shipment_packing_box_suggested_name as shipment_packing_box_suggested_name,
inner_query.shipment_packing_box_suggested_volume as shipment_packing_box_suggested_volume,
inner_query.shipment_destination_type as shipment_destination_type,
inner_query.shipment_packing_box_suggested_bucket as shipment_packing_box_suggested_bucket,
inner_query.shipment_box_used_packing_box_id as shipment_box_used_packing_box_id,
inner_query.shipment_box_suggested_packing_box_id as shipment_box_suggested_packing_box_id,
inner_query.is_packing_box_suggested as is_packing_box_suggested,
inner_query.is_packing_box_used as is_packing_box_used,
inner_query.is_suggested_packing_box_valid as is_suggested_packing_box_valid,
inner_query.is_used_packing_box_valid as is_used_packing_box_valid,
inner_query.used_packing_box_volume_order as used_packing_box_volume_order,
inner_query.suggested_packing_box_volume_order as suggested_packing_box_volume_order,
inner_query.used_packing_bucket_volume_order as used_packing_bucket_volume_order,
inner_query.suggested_packing_bucket_volume_order as suggested_packing_bucket_volume_order,
inner_query.is_box_suggested_new as is_box_suggested_new,
inner_query.is_box_used_new as is_box_used_new,
if( shipment_packing_box_suggested_name=shipment_packing_box_used and (
warehouse_id='hyderabad_medchal_01' or  (is_mobile_tablet_category=1 and shipment_packing_box_used_bucket='SecurityBag') or
 is_mobile_tablet_category=0
    )
    ,1,0)*is_packing_box_suggested*is_suggested_packing_box_valid*
is_packing_box_used*is_used_packing_box_valid as is_exact_adhered,
inner_query.is_mobile_tablet_category as is_mobile_tablet_category,
if(is_mobile_tablet_category=1 and ( ((shipment_packing_box_suggested_name in ('B0','B28')) AND (shipment_packing_box_used='SB2' )) OR
    ((shipment_packing_box_suggested_name ='B37') AND (shipment_packing_box_used = 'SB3')) OR
    ((shipment_packing_box_suggested_name='B1') AND (shipment_packing_box_used='SB4')) OR
    ((shipment_packing_box_suggested_name in ('B0','B35')) AND (shipment_packing_box_used='SW'))
    )
   ,1,0) as is_mobile_adherence,
if(
if(
    (shipment_packing_box_suggested_bucket=shipment_packing_box_used_bucket and ( (warehouse_id='hyderabad_medchal_01') or 
is_mobile_tablet_category=0)) OR
     (is_mobile_tablet_category=1 and shipment_packing_box_used_bucket  = 'SecurityBag') OR
     (shipment_packing_box_suggested_bucket='brown_box' and shipment_packing_box_used_bucket ='LifestyleBox') OR
     (shipment_packing_box_suggested_bucket='LifestyleBox' and shipment_packing_box_used_bucket ='brown_box')
  ,1,0) =1 
AND
(shipment_packing_box_used<>shipment_packing_box_suggested_name and used_packing_box_volume_order < suggested_packing_box_volume_order),
  1,0)*
is_box_suggested_new
*
is_box_used_new
as is_packed_in_lower_volume,
if(
    (shipment_packing_box_suggested_bucket=shipment_packing_box_used_bucket and ( (warehouse_id='hyderabad_medchal_01') or 
is_mobile_tablet_category=0)) OR
     (is_mobile_tablet_category=1 and shipment_packing_box_used_bucket  = 'SecurityBag') OR
     (shipment_packing_box_suggested_bucket='brown_box' and shipment_packing_box_used_bucket ='LifestyleBox') OR
     (shipment_packing_box_suggested_bucket='LifestyleBox' and shipment_packing_box_used_bucket ='brown_box')
  ,1,0) as is_bucket_adhered,
  inner_query.shipment_item_order_item_id as shipment_item_order_item_id,
inner_query.shipment_item_is_excess as shipment_item_is_excess,
inner_query.shipment_return_warehouse_id as shipment_return_warehouse_id,
inner_query.shipment_is_tampered as shipment_is_tampered,

--new columns added for outer  packaging
inner_query.box_suggested_outer_packing_box_id as box_suggested_outer_packing_box_id,
inner_query.packing_box_suggested_outer_name as packing_box_suggested_outer_name,
inner_query.packing_box_suggested_outer_bucket as packing_box_suggested_outer_bucket,
inner_query.is_outer_packing_box_suggested  as is_outer_packing_box_suggested,
inner_query.is_outer_suggested_packing_box_valid as is_outer_suggested_packing_box_valid,
inner_query.suggested_outer_packing_box_volume_order as suggested_outer_packing_box_volume_order,
inner_query.suggested_outer_packing_bucket_volume_order as suggested_outer_packing_bucket_volume_order,
inner_query.is_box_outer_suggested_new as is_box_outer_suggested_new,

inner_query.box_outer_used_packing_box_id as box_outer_used_packing_box_id,
inner_query.packing_box_used_outer_name as packing_box_used_outer_name,
inner_query.packing_box_used_outer_bucket as packing_box_used_outer_bucket,
inner_query.is_outer_packing_box_used as is_outer_packing_box_used,
inner_query.is_outer_used_packing_box_valid as is_outer_used_packing_box_valid,
inner_query.used_outer_packing_box_volume_order as used_outer_packing_box_volume_order,
inner_query.used_outer_packing_bucket_volume_order as used_outer_packing_bucket_volume_order,
inner_query.is_box_outer_used_new as is_box_outer_used_new,
inner_query.shipment_dispatched_date_key as shipment_dispatched_date_key,
(
Case 
when is_mobile_tablet_category=0 and (packing_box_used_outer_name=packing_box_suggested_outer_name or 
(
(shipment_packing_box_suggested_name like 'M1%' and packing_box_used_outer_name='M1')
or (shipment_packing_box_suggested_name like 'M2%' and packing_box_used_outer_name='M2')
or (shipment_packing_box_suggested_name like 'M3%' and packing_box_used_outer_name='M3')
or (shipment_packing_box_suggested_name like 'M4%' and packing_box_used_outer_name='M4')
or (shipment_packing_box_suggested_name like 'M5%' and packing_box_used_outer_name='M5')
) )
then 1
else 0
end
)
*is_outer_packing_box_suggested*is_outer_suggested_packing_box_valid*
is_outer_packing_box_used*is_outer_used_packing_box_valid as is_exact_adhered_outer,
(
Case 
when (is_mobile_tablet_category=1 and warehouse_id<>'chn_puzhal_01' and (packing_box_used_outer_name=packing_box_suggested_outer_name or 
((shipment_packing_box_suggested_name in ('B0','B35') or shipment_packing_box_suggested_name like 'M%') AND (packing_box_used_outer_name='SW'))
)
)
or 
(
(is_mobile_tablet_category=1 and warehouse_id='chn_puzhal_01'and 
(packing_box_used_outer_name=packing_box_suggested_outer_name or 
((shipment_packing_box_suggested_name like 'M1%' and packing_box_used_outer_name='M1')
or (shipment_packing_box_suggested_name like 'M2%' and packing_box_used_outer_name='M2')
or (shipment_packing_box_suggested_name like 'M3%' and packing_box_used_outer_name='M3')
or (shipment_packing_box_suggested_name like 'M4%' and packing_box_used_outer_name='M4')
or (shipment_packing_box_suggested_name like 'M5%' and packing_box_used_outer_name='M5'))
)
)  
)
then 1
else 0
end
)  as is_mobile_adherence_outer
,
if(
(
Case when packing_box_suggested_outer_bucket=packing_box_used_outer_bucket 
OR
(shipment_packing_box_suggested_bucket='brown_box' and packing_box_used_outer_bucket ='LifestyleBox') OR
(shipment_packing_box_suggested_bucket='LifestyleBox' and packing_box_used_outer_bucket ='brown_box')
then 1 
else 0
end
) =1 
AND
(packing_box_used_outer_name<>packing_box_suggested_outer_name and used_outer_packing_box_volume_order < suggested_outer_packing_box_volume_order),
  1,0)*
is_box_outer_suggested_new*
is_box_outer_used_new
as is_packed_in_lower_volume_outer,
(
Case when packing_box_suggested_outer_bucket=packing_box_used_outer_bucket 
OR
(shipment_packing_box_suggested_bucket='brown_box' and packing_box_used_outer_bucket ='LifestyleBox') OR
(shipment_packing_box_suggested_bucket='LifestyleBox' and packing_box_used_outer_bucket ='brown_box')
then 1 
else 0
end
) as is_bucket_adhered_outer,
inner_query.shipment_outer_packing_box_suggested_volume as shipment_outer_packing_box_suggested_volume,
Case when packing_box_used_outer_bucket ='brown_box' then packing_box_used_outer_name 
when packing_box_used_outer_name='GFT' then packing_box_suggested_outer_name 
when ((is_mobile_tablet_category=1 and shipment_packing_box_suggested_bucket='brown_box' and packing_box_used_outer_bucket='SecurityBag')
or
(is_mobile_tablet_category=1 and packing_box_used_outer_name='SW')) then shipment_packing_box_suggested_name
end as Box_Usage,
Case when packing_box_used_outer_bucket='SecurityBag' then packing_box_used_outer_name end as SB_Usage,
2 as Label_Usage,
Case when
((is_mobile_tablet_category=0 and packing_box_used_outer_bucket='SecurityBag' )
or
(is_mobile_tablet_category=1 and nvl(shipment_packing_box_suggested_bucket,'') <> 'brown_box' and packing_box_used_outer_bucket='SecurityBag' ))
then
packing_box_used_outer_breadth
when
(packing_box_used_outer_bucket ='brown_box' or packing_box_used_outer_name='GFT' or (is_mobile_tablet_category=1 and packing_box_used_outer_name='SW' ))
then (2*(packing_box_used_outer_length + packing_box_used_outer_height))
when (is_mobile_tablet_category=1 and shipment_packing_box_suggested_bucket='brown_box' and packing_box_used_outer_bucket='SecurityBag')  
then (2*(packing_box_suggested_length + packing_box_suggested_height) + packing_box_used_outer_breadth)
end  as Tape_Usage,
Case when 
(packing_box_used_outer_bucket ='brown_box' or packing_box_used_outer_bucket ='2Ply' or packing_box_used_outer_name='GFT'
or (is_mobile_tablet_category=1 and packing_box_used_outer_name='SW')) then 1 end as Invoice_Pouch_Usage,
Case when packing_box_used_outer_bucket ='2Ply' then (2*(max_product_detail_length*max_product_detail_breadth + max_product_detail_breadth*total_product_detail_height
+ max_product_detail_length*total_product_detail_height)) end as 2PL_Usage,
Case when packing_box_used_outer_name='GFT' then 2 end as Gift_Paper_Usage,
Case when packing_box_used_outer_bucket ='2Ply' then (6*(max_product_detail_length + total_product_detail_height)) end as Extra_Tape_Usage,
Case when (packing_box_used_outer_bucket ='brown_box' 
or (is_mobile_tablet_category=1 and shipment_packing_box_suggested_bucket='brown_box' and packing_box_used_outer_bucket='SecurityBag' )) 
then (12 * 0.6 * 2*(packing_box_used_outer_breadth + packing_box_used_outer_height))
when ((is_mobile_tablet_category=0 and is_pack_cost_clothing_cat=0   and packing_box_used_outer_bucket='SecurityBag') 
or (is_mobile_tablet_category=1 and nvl(shipment_packing_box_suggested_bucket,'') <> 'brown_box' and packing_box_used_outer_bucket='SecurityBag'))
then (12 * 2*(max_product_detail_breadth + total_product_detail_height))
when packing_box_used_outer_bucket ='2Ply'
then (12 * 0.6 * 2*(max_product_detail_breadth + total_product_detail_height))
when (is_mobile_tablet_category=1 and packing_box_used_outer_name='SW' )
then  (12 * 0.6 * 2*(packing_box_used_outer_breadth + packing_box_used_outer_height))
end as Bubble_Usage,
Case when (packing_box_used_outer_bucket ='brown_box')  then 
0.3*0.15 * shipment_volume
when (packing_box_used_outer_name='GFT') then 
0.3*0.15 * shipment_outer_packing_box_suggested_volume
end as Void_Filler_Usage,
Case when (is_mobile_tablet_category=1 and packing_box_used_outer_name='SW' ) then (12 * (packing_box_used_outer_length + 3)) end as  Shrink_Wrap_Usage,
inner_query.shipment_product_volume as shipment_product_volume

from 
(
select 
a.shipment_display_id as shipment_display_id , 
max(a.warehouse_company) as warehouse_company , 
max(a.shipment_warehouse_id) as warehouse_id , 
max(a.shipment_id) as shipment_id , 
max(a.box_tracking_id) as shipment_tracking_id , 
max(a.shipment_status) as shipment_status , 
max(a.packing_box_used_name) as shipment_packing_box_used , 
if(max(pbou.packing_box_volume) is not null ,max(pbou.packing_box_volume), max(pbu.packing_box_volume)) as shipment_volume , 
max(a.packing_box_used_bucket) as shipment_packing_box_used_bucket , 
count ( distinct b.product_detail_fsn) as distinct_fsn_count , 
count ( distinct b.product_detail_cms_vertical) as distinct_cms_vertical_count , 
count ( distinct b.product_detail_wid) as distinct_wid_count , 
max(b.product_detail_is_fragile) as shipment_is_fragile , 
sum(a.shipment_item_quantity) as shipment_quantity ,
max(a.packing_box_suggested_name) as shipment_packing_box_suggested_name,
max(pbs.packing_box_volume) as shipment_packing_box_suggested_volume,
max(a.shipment_destination_type) as shipment_destination_type,
max(a.packing_box_suggested_bucket) as shipment_packing_box_suggested_bucket,
max(a.box_used_packing_box_id) as shipment_box_used_packing_box_id,
max(a.box_suggested_packing_box_id) as shipment_box_suggested_packing_box_id,
if(isnull(max(a.box_suggested_packing_box_id)),0,1) as is_packing_box_suggested,
if(isnull(max(a.box_used_packing_box_id)),0,1) as is_packing_box_used,
max(pbs.packing_box_is_valid) as is_suggested_packing_box_valid,
max(pbu.packing_box_is_valid) as is_used_packing_box_valid,
max(pbu.packing_box_volume_order) as used_packing_box_volume_order,
max(pbs.packing_box_volume_order) as suggested_packing_box_volume_order,
max(pbu.packing_box_bucket_volume_order) as used_packing_bucket_volume_order,
max(pbs.packing_box_bucket_volume_order) as suggested_packing_bucket_volume_order,
if(   
if(isnull(max(a.box_suggested_packing_box_id)),0,1) = 0
or 
max(pbs.packing_box_is_valid)=0,0,1) as is_box_suggested_new,
if(   
if(isnull(max(a.box_used_packing_box_id)),0,1) = 0
or 
max(pbu.packing_box_is_valid)=0,0,1) as is_box_used_new,
max(a.is_mobile_tablet_category) as is_mobile_tablet_category,

max(a.shipment_item_order_item_id)  as shipment_item_order_item_id,
max(a.shipment_item_is_excess)  as shipment_item_is_excess,
max(a.shipment_return_warehouse_id)  as shipment_return_warehouse_id,
max(a.shipment_is_tampered) as shipment_is_tampered,
--new columns added for outer  packaging

max(a.box_suggested_outer_packing_box_id) as box_suggested_outer_packing_box_id,
max(a.packing_box_suggested_outer_name) as packing_box_suggested_outer_name,
max(a.packing_box_suggested_outer_bucket) as packing_box_suggested_outer_bucket,
if(isnull(max(a.box_suggested_outer_packing_box_id)),0,1)  as is_outer_packing_box_suggested,
max(pbos.packing_box_is_valid) as is_outer_suggested_packing_box_valid,
max(pbos.packing_box_volume_order) as suggested_outer_packing_box_volume_order,
max(pbos.packing_box_bucket_volume_order) as suggested_outer_packing_bucket_volume_order,
if(   
if(isnull(max(a.box_suggested_outer_packing_box_id)),0,1) = 0
or 
max(pbos.packing_box_is_valid)=0,0,1) as is_box_outer_suggested_new,

max(a.box_outer_used_packing_box_id) as box_outer_used_packing_box_id,
max(a.packing_box_used_outer_name) as packing_box_used_outer_name,
max(a.packing_box_used_outer_bucket) as packing_box_used_outer_bucket,
if(isnull(max(a.box_outer_used_packing_box_id)),0,1) as is_outer_packing_box_used,
max(pbou.packing_box_is_valid) as is_outer_used_packing_box_valid,
max(pbou.packing_box_volume_order) as used_outer_packing_box_volume_order,
max(pbou.packing_box_bucket_volume_order) as used_outer_packing_bucket_volume_order,
if(   
if(isnull(max(a.box_outer_used_packing_box_id)),0,1) = 0
or 
max(pbou.packing_box_is_valid)=0,0,1) as is_box_outer_used_new,
max(a.shipment_dispatched_date_key) as shipment_dispatched_date_key,
if(max(pbos.packing_box_volume) is not null, max(pbos.packing_box_volume),max(pbs.packing_box_volume)) as shipment_outer_packing_box_suggested_volume,
max(Case when b.product_categorization_dim_category in ('MenClothing','WomenClothing','MenFW','WomenFW','KidClothing')  then 1 else 0 end) as is_pack_cost_clothing_cat, 
max(b.product_detail_length) as max_product_detail_length,
max(b.product_detail_breadth) as max_product_detail_breadth,
sum(product_detail_height*shipment_item_quantity) as total_product_detail_height,
max(a.packing_box_used_outer_breadth) as packing_box_used_outer_breadth,
max(a.packing_box_used_outer_length) as packing_box_used_outer_length,
max(a.packing_box_used_outer_height) as packing_box_used_outer_height,
max(a.packing_box_suggested_length) as packing_box_suggested_length,
max(a.packing_box_suggested_height) as packing_box_suggested_height,
sum(b.product_detail_length * b.product_detail_breadth * product_detail_height * shipment_item_quantity) as shipment_product_volume

from bigfoot_external_neo.scp_warehouse__fc_shipment_item_l0_hive_fact as a left outer join bigfoot_external_neo.scp_warehouse__fc_product_detail_hive_dim as b  on  concat(a.shipment_item_product_id,a.warehouse_company)=b.product_detail_product_id
left join bigfoot_external_neo.scp_warehouse__fc_packing_box_l0_hive_fact pbu on (a.box_used_packing_box_id=pbu.packing_box_id  and a.warehouse_company=pbu.packing_box_company) 
left join bigfoot_external_neo.scp_warehouse__fc_packing_box_l0_hive_fact pbs on (a.box_suggested_packing_box_id=pbs.packing_box_id  and a.warehouse_company=pbs.packing_box_company) 
left join bigfoot_external_neo.scp_warehouse__fc_packing_box_l0_hive_fact pbou on (a.box_outer_used_packing_box_id=pbou.packing_box_id  and a.warehouse_company=pbou.packing_box_company) 
left join bigfoot_external_neo.scp_warehouse__fc_packing_box_l0_hive_fact pbos on (a.box_suggested_outer_packing_box_id=pbos.packing_box_id  and a.warehouse_company=pbos.packing_box_company) 
where  a.shipment_status <>'cancelled' and a.shipment_item_is_excess=0  
group by a.shipment_display_id ) inner_query ) final_query 
left join bigfoot_common.fc_packaging_cost_mapping box_cost_mapping on 
(substring(final_query.shipment_dispatched_date_key,1,6)=box_cost_mapping.month and final_query.warehouse_id=box_cost_mapping.fc_name and final_query.Box_Usage = box_cost_mapping.item)
left join bigfoot_common.fc_packaging_cost_mapping SB_cost_mapping on
(substring(final_query.shipment_dispatched_date_key,1,6)=SB_cost_mapping.month and final_query.warehouse_id=SB_cost_mapping.fc_name and final_query.SB_Usage = SB_cost_mapping.item)
left join 
(
select 
month,
fc_name,
max(
Case when item='2PL' then rate_rs else null end
) as 2PL_rate ,
max(
Case when item='Tape' then rate_rs else null end
) as Tape_rate ,
max(
Case when item='Invoice Pouch' then rate_rs else null end
) as Invoice_Pouch_rate ,
max(
Case when item='Label' then rate_rs else null end
) as Label_rate ,
max(
Case when item='Bubble' then rate_rs else null end
) as Bubble_rate ,
max(
Case when item='Gift Paper' then rate_rs else null end
) as Gift_Paper_rate ,
max(
Case when item='Void Filler' then rate_rs else null end
) as Void_Filler_rate ,
max(
Case when item='Shrink Wrap' then rate_rs else null end
) as Shrink_Wrap_rate
from bigfoot_common.fc_packaging_cost_mapping
where item in ('Tape',
'Invoice Pouch',
'Label',
'Bubble', 
'Gift Paper',
'Void Filler',
'2PL',
'Shrink Wrap')
group by 
month,
fc_name
) other_pk_cost_mapping on
(substring(final_query.shipment_dispatched_date_key,1,6)=other_pk_cost_mapping.month and final_query.warehouse_id=other_pk_cost_mapping.fc_name )
;
