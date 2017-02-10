INSERT OVERWRITE TABLE fc_shipment_item_l0_hive_fact
select
final_table.warehouse_company as warehouse_company,
final_table.shipment_item_id as shipment_item_id,
final_table.shipment_item_is_excess as shipment_item_is_excess,
final_table.shipment_item_on_hold_reason_id as shipment_item_on_hold_reason_id,
final_table.shipment_item_order_item_id as shipment_item_order_item_id,
final_table.shipment_item_product_id as shipment_item_product_id,
final_table.shipment_item_product_key as shipment_item_product_key,
final_table.shipment_item_quantity as shipment_item_quantity,
final_table.shipment_item_status as shipment_item_status,
final_table.shipment_item_updated_timestamp as shipment_item_updated_timestamp,
final_table.shipment_item_updated_date_key as shipment_item_updated_date_key,
final_table.shipment_item_updated_time_key as shipment_item_updated_time_key,
final_table.shipment_item_cancelled_timestamp as shipment_item_cancelled_timestamp,
final_table.shipment_item_cancelled_date_key as shipment_item_cancelled_date_key,
final_table.shipment_item_cancelled_time_key as shipment_item_cancelled_time_key,
--shipment 
final_table.shipment_id as shipment_id,
final_table.shipment_courier_name as shipment_courier_name,
final_table.shipment_created_timestamp as shipment_created_timestamp,
final_table.shipment_created_date_key as shipment_created_date_key,
final_table.shipment_created_time_key as shipment_created_time_key,
final_table.shipment_destination_type as shipment_destination_type,
final_table.shipment_display_id as shipment_display_id,
final_table.shipment_warehouse_id as shipment_warehouse_id,
final_table.shipment_is_large_warehouse as shipment_is_large_warehouse,
final_table.shipment_hold_initiated_by as shipment_hold_initiated_by,
final_table.shipment_original_shipment_id as shipment_original_shipment_id,
final_table.shipment_qc_done_timestamp as shipment_qc_done_timestamp,
final_table.shipment_qc_done_date_key as shipment_qc_done_date_key,
final_table.shipment_qc_done_time_key as shipment_qc_done_time_key,
final_table.shipment_qc_done_by as shipment_qc_done_by,
final_table.shipment_return_warehouse_id as shipment_return_warehouse_id,
final_table.shipment_ship_method as shipment_ship_method,
final_table.shipment_status as shipment_status,
final_table.shipment_updated_timestamp as shipment_updated_timestamp,
final_table.shipment_updated_date_key as shipment_updated_date_key,
final_table.shipment_updated_time_key as shipment_updated_time_key,
final_table.shipment_cancelled_timestamp as shipment_cancelled_timestamp,
final_table.shipment_cancelled_date_key as shipment_cancelled_date_key,
final_table.shipment_cancelled_time_key as shipment_cancelled_time_key,
final_table.shipment_rto_received_timestamp as shipment_rto_received_timestamp,
final_table.shipment_rto_received_date_key as shipment_rto_received_date_key,
final_table.shipment_rto_received_time_key as shipment_rto_received_time_key,
final_table.shipment_rvp_received_timestamp as shipment_rvp_received_timestamp,
final_table.shipment_rvp_received_date_key as shipment_rvp_received_date_key,
final_table.shipment_rvp_received_time_key as shipment_rvp_received_time_key,
--shipment event
final_table.shipment_rto_mh_scan_timestamp as shipment_rto_mh_scan_timestamp,
final_table.shipment_rto_mh_scan_date_key as shipment_rto_mh_scan_date_key,
final_table.shipment_rto_mh_scan_time_key as shipment_rto_mh_scan_time_key,
final_table.shipment_rvp_mh_scan_timestamp as shipment_rvp_mh_scan_timestamp, 
final_table.shipment_rvp_mh_scan_date_key as shipment_rvp_mh_scan_date_key,
final_table.shipment_rvp_mh_scan_time_key as shipment_rvp_mh_scan_time_key, 
final_table.shipment_dispatched_timestamp as shipment_dispatched_timestamp,
final_table.shipment_dispatched_date_key as shipment_dispatched_date_key,
final_table.shipment_dispatched_time_key as shipment_dispatched_time_key,
final_table.shipment_dispatched_by as shipment_dispatched_by,
final_table.shipment_return_mh_scan_done_by as shipment_return_mh_scan_done_by,
-- box
final_table.box_used_packing_box_id as box_used_packing_box_id,
final_table.box_actual_weight as box_actual_weight,
final_table.box_suggested_packing_box_id as box_suggested_packing_box_id,
final_table.box_tracking_id as box_tracking_id,
final_table.box_shipment_calculated_weight as box_shipment_calculated_weight,
-- packing box
final_table.packing_box_used_name as packing_box_used_name,
final_table.packing_box_used_length as packing_box_used_length,
final_table.packing_box_used_breadth as packing_box_used_breadth,
final_table.packing_box_used_height as packing_box_used_height,
final_table.packing_box_used_is_active as packing_box_used_is_active,
final_table.packing_box_used_bucket as packing_box_used_bucket,
final_table.packing_box_suggested_name as packing_box_suggested_name,
final_table.packing_box_suggested_length as packing_box_suggested_length,
final_table.packing_box_suggested_breadth as packing_box_suggested_breadth,
final_table.packing_box_suggested_height as packing_box_suggested_height,
final_table.packing_box_suggested_is_active as packing_box_suggested_is_active,
final_table.packing_box_suggested_bucket as packing_box_suggested_bucket,
--product details
final_table.product_length as product_length,
final_table.product_breadth as product_breadth,
final_table.product_height as product_height,
final_table.product_cms_vertical as product_cms_vertical,
final_table.product_seller_id as product_seller_id,
final_table.product_weight as product_weight,
final_table.product_listing_id as product_listing_id,
final_table.is_non_wsr_fbf_shipment as is_non_wsr_fbf_shipment,
--new columns addition
final_table.shipment_warehouse_dim_key as shipment_warehouse_dim_key,
final_table.shipment_ekart_dispatch_by_date as  shipment_ekart_dispatch_by_date, 
final_table.shipment_ekart_dispatch_by_date_key as shipment_ekart_dispatch_by_date_key ,
final_table.shipment_ekart_dispatch_by_time_key as shipment_ekart_dispatch_by_time_key ,
final_table.shipment_entity_type as  shipment_entity_type,
final_table.shipment_ff_dispatch_by_date as shipment_ff_dispatch_by_date,
final_table.shipment_ff_dispatch_by_date_key as shipment_ff_dispatch_by_date_key ,
final_table.shipment_ff_dispatch_by_time_key as shipment_ff_dispatch_by_time_key ,
final_table.shipment_is_reshipment as shipment_is_reshipment,
final_table.shipment_item_entity_type as shipment_item_entity_type,
final_table.shipment_item_on_hold_reason as shipment_item_on_hold_reason,
final_table.shipment_item_polymorphic_id as shipment_item_polymorphic_id,
final_table.shipment_item_polymorphic_type as shipment_item_polymorphic_type,
final_table.shipment_item_quantity_lost_during_dispatch as shipment_item_quantity_lost_during_dispatch,
final_table.shipment_item_quantity_packed as shipment_item_quantity_packed,
final_table.shipment_item_quantity_received as shipment_item_quantity_received,
final_table.box_ideal_packing_box_id as box_ideal_packing_box_id,
final_table.box_bag_serial_number as box_bag_serial_number,
final_table.box_entity_type as box_entity_type,
final_table.is_mobile_tablet_category as is_mobile_tablet_category,
final_table.is_box_suggested as is_box_suggested,
final_table.is_box_used as is_box_used,
--new event columns
final_table.shipment_packed_timestamp as shipment_packed_timestamp,
final_table.shipment_packed_date_key as shipment_packed_date_key,
final_table.shipment_packed_time_key as shipment_packed_time_key,
final_table.shipment_packed_by as shipment_packed_by,
--packing box new columns
final_table.packing_box_used_internal_name as packing_box_used_internal_name,
final_table.packing_box_suggested_internal_name as packing_box_suggested_internal_name,
final_table.is_tampered as shipment_is_tampered,
-- new box columns to be added 
final_table.box_outer_used_packing_box_id as box_outer_used_packing_box_id,
final_table.box_suggested_outer_packing_box_id as box_suggested_outer_packing_box_id,
--new packing box columns to be added
final_table.packing_box_used_outer_name as packing_box_used_outer_name,
final_table.packing_box_used_outer_length as packing_box_used_outer_length,
final_table.packing_box_used_outer_breadth as packing_box_used_outer_breadth,
final_table.packing_box_used_outer_height as packing_box_used_outer_height,
final_table.packing_box_used_outer_is_active as packing_box_used_outer_is_active,
final_table.packing_box_used_outer_bucket as packing_box_used_outer_bucket,
final_table.packing_box_suggested_outer_name as packing_box_suggested_outer_name,
final_table.packing_box_suggested_outer_length as packing_box_suggested_outer_length,
final_table.packing_box_suggested_outer_breadth as packing_box_suggested_outer_breadth,
final_table.packing_box_suggested_outer_height as packing_box_suggested_outer_height,
final_table.packing_box_suggested_outer_is_active as packing_box_suggested_outer_is_active,
final_table.packing_box_suggested_outer_bucket as packing_box_suggested_outer_bucket,
--new columns for internal name
final_table.packing_box_used_outer_internal_name as packing_box_used_outer_internal_name,
final_table.packing_box_suggested_outer_internal_name as packing_box_suggested_outer_internal_name,
final_table.shipment_item_quantity_original_table as shipment_item_quantity_original_table,
final_table.box_item_id as box_item_id
from
(select 'wsr' as warehouse_company,
si_wsr.data.id as shipment_item_id,
IF(si_wsr.`data`.is_excess=TRUE,1,0) as shipment_item_is_excess,
si_wsr.`data`.on_hold_reason_id as shipment_item_on_hold_reason_id,
si_wsr.`data`.order_item_id as shipment_item_order_item_id,
si_wsr.`data`.product_id as shipment_item_product_id,
lookupkey('product_detail_product_id',concat(si_wsr.`data`.product_id,'wsr')) as shipment_item_product_key,
bi_wsr.data.quantity as shipment_item_quantity,
si_wsr.`data`.status as shipment_item_status,
si_wsr.`data`.updated_at as shipment_item_updated_timestamp,
lookup_date(si_wsr.`data`.updated_at) as shipment_item_updated_date_key,
lookup_time(si_wsr.`data`.updated_at) as shipment_item_updated_time_key,
IF(lower(si_wsr.`data`.status)='cancelled',s_wsr.`data`.updated_at,null) as shipment_item_cancelled_timestamp,
lookup_date(IF(lower(si_wsr.`data`.status)='cancelled',s_wsr.`data`.updated_at,null)) as shipment_item_cancelled_date_key,
lookup_time(IF(lower(si_wsr.`data`.status)='cancelled',s_wsr.`data`.updated_at,null)) as shipment_item_cancelled_time_key,
--shipment 
s_wsr.data.id as shipment_id,
s_wsr.`data`.courier_name as shipment_courier_name,
s_wsr.`data`.created_at as shipment_created_timestamp,
lookup_date(s_wsr.`data`.created_at) as shipment_created_date_key,
lookup_time(s_wsr.`data`.created_at) as shipment_created_time_key,
s_wsr.`data`.destination_type as shipment_destination_type,
s_wsr.`data`.display_id as shipment_display_id,
s_wsr.`data`.warehouse_id as shipment_warehouse_id,
if(s_wsr.`data`.warehouse_id like '%0_L', 1,0) as shipment_is_large_warehouse,
(case when s_wsr.`data`.warehouse_initiated_hold = TRUE then 'warehouse_initiated_hold' else NULL end) as shipment_hold_initiated_by,
s_wsr.`data`.original_shipment_id as shipment_original_shipment_id,
s_wsr.`data`.qc_done_at as shipment_qc_done_timestamp,
lookup_date(s_wsr.`data`.qc_done_at) as shipment_qc_done_date_key,
lookup_time(s_wsr.`data`.qc_done_at) as shipment_qc_done_time_key,
s_wsr.`data`.qc_done_by as shipment_qc_done_by,
s_wsr.`data`.return_warehouse_id as shipment_return_warehouse_id,
s_wsr.`data`.ship_method as shipment_ship_method,
s_wsr.`data`.status as shipment_status,
s_wsr.`data`.updated_at as shipment_updated_timestamp,
lookup_date(s_wsr.`data`.updated_at) as shipment_updated_date_key,
lookup_time(s_wsr.`data`.updated_at) as shipment_updated_time_key,
IF(s_wsr.`data`.status='cancelled',si_wsr.`data`.updated_at,null) as shipment_cancelled_timestamp,
lookup_date(IF(s_wsr.`data`.status='cancelled',si_wsr.`data`.updated_at,null)) as shipment_cancelled_date_key,
lookup_time(IF(s_wsr.`data`.status='cancelled',si_wsr.`data`.updated_at,null)) as shipment_cancelled_time_key,
IF(s_wsr.`data`.status='returned',si_wsr.`data`.updated_at,null) as shipment_rto_received_timestamp,
lookup_date(IF(s_wsr.`data`.status='returned',si_wsr.`data`.updated_at,null)) as shipment_rto_received_date_key,
lookup_time(IF(s_wsr.`data`.status='returned',si_wsr.`data`.updated_at,null)) as shipment_rto_received_time_key,
IF(s_wsr.`data`.status='received',si_wsr.`data`.updated_at,null) as shipment_rvp_received_timestamp,
lookup_date(IF(s_wsr.`data`.status='received',si_wsr.`data`.updated_at,null)) as shipment_rvp_received_date_key,
lookup_time(IF(s_wsr.`data`.status='received',si_wsr.`data`.updated_at,null)) as shipment_rvp_received_time_key,
--shipment event
IF(s_wsr.`data`.destination_type = 'customer',from_unixtime(cast(rse_wsr.eventtime/1000 as bigint)),null) as shipment_rto_mh_scan_timestamp,
lookup_date(IF(s_wsr.`data`.destination_type = 'customer',from_unixtime(cast(rse_wsr.eventtime/1000 as bigint)),null)) as shipment_rto_mh_scan_date_key,
lookup_time(IF(s_wsr.`data`.destination_type = 'customer',from_unixtime(cast(rse_wsr.eventtime/1000 as bigint)),null)) as shipment_rto_mh_scan_time_key,
IF(s_wsr.`data`.destination_type = 'warehouse',from_unixtime(cast(rvpse_wsr.eventtime/1000 as bigint)),null) as shipment_rvp_mh_scan_timestamp,
lookup_date(IF(s_wsr.`data`.destination_type = 'warehouse',from_unixtime(cast(rvpse_wsr.eventtime/1000 as bigint)),null)) as shipment_rvp_mh_scan_date_key,
lookup_time(IF(s_wsr.`data`.destination_type = 'warehouse',from_unixtime(cast(rvpse_wsr.eventtime/1000 as bigint)),null)) as shipment_rvp_mh_scan_time_key, 
from_unixtime(cast(dse_wsr.eventtime/1000 as bigint)) as shipment_dispatched_timestamp,
lookup_date(from_unixtime(cast(dse_wsr.eventtime/1000 as bigint))) as shipment_dispatched_date_key,
lookup_time(from_unixtime(cast(dse_wsr.eventtime/1000 as bigint))) as shipment_dispatched_time_key,
dse_wsr.done_by as shipment_dispatched_by,
IF(s_wsr.`data`.destination_type = 'customer',rse_wsr.done_by,rvpse_wsr.done_by) as shipment_return_mh_scan_done_by,
-- box
b_wsr.`data`.actual_box_id as box_used_packing_box_id,
b_wsr.`data`.actual_weight as box_actual_weight,
b_wsr.`data`.suggested_box_id as box_suggested_packing_box_id,
b_wsr.`data`.tracking_id as box_tracking_id,
b_wsr.`data`.shipment_weight as box_shipment_calculated_weight,
-- packing box
upb_wsr.`data`.display_name as packing_box_used_name,
upb_wsr.`data`.length as packing_box_used_length,
upb_wsr.`data`.breadth as packing_box_used_breadth,
upb_wsr.`data`.height as packing_box_used_height,
if(upb_wsr.`data`.is_active=TRUE,1,0) as packing_box_used_is_active,
upb_wsr.`data`.packing_bucket as packing_box_used_bucket,
spb_wsr.`data`.display_name as packing_box_suggested_name,
spb_wsr.`data`.length as packing_box_suggested_length,
spb_wsr.`data`.breadth as packing_box_suggested_breadth,
spb_wsr.`data`.height as packing_box_suggested_height,
IF(spb_wsr.`data`.is_active=TRUE,1,0) as packing_box_suggested_is_active,
spb_wsr.`data`.packing_bucket as packing_box_suggested_bucket,
--product details
pd_wsr.`data`.length as product_length,
pd_wsr.`data`.breadth as product_breadth,
pd_wsr.`data`.height as product_height,
pd_wsr.`data`.cms_vertical as product_cms_vertical,
pd_wsr.`data`.seller_id as product_seller_id,
pd_wsr.`data`.shipping_weight as product_weight,
pd_wsr.`data`. listing_id as product_listing_id,
IF (pd_wsr.`data`.seller_id not in('wsr','fki'),1,0) as is_non_wsr_fbf_shipment,
--new columns addition
lookupkey('warehouse_id',s_wsr.`data`.warehouse_id) as shipment_warehouse_dim_key,
s_wsr.`data`.ekart_dispatch_by_date as  shipment_ekart_dispatch_by_date, 
lookup_date(s_wsr.`data`.ekart_dispatch_by_date) as shipment_ekart_dispatch_by_date_key ,
lookup_time(s_wsr.`data`.ekart_dispatch_by_date) as shipment_ekart_dispatch_by_time_key ,
s_wsr.`data`.entity_type as  shipment_entity_type,
s_wsr.`data`.ff_dispatch_by_date as shipment_ff_dispatch_by_date,
lookup_date(s_wsr.`data`.ff_dispatch_by_date) as shipment_ff_dispatch_by_date_key ,
lookup_time(s_wsr.`data`.ff_dispatch_by_date) as shipment_ff_dispatch_by_time_key ,
if( s_wsr.`data`.reshipment=TRUE,1,0) as shipment_is_reshipment,
si_wsr.`data`.entity_type as shipment_item_entity_type,
si_wsr.`data`.on_hold_reason as shipment_item_on_hold_reason,
si_wsr.`data`.polymorphic_id as shipment_item_polymorphic_id,
si_wsr.`data`.polymorphic_type as shipment_item_polymorphic_type,
si_wsr.`data`.quantity_lost_during_dispatch as shipment_item_quantity_lost_during_dispatch,
si_wsr.`data`.quantity_packed as shipment_item_quantity_packed,
si_wsr.`data`.quantity_received as shipment_item_quantity_received,
b_wsr.`data`.ideal_packing_box_id as box_ideal_packing_box_id,
b_wsr.`data`.bag_serial_number as box_bag_serial_number,
b_wsr.`data`.entity_type as box_entity_type,
if(pd_wsr.`data`.cms_vertical in ('mobile','tablet'),1,0) as is_mobile_tablet_category,
if(b_wsr.`data`.suggested_box_id is not null,1,0) as is_box_suggested,
if(b_wsr.`data`.actual_box_id is not null,1,0) as is_box_used,
--new event columns
from_unixtime(cast(pse_wsr.eventtime/1000 as bigint)) as shipment_packed_timestamp,
lookup_date(from_unixtime(cast(pse_wsr.eventtime/1000 as bigint))) as shipment_packed_date_key,
lookup_time(from_unixtime(cast(pse_wsr.eventtime/1000 as bigint))) as shipment_packed_time_key,
pse_wsr.done_by as shipment_packed_by,
--packing box new columns
upb_wsr.`data`.name as packing_box_used_internal_name,
spb_wsr.`data`.name as packing_box_suggested_internal_name,
NULL as is_tampered,
--IF(si_wsr.`data`.is_tampered=TRUE,1,0) as is_tampered
--s_wsr.`data`.is_tampered as  is_tampered

-- new box columns to be added 
if(b_wsr.`data`.outer_used_packing_box_id is not null,b_wsr.`data`.outer_used_packing_box_id,b_wsr.`data`.actual_box_id) as box_outer_used_packing_box_id,
if(b_wsr.`data`.suggested_outer_packing_box_id is not null,b_wsr.`data`.suggested_outer_packing_box_id,b_wsr.`data`.suggested_box_id) as box_suggested_outer_packing_box_id,
--new packing box columns to be added
if(b_wsr.`data`.outer_used_packing_box_id is not null,uopb_wsr.`data`.display_name,upb_wsr.`data`.display_name) as packing_box_used_outer_name,
if(b_wsr.`data`.outer_used_packing_box_id is not null,uopb_wsr.`data`.length,upb_wsr.`data`.length) as packing_box_used_outer_length,
if(b_wsr.`data`.outer_used_packing_box_id is not null,uopb_wsr.`data`.breadth,upb_wsr.`data`.breadth) as packing_box_used_outer_breadth,
if(b_wsr.`data`.outer_used_packing_box_id is not null,uopb_wsr.`data`.height,upb_wsr.`data`.height) as packing_box_used_outer_height,
if(b_wsr.`data`.outer_used_packing_box_id is not null,IF(uopb_wsr.`data`.is_active=TRUE,1,0),if(upb_wsr.`data`.is_active=TRUE,1,0)) as packing_box_used_outer_is_active,
if(b_wsr.`data`.outer_used_packing_box_id is not null,uopb_wsr.`data`.packing_bucket,upb_wsr.`data`.packing_bucket) as packing_box_used_outer_bucket,
if(b_wsr.`data`.suggested_outer_packing_box_id is not null,sopb_wsr.`data`.display_name,spb_wsr.`data`.display_name) as packing_box_suggested_outer_name,
if(b_wsr.`data`.suggested_outer_packing_box_id is not null,sopb_wsr.`data`.length,spb_wsr.`data`.length) as packing_box_suggested_outer_length,
if(b_wsr.`data`.suggested_outer_packing_box_id is not null,sopb_wsr.`data`.breadth,spb_wsr.`data`.breadth) as packing_box_suggested_outer_breadth,
if(b_wsr.`data`.suggested_outer_packing_box_id is not null,sopb_wsr.`data`.height,spb_wsr.`data`.height) as packing_box_suggested_outer_height,
if(b_wsr.`data`.suggested_outer_packing_box_id is not null,IF(sopb_wsr.`data`.is_active=TRUE,1,0),IF(spb_wsr.`data`.is_active=TRUE,1,0)) as packing_box_suggested_outer_is_active,
if(b_wsr.`data`.suggested_outer_packing_box_id is not null,sopb_wsr.`data`.packing_bucket,spb_wsr.`data`.packing_bucket) as packing_box_suggested_outer_bucket,
--new columns for internal name
if(b_wsr.`data`.outer_used_packing_box_id is not null,uopb_wsr.`data`.name,upb_wsr.`data`.name) as packing_box_used_outer_internal_name,
if(b_wsr.`data`.suggested_outer_packing_box_id is not null,sopb_wsr.`data`.name,spb_wsr.`data`.name) as packing_box_suggested_outer_internal_name,

si_wsr.`data`.quantity as shipment_item_quantity_original_table,
bi_wsr.data.box_item_id as box_item_id
from bigfoot_snapshot.dart_wsr_scp_warehouse_shipment_item_5_view si_wsr  LEFT OUTER JOIN
bigfoot_snapshot.dart_wsr_scp_warehouse_shipment_5_view s_wsr ON (si_wsr.data.shipment_id = s_wsr.data.id 
and regexp_replace(si_wsr.data.entity_type,'_item','') = s_wsr.data.entity_type )
LEFT OUTER JOIN (select eventid,eventtime, `data`.done_by from bigfoot_journal.dart_wsr_scp_warehouse_shipment_event_1
where data.status = 'rto_shipped' and `data`.done_by is not null) rse_wsr 
ON concat(s_wsr.entityid,'_rto_shipped') = rse_wsr.eventid 
LEFT OUTER JOIN (select eventid,eventtime, `data`.done_by from bigfoot_journal.dart_wsr_scp_warehouse_shipment_event_1
where data.status = 'shipped' and `data`.done_by is not null) rvpse_wsr 
ON concat(s_wsr.entityid,'_shipped') = rvpse_wsr.eventid 
LEFT OUTER JOIN (select eventid,eventtime, `data`.done_by from bigfoot_journal.dart_wsr_scp_warehouse_shipment_event_1 
where ((data.status = 'dispatched' and `data`.done_by is not null and data.type='shipment') 
or (data.status = 'dispatched' and data.type='consignment'))) dse_wsr 
ON concat(s_wsr.entityid,'_dispatched') = dse_wsr.eventid 
LEFT OUTER JOIN (select eventid,eventtime, `data`.done_by from bigfoot_journal.dart_wsr_scp_warehouse_shipment_event_1 
where data.status = 'packed' and `data`.done_by is not null) pse_wsr
ON concat(s_wsr.entityid,'_packed') = pse_wsr.eventid
LEFT OUTER JOIN bigfoot_snapshot.dart_wsr_scp_warehouse_box_item_1_view bi_wsr on (si_wsr.data.id=bi_wsr.data.shipment_item_id)
LEFT OUTER JOIN bigfoot_snapshot.dart_wsr_scp_warehouse_box_1_view b_wsr ON (bi_wsr.data.box_id = b_wsr.data.box_id)
LEFT OUTER JOIN bigfoot_snapshot.dart_wsr_scp_warehouse_packing_box_3_view_total spb_wsr
ON b_wsr.data.suggested_box_id = spb_wsr.entityid
LEFT OUTER JOIN bigfoot_snapshot.dart_wsr_scp_warehouse_packing_box_3_view_total upb_wsr
ON b_wsr.data.actual_box_id = upb_wsr.entityid
--new joins for outer packing box
LEFT OUTER JOIN bigfoot_snapshot.dart_wsr_scp_warehouse_packing_box_3_view_total uopb_wsr
ON b_wsr.data.outer_used_packing_box_id = uopb_wsr.entityid
LEFT OUTER JOIN bigfoot_snapshot.dart_wsr_scp_warehouse_packing_box_3_view_total sopb_wsr
ON b_wsr.data.suggested_outer_packing_box_id = sopb_wsr.entityid
LEFT OUTER JOIN bigfoot_snapshot.dart_wsr_scp_warehouse_product_detail_3_view_total pd_wsr ON
si_wsr.data.product_id = pd_wsr.entityid
where si_wsr.`data`.entity_type is not null

union all

select 'fki' as warehouse_company,
si_fki.data.id as shipment_item_id,
IF(si_fki.`data`.is_excess=TRUE,1,0) as shipment_item_is_excess,
si_fki.`data`.on_hold_reason_id as shipment_item_on_hold_reason_id,
si_fki.`data`.order_item_id as shipment_item_order_item_id,
si_fki.`data`.product_id as shipment_item_product_id,
lookupkey('product_detail_product_id',concat(si_fki.`data`.product_id,'fki')) as shipment_item_product_key,
bi_fki.data.quantity as shipment_item_quantity,
si_fki.`data`.status as shipment_item_status,
si_fki.`data`.updated_at as shipment_item_updated_timestamp,
lookup_date(si_fki.`data`.updated_at) as shipment_item_updated_date_key,
lookup_time(si_fki.`data`.updated_at) as shipment_item_updated_time_key,
IF(lower(si_fki.`data`.status)='cancelled',s_fki.`data`.updated_at,null) as shipment_item_cancelled_timestamp,
lookup_date(IF(lower(si_fki.`data`.status)='cancelled',s_fki.`data`.updated_at,null)) as shipment_item_cancelled_date_key,
lookup_time(IF(lower(si_fki.`data`.status)='cancelled',s_fki.`data`.updated_at,null)) as shipment_item_cancelled_time_key,
--shipment 
s_fki.data.id as shipment_id,
s_fki.`data`.courier_name as shipment_courier_name,
s_fki.`data`.created_at as shipment_created_timestamp,
lookup_date(s_fki.`data`.created_at) as shipment_created_date_key,
lookup_time(s_fki.`data`.created_at) as shipment_created_time_key,
s_fki.`data`.destination_type as shipment_destination_type,
s_fki.`data`.display_id as shipment_display_id,
s_fki.`data`.warehouse_id as shipment_warehouse_id,
if(s_fki.`data`.warehouse_id like '%0_L', 1,0) as shipment_is_large_warehouse,
(case when s_fki.`data`.warehouse_initiated_hold = TRUE then 'warehouse_initiated_hold' else NULL end) as shipment_hold_initiated_by,
s_fki.`data`.original_shipment_id as shipment_original_shipment_id,
s_fki.`data`.qc_done_at as shipment_qc_done_timestamp,
lookup_date(s_fki.`data`.qc_done_at) as shipment_qc_done_date_key,
lookup_time(s_fki.`data`.qc_done_at) as shipment_qc_done_time_key,
s_fki.`data`.qc_done_by as shipment_qc_done_by,
s_fki.`data`.return_warehouse_id as shipment_return_warehouse_id,
s_fki.`data`.ship_method as shipment_ship_method,
s_fki.`data`.status as shipment_status,
s_fki.`data`.updated_at as shipment_updated_timestamp,
lookup_date(s_fki.`data`.updated_at) as shipment_updated_date_key,
lookup_time(s_fki.`data`.updated_at) as shipment_updated_time_key,
IF(s_fki.`data`.status='cancelled',si_fki.`data`.updated_at,null) as shipment_cancelled_timestamp,
lookup_date(IF(s_fki.`data`.status='cancelled',si_fki.`data`.updated_at,null)) as shipment_cancelled_date_key,
lookup_time(IF(s_fki.`data`.status='cancelled',si_fki.`data`.updated_at,null)) as shipment_cancelled_time_key,
IF(s_fki.`data`.status='returned',si_fki.`data`.updated_at,null) as shipment_rto_received_timestamp,
lookup_date(IF(s_fki.`data`.status='returned',si_fki.`data`.updated_at,null)) as shipment_rto_received_date_key,
lookup_time(IF(s_fki.`data`.status='returned',si_fki.`data`.updated_at,null)) as shipment_rto_received_time_key,
IF(s_fki.`data`.status='received',si_fki.`data`.updated_at,null) as shipment_rvp_received_timestamp,
lookup_date(IF(s_fki.`data`.status='received',si_fki.`data`.updated_at,null)) as shipment_rvp_received_date_key,
lookup_time(IF(s_fki.`data`.status='received',si_fki.`data`.updated_at,null)) as shipment_rvp_received_time_key,
--shipment event
IF(s_fki.`data`.destination_type = 'customer',from_unixtime(cast(rse_fki.eventtime/1000 as bigint)),null) as shipment_rto_mh_scan_timestamp,
lookup_date(IF(s_fki.`data`.destination_type = 'customer',from_unixtime(cast(rse_fki.eventtime/1000 as bigint)),null)) as shipment_rto_mh_scan_date_key,
lookup_time(IF(s_fki.`data`.destination_type = 'customer',from_unixtime(cast(rse_fki.eventtime/1000 as bigint)),null)) as shipment_rto_mh_scan_time_key,
IF(s_fki.`data`.destination_type = 'warehouse',from_unixtime(cast(rvpse_fki.eventtime/1000 as bigint)),null) as shipment_rvp_mh_scan_timestamp, 
lookup_date(IF(s_fki.`data`.destination_type = 'warehouse',from_unixtime(cast(rvpse_fki.eventtime/1000 as bigint)),null)) as shipment_rvp_mh_scan_date_key,
lookup_time(IF(s_fki.`data`.destination_type = 'warehouse',from_unixtime(cast(rvpse_fki.eventtime/1000 as bigint)),null)) as shipment_rvp_mh_scan_time_key, 
from_unixtime(cast(dse_fki.eventtime/1000 as bigint)) as shipment_dispatched_timestamp,
lookup_date(from_unixtime(cast(dse_fki.eventtime/1000 as bigint))) as shipment_dispatched_date_key,
lookup_time(from_unixtime(cast(dse_fki.eventtime/1000 as bigint))) as shipment_dispatched_time_key,
dse_fki.done_by as shipment_dispatched_by,
IF(s_fki.`data`.destination_type = 'customer',rse_fki.done_by,rvpse_fki.done_by) as shipment_return_mh_scan_done_by,
-- box
b_fki.`data`.actual_box_id as box_used_packing_box_id,
b_fki.`data`.actual_weight as box_actual_weight,
b_fki.`data`.suggested_box_id as box_suggested_packing_box_id,
b_fki.`data`.tracking_id as box_tracking_id,
b_fki.`data`.shipment_weight as box_shipment_calculated_weight,
-- packing box
upb_fki.`data`.display_name as packing_box_used_name,
upb_fki.`data`.length as packing_box_used_length,
upb_fki.`data`.breadth as packing_box_used_breadth,
upb_fki.`data`.height as packing_box_used_height,
if(upb_fki.`data`.is_active=TRUE,1,0) as packing_box_used_is_active,
upb_fki.`data`.packing_bucket as packing_box_used_bucket,
spb_fki.`data`.display_name as packing_box_suggested_name,
spb_fki.`data`.length as packing_box_suggested_length,
spb_fki.`data`.breadth as packing_box_suggested_breadth,
spb_fki.`data`.height as packing_box_suggested_height,
IF(spb_fki.`data`.is_active=TRUE,1,0) as packing_box_suggested_is_active,
spb_fki.`data`.packing_bucket as packing_box_suggested_bucket,
--product details
pd_fki.`data`.length as product_length,
pd_fki.`data`.breadth as product_breadth,
pd_fki.`data`.height as product_height,
pd_fki.`data`.cms_vertical as product_cms_vertical,
pd_fki.`data`.seller_id as product_seller_id,
pd_fki.`data`.shipping_weight as product_weight,
pd_fki.`data`. listing_id as product_listing_id,
IF (pd_fki.`data`.seller_id not in('wsr','fki'),1,0) as is_non_wsr_fbf_shipment,
--new columns addition
lookupkey('warehouse_id',s_fki.`data`.warehouse_id) as shipment_warehouse_dim_key,
s_fki.`data`.ekart_dispatch_by_date as  shipment_ekart_dispatch_by_date, 
lookup_date(s_fki.`data`.ekart_dispatch_by_date) as shipment_ekart_dispatch_by_date_key ,
lookup_time(s_fki.`data`.ekart_dispatch_by_date) as shipment_ekart_dispatch_by_time_key ,
s_fki.`data`.entity_type as  shipment_entity_type,
s_fki.`data`.ff_dispatch_by_date as shipment_ff_dispatch_by_date,
lookup_date(s_fki.`data`.ff_dispatch_by_date) as shipment_ff_dispatch_by_date_key ,
lookup_time(s_fki.`data`.ff_dispatch_by_date) as shipment_ff_dispatch_by_time_key ,
if( s_fki.`data`.reshipment=TRUE,1,0) as shipment_is_reshipment,
si_fki.`data`.entity_type as shipment_item_entity_type,
si_fki.`data`.on_hold_reason as shipment_item_on_hold_reason,
si_fki.`data`.polymorphic_id as shipment_item_polymorphic_id,
si_fki.`data`.polymorphic_type as shipment_item_polymorphic_type,
si_fki.`data`.quantity_lost_during_dispatch as shipment_item_quantity_lost_during_dispatch,
si_fki.`data`.quantity_packed as shipment_item_quantity_packed,
si_fki.`data`.quantity_received as shipment_item_quantity_received,
b_fki.`data`.ideal_packing_box_id as box_ideal_packing_box_id,
b_fki.`data`.bag_serial_number as box_bag_serial_number,
b_fki.`data`.entity_type as box_entity_type,
if(pd_fki.`data`.cms_vertical in ('mobile','tablet'),1,0) as is_mobile_tablet_category,
if(b_fki.`data`.suggested_box_id is not null,1,0) as is_box_suggested,
if(b_fki.`data`.actual_box_id is not null,1,0) as is_box_used,
--new event columns
from_unixtime(cast(pse_fki.eventtime/1000 as bigint)) as shipment_packed_timestamp,
lookup_date(from_unixtime(cast(pse_fki.eventtime/1000 as bigint))) as shipment_packed_date_key,
lookup_time(from_unixtime(cast(pse_fki.eventtime/1000 as bigint))) as shipment_packed_time_key,
pse_fki.done_by as shipment_packed_by,
--packing box new columns
upb_fki.`data`.name as packing_box_used_internal_name,
spb_fki.`data`.name as packing_box_suggested_internal_name,
IF(s_fki.`data`.is_tampered=TRUE,1,0) as is_tampered,
--s_fki.`data`.is_tampered as  is_tampered
-- new box columns to be added 
if(b_fki.`data`.outer_used_packing_box_id is not null,b_fki.`data`.outer_used_packing_box_id,b_fki.`data`.actual_box_id) as box_outer_used_packing_box_id,
if(b_fki.`data`.suggested_outer_packing_box_id is not null,b_fki.`data`.suggested_outer_packing_box_id,b_fki.`data`.suggested_box_id) as box_suggested_outer_packing_box_id,
--new packing box columns to be added
if(b_fki.`data`.outer_used_packing_box_id is not null,uopb_fki.`data`.display_name,upb_fki.`data`.display_name) as packing_box_used_outer_name,
if(b_fki.`data`.outer_used_packing_box_id is not null,uopb_fki.`data`.length,upb_fki.`data`.length) as packing_box_used_outer_length,
if(b_fki.`data`.outer_used_packing_box_id is not null,uopb_fki.`data`.breadth,upb_fki.`data`.breadth) as packing_box_used_outer_breadth,
if(b_fki.`data`.outer_used_packing_box_id is not null,uopb_fki.`data`.height,upb_fki.`data`.height) as packing_box_used_outer_height,
if(b_fki.`data`.outer_used_packing_box_id is not null,IF(uopb_fki.`data`.is_active=TRUE,1,0),if(upb_fki.`data`.is_active=TRUE,1,0)) as packing_box_used_outer_is_active,
if(b_fki.`data`.outer_used_packing_box_id is not null,uopb_fki.`data`.packing_bucket,upb_fki.`data`.packing_bucket) as packing_box_used_outer_bucket,
if(b_fki.`data`.suggested_outer_packing_box_id is not null,sopb_fki.`data`.display_name,spb_fki.`data`.display_name) as packing_box_suggested_outer_name,
if(b_fki.`data`.suggested_outer_packing_box_id is not null,sopb_fki.`data`.length,spb_fki.`data`.length) as packing_box_suggested_outer_length,
if(b_fki.`data`.suggested_outer_packing_box_id is not null,sopb_fki.`data`.breadth,spb_fki.`data`.breadth) as packing_box_suggested_outer_breadth,
if(b_fki.`data`.suggested_outer_packing_box_id is not null,sopb_fki.`data`.height,spb_fki.`data`.height) as packing_box_suggested_outer_height,
if(b_fki.`data`.suggested_outer_packing_box_id is not null,IF(sopb_fki.`data`.is_active=TRUE,1,0),IF(spb_fki.`data`.is_active=TRUE,1,0)) as packing_box_suggested_outer_is_active,
if(b_fki.`data`.suggested_outer_packing_box_id is not null,sopb_fki.`data`.packing_bucket,spb_fki.`data`.packing_bucket) as packing_box_suggested_outer_bucket,
--new columns for internal name
if(b_fki.`data`.outer_used_packing_box_id is not null,uopb_fki.`data`.name,upb_fki.`data`.name) as packing_box_used_outer_internal_name,
if(b_fki.`data`.suggested_outer_packing_box_id is not null,sopb_fki.`data`.name,spb_fki.`data`.name) as packing_box_suggested_outer_internal_name,
si_fki.`data`.quantity as shipment_item_quantity_original_table,
bi_fki.data.box_item_id as box_item_id
from bigfoot_snapshot.dart_fki_scp_warehouse_shipment_item_5_view si_fki  LEFT OUTER JOIN
bigfoot_snapshot.dart_fki_scp_warehouse_shipment_5_view s_fki ON (si_fki.data.shipment_id = s_fki.data.id 
and regexp_replace(si_fki.data.entity_type,'_item','') = s_fki.data.entity_type )
LEFT OUTER JOIN (select eventid,eventtime, `data`.done_by from bigfoot_journal.dart_fki_scp_warehouse_shipment_event_1
where data.status = 'rto_shipped' and `data`.done_by is not null) rse_fki 
ON concat(s_fki.entityid,'_rto_shipped') = rse_fki.eventid 
LEFT OUTER JOIN (select eventid,eventtime, `data`.done_by from bigfoot_journal.dart_fki_scp_warehouse_shipment_event_1
where data.status = 'shipped' and `data`.done_by is not null) rvpse_fki 
ON concat(s_fki.entityid,'_shipped') = rvpse_fki.eventid 
LEFT OUTER JOIN (select eventid,eventtime, `data`.done_by from bigfoot_journal.dart_fki_scp_warehouse_shipment_event_1
where ((data.status = 'dispatched' and `data`.done_by is not null and data.type='shipment') 
or (data.status = 'dispatched' and data.type='consignment'))) dse_fki
ON concat(s_fki.entityid,'_dispatched') = dse_fki.eventid 
LEFT OUTER JOIN (select eventid,eventtime, `data`.done_by from bigfoot_journal.dart_fki_scp_warehouse_shipment_event_1
where data.status = 'packed' and `data`.done_by is not null) pse_fki
ON concat(s_fki.entityid,'_packed') = pse_fki.eventid 
LEFT OUTER JOIN bigfoot_snapshot.dart_fki_scp_warehouse_box_item_1_view bi_fki on (si_fki.data.id=bi_fki.data.shipment_item_id)
LEFT OUTER JOIN bigfoot_snapshot.dart_fki_scp_warehouse_box_1_view b_fki ON (bi_fki.data.box_id = b_fki.data.box_id)
LEFT OUTER JOIN bigfoot_snapshot.dart_fki_scp_warehouse_packing_box_2_view_total spb_fki
ON b_fki.data.suggested_box_id = spb_fki.entityid
LEFT OUTER JOIN bigfoot_snapshot.dart_fki_scp_warehouse_packing_box_2_view_total upb_fki
ON b_fki.data.actual_box_id = upb_fki.entityid
--new joins for outer packing box
LEFT OUTER JOIN bigfoot_snapshot.dart_fki_scp_warehouse_packing_box_2_view_total uopb_fki
ON b_fki.`data`.outer_used_packing_box_id = uopb_fki.entityid
LEFT OUTER JOIN bigfoot_snapshot.dart_fki_scp_warehouse_packing_box_2_view_total sopb_fki
ON b_fki.data.suggested_outer_packing_box_id = sopb_fki.entityid
LEFT OUTER JOIN bigfoot_snapshot.dart_fki_scp_warehouse_product_detail_2_view_total pd_fki ON
si_fki.data.product_id = pd_fki.entityid
where si_fki.`data`.entity_type is not null
) final_table;
