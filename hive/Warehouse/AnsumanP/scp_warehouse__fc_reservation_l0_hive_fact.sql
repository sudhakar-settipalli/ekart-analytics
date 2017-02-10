INSERT OVERWRITE TABLE fc_reservation_l0_hive_fact
select 'wsr' as warehouse_company,
r.data.reservation_id as reservation_id,
r.data.created_at as reservation_created_timestamp,
lookup_date(r.data.created_at) as reservation_created_date_key,
lookup_time(r.data.created_at) as reservation_created_time_key,
r.data.fulfil_reference_id as reservation_fulfill_reference_id,
r.data.inventory_item_id as reservation_inventory_item_id,
r.data.order_item_id as reservation_order_item_id,
r.data.product_id as reservation_product_id,
lookupkey('product_detail_product_id',concat(r.data.product_id,'wsr')) as reservation_product_key,
r.data.quantity as reservation_quantity,
r.data.source_type as reservation_inv_source_type,
r.data.status as reservation_status,
r.data.updated_at as reservation_updated_timestamp,
lookup_date(r.data.updated_at) as reservation_updated_date_key,
lookup_time(r.data.updated_at) as reservation_updated_time_key,
r.data.warehouse_id as reservataion_warehouse_id,
r.data.wid as reservation_wid,
if(r.data.warehouse_id like '%0_L', 1,0) as reservation_is_large_warehouse,
if(r.data.status = 'cancelled',r.data.updated_at,null) as reservation_cancelled_timestamp,
lookup_date(if(r.data.status = 'cancelled',r.data.updated_at,null)) as reservation_cancelled_date_key,
lookup_time(if(r.data.status = 'cancelled',r.data.updated_at,null)) as reservation_cancelled_time_key,
--picklist_item
pli.data.id as picklist_item_id,
pli.data.actual_location as picklist_item_actual_location,
pli.data.actual_quantity as picklist_item_actual_quantity,
pli.data.lost_quantity as picklist_item_lost_quantity,
pli.data.shipment_item_id as picklist_item_shipment_item_id,
pli.data.status as picklist_item_status,
pli.data.suggested_location as picklist_item_suggested_location,
pli.data.suggested_quantity as picklist_item_suggested_quantity,
if(pli.data.status = 'picked', pli.data.updated_at,null) as picklist_item_picked_timestamp,
lookup_date(if(pli.data.status = 'picked', pli.data.updated_at,null)) as picklist_item_picked_date_key,
lookup_time(if(pli.data.status = 'picked', pli.data.updated_at,null)) as picklist_item_picked_time_key,
if(pli.data.status = 'cancelled', pli.data.updated_at,null) as picklist_item_cancelled_timestamp,
lookup_date(if(pli.data.status = 'cancelled', pli.data.updated_at,null)) as picklist_item_cancelled_date_key,
lookup_time(if(pli.data.status = 'cancelled', pli.data.updated_at,null)) as picklist_item_cancelled_time_key,
--picklist
pl.data.id as picklist_id,
pl.data.assigned_to as picklist_assigned_to,
pl.data.completed_by as picklist_compledted_by,
pl.data.created_at as picklist_created_timestamp,
lookup_date(pl.data.created_at) as picklist_created_date_key,
lookup_time(pl.data.created_at) as picklist_created_time_key,
pl.data.created_by as picklist_created_by,
pl.data.dispatch_by_date as picklist_dispatch_by_date,
pl.data.display_id as picklist_display_id,
if(pl.data.ibl_prn_printed=TRUE,1,0) as picklist_is_ibl_prn_printed,
pl.data.picklist_status as picklist_status,
pl.data.picklist_type as picklist_picking_zone,
pl.data.updated_at as picklist_updated_timestamp,
lookup_date(pl.data.updated_at) as picklist_updated_date_key,
lookup_time(pl.data.updated_at) as picklist_updated_time_key,
if(pl.data.picklist_type like 'cross%','cross_zone',pl.data.location_type) as picklist_location_type,
---reservationevent
re.reservation_max_in_ship_group_timestamp as reservation_max_in_ship_group_timestamp,
lookup_date(re.reservation_max_in_ship_group_timestamp) as reservation_max_in_ship_group_date_key,
lookup_time(re.reservation_max_in_ship_group_timestamp) as reservation_max_in_ship_group_time_key,
1 as extra,
--new column addition
lookupkey('warehouse_id',r.data.warehouse_id) as reservation_warehouse_dim_key,
r.data.ekart_dispatch_by_date as reservation_ekart_dispatch_by_date,
lookup_date(r.data.ekart_dispatch_by_date) as reservation_ekart_dispatch_by_date_key ,
lookup_time(r.data.ekart_dispatch_by_date) as reservation_ekart_dispatch_by_time_key ,
r.data.ff_dispatch_by_date as reservation_ff_dispatch_by_date,
lookup_date(r.data.ff_dispatch_by_date) as reservation_ff_dispatch_by_date_key ,
lookup_time(r.data.ff_dispatch_by_date) as reservation_ff_dispatch_by_time_key ,
r.data.outbound_request_id as reservation_outbound_request_id,
r.data.outbound_type as reservation_outbound_type,
r.data.ship_method as reservation_ship_method,
r.data.shipment_item_id as reservation_shipment_item_id,
r.data.source_storage_location_id as reservation_source_storage_location_id,
pli.data.entity_type as picklist_item_entity_type,
pli.data.shipment_id as picklist_item_shipment_id,
pl.data.entity_type as picklist_entity_type
from bigfoot_snapshot.dart_wsr_scp_warehouse_reservation_5_view as r LEFT OUTER JOIN
bigfoot_snapshot.dart_wsr_scp_warehouse_picklist_item_3_view as pli ON (r.data.reservation_id = pli.data.reservation_id 
and (r.data.outbound_type=if(pli.data.entity_type='picklist_item','customer_reservation',
if(pli.data.entity_type in ('TTL','wh_picklist_item'),'non_customer_reservation','undefined'))))
LEFT OUTER JOIN
bigfoot_snapshot.dart_wsr_scp_warehouse_picklist_5_view as pl ON (pl.data.id = pli.data.picklist_id
and (pl.data.entity_type=if(pli.data.entity_type='picklist_item','picklist',
if(pli.data.entity_type='TTL','transfer_task_list',if(pli.data.entity_type='wh_picklist_item','wh_picklist','undefined')))))
LEFT OUTER JOIN
(select eventid, 
max(from_unixtime(cast(eventtime/1000 as bigint))) as reservation_max_in_ship_group_timestamp
from bigfoot_journal.dart_wsr_scp_warehouse_reservation_event_1 where day<#90#DAY# and data.status='in_ship_group'
group by eventid )  re ON r.entityid = re.eventid
where r.data.outbound_type is not null

union all

select 'fki' as warehouse_company,
r2.data.reservation_id as reservation_id,
r2.data.created_at as reservation_created_timestamp,
lookup_date(r2.data.created_at) as reservation_created_date_key,
lookup_time(r2.data.created_at) as reservation_created_time_key,
r2.data.fulfil_reference_id as reservation_fulfill_reference_id,
r2.data.inventory_item_id as reservation_inventory_item_id,
r2.data.order_item_id as reservation_order_item_id,
r2.data.product_id as reservation_product_id,
lookupkey('product_detail_product_id',concat(r2.data.product_id,'fki')) as reservation_product_key,
r2.data.quantity as reservation_quantity,
r2.data.source_type as reservation_inv_source_type,
r2.data.status as reservation_status,
r2.data.updated_at as reservation_updated_timestamp,
lookup_date(r2.data.updated_at) as reservation_updated_date_key,
lookup_time(r2.data.updated_at) as reservation_updated_time_key,
r2.data.warehouse_id as reservataion_warehouse_id,
r2.data.wid as reservation_wid,
if(r2.data.warehouse_id like '%0_L', 1,0) as reservation_is_large_warehouse,
if(r2.data.status = 'cancelled',r2.data.updated_at,null) as reservation_cancelled_timestamp,
lookup_date(if(r2.data.status = 'cancelled',r2.data.updated_at,null)) as reservation_cancelled_date_key,
lookup_time(if(r2.data.status = 'cancelled',r2.data.updated_at,null)) as reservation_cancelled_time_key,
--picklist_item
pli2.data.id as picklist_item_id,
pli2.data.actual_location as picklist_item_actual_location,
pli2.data.actual_quantity as picklist_item_actual_quantity,
pli2.data.lost_quantity as picklist_item_lost_quantity,
pli2.data.shipment_item_id as picklist_item_shipment_item_id,
pli2.data.status as picklist_item_status,
pli2.data.suggested_location as picklist_item_suggested_location,
pli2.data.suggested_quantity as picklist_item_suggested_quantity,
if(pli2.data.status = 'picked', pli2.data.updated_at,null) as picklist_item_picked_timestamp,
lookup_date(if(pli2.data.status = 'picked', pli2.data.updated_at,null)) as picklist_item_picked_date_key,
lookup_time(if(pli2.data.status = 'picked', pli2.data.updated_at,null)) as picklist_item_picked_time_key,
if(pli2.data.status = 'cancelled', pli2.data.updated_at,null) as picklist_item_cancelled_timestamp,
lookup_date(if(pli2.data.status = 'cancelled', pli2.data.updated_at,null)) as picklist_item_cancelled_date_key,
lookup_time(if(pli2.data.status = 'cancelled', pli2.data.updated_at,null)) as picklist_item_cancelled_time_key,
--picklist
pl2.data.id as picklist_id,
pl2.data.assigned_to as picklist_assigned_to,
pl2.data.completed_by as picklist_compledted_by,
pl2.data.created_at as picklist_created_timestamp,
lookup_date(pl2.data.created_at) as picklist_created_date_key,
lookup_time(pl2.data.created_at) as picklist_created_time_key,
pl2.data.created_by as picklist_created_by,
pl2.data.dispatch_by_date as picklist_dispatch_by_date,
pl2.data.display_id as picklist_display_id,
if(pl2.data.ibl_prn_printed=TRUE,1,0) as picklist_is_ibl_prn_printed,
pl2.data.picklist_status as picklist_status,
pl2.data.picklist_type as picklist_picking_zone,
pl2.data.updated_at as picklist_updated_timestamp,
lookup_date(pl2.data.updated_at) as picklist_updated_date_key,
lookup_time(pl2.data.updated_at) as picklist_updated_time_key,
if(pl2.data.picklist_type like 'cross%','cross_zone',pl2.data.location_type) as picklist_location_type,
---reservationevent
re2.reservation_max_in_ship_group_timestamp as reservation_max_in_ship_group_timestamp,
lookup_date(re2.reservation_max_in_ship_group_timestamp) as reservation_max_in_ship_group_date_key,
lookup_time(re2.reservation_max_in_ship_group_timestamp) as reservation_max_in_ship_group_time_key,
1 as extra,
--new column addition
lookupkey('warehouse_id',r2.data.warehouse_id) as reservation_warehouse_dim_key,
r2.data.ekart_dispatch_by_date as reservation_ekart_dispatch_by_date,
lookup_date(r2.data.ekart_dispatch_by_date) as reservation_ekart_dispatch_by_date_key ,
lookup_time(r2.data.ekart_dispatch_by_date) as reservation_ekart_dispatch_by_time_key ,
r2.data.ff_dispatch_by_date as reservation_ff_dispatch_by_date,
lookup_date(r2.data.ff_dispatch_by_date) as reservation_ff_dispatch_by_date_key ,
lookup_time(r2.data.ff_dispatch_by_date) as reservation_ff_dispatch_by_time_key ,
r2.data.outbound_request_id as reservation_outbound_request_id,
r2.data.outbound_type as reservation_outbound_type,
r2.data.ship_method as reservation_ship_method,
r2.data.shipment_item_id as reservation_shipment_item_id,
r2.data.source_storage_location_id as reservation_source_storage_location_id,
pli2.data.entity_type as picklist_item_entity_type,
pli2.data.shipment_id as picklist_item_shipment_id,
pl2.data.entity_type as picklist_entity_type
from bigfoot_snapshot.dart_fki_scp_warehouse_reservation_6_view as r2 LEFT OUTER JOIN
bigfoot_snapshot.dart_fki_scp_warehouse_picklist_item_2_view as pli2 ON (r2.data.reservation_id = pli2.data.reservation_id
and (r2.data.outbound_type=if(pli2.data.entity_type='picklist_item','customer_reservation',
if(pli2.data.entity_type in ('TTL','wh_picklist_item'),'non_customer_reservation','undefined'))))
LEFT OUTER JOIN
bigfoot_snapshot.dart_fki_scp_warehouse_picklist_4_view as pl2 ON (pl2.data.id = pli2.data.picklist_id and (pl2.data.entity_type=if(pli2.data.entity_type='picklist_item','picklist',
if(pli2.data.entity_type='TTL','transfer_task_list',if(pli2.data.entity_type='wh_picklist_item','wh_picklist','undefined')))))
LEFT OUTER JOIN
(select eventid, 
max(from_unixtime(cast(eventtime/1000 as bigint))) as reservation_max_in_ship_group_timestamp
from bigfoot_journal.dart_fki_scp_warehouse_reservation_event_1 where day<#90#DAY# and data.status='in_ship_group'
group by eventid )  re2 ON r2.entityid = re2.eventid
where r2.data.outbound_type is not null
;
