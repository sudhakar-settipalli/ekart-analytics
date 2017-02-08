insert overwrite table la_fulfilment_fact 
select fiu.fulfill_item_unit_dispatch_actual_time,
fiu.fulfill_item_unit_ship_actual_time,
fiu.fulfill_item_unit_dispatch_expected_time,
fiu.fulfill_item_unit_ship_expected_time,
fiu.fulfillment_order_item_id,
fiu.fulfill_item_unit_id,
fiu.fulfill_item_unit_status,
fiu.shipment_merchant_reference_id,
fiu.fulfill_item_unit_updated_at,
fiu.fulfill_item_unit_region,
fiu.fulfill_item_unit_dispatch_actual_date_key,
fiu.fulfill_item_unit_ship_actual_date_key,
fiu.fulfill_item_unit_dispatch_expected_date_key,
fiu.fulfill_item_unit_order_date_key,
fiu.fulfill_item_unit_dispatch_actual_time_key,
fiu.fulfill_item_unit_ship_actual_time_key,
fiu.fulfill_item_unit_dispatch_expected_time_key,
fiu.fulfill_item_unit_order_time_key,
fiu.fulfill_item_unit_region_type,
fiu.fulfill_item_id,
fiu.fulfill_item_type,
fiu.fulfill_item_unit_deliver_after_time,
fiu.fulfill_item_unit_dispatch_after_time,
fiu.fulfill_item_unit_deliver_after_date_key,
fiu.fulfill_item_unit_deliver_after_time_key,
fiu.fulfill_item_unit_dispatch_after_date_key,
fiu.fulfill_item_unit_dispatch_after_time_key,
fiu.fulfill_item_unit_is_for_slotted_delivery,
fiu.fulfill_item_unit_reserved_status_b2c_actual_time,
fiu.fulfill_item_unit_reserved_status_expected_time,
fiu.fulfill_item_unit_reserved_status_b2c_actual_date_key,
fiu.fulfill_item_unit_reserved_status_b2c_actual_time_key,
fiu.fulfill_item_unit_reserved_status_expected_date_key,
fiu.fulfill_item_unit_reserved_status_expected_time_key,
fiu.fulfill_item_unit_delivered_status_expected_time,
fiu.fulfill_item_unit_delivered_status_expected_date_key,
fiu.fulfill_item_unit_delivered_status_expected_time_key,
fiu.fulfill_item_unit_delivered_status_id,
fiu.fulfill_item_unit_size,
fiu.fulfill_item_unit_reserved_status_b2b_actual_date_key,
fiu.fulfill_item_unit_reserved_status_b2b_actual_time_key,
fiu.fulfill_item_unit_reserved_status_b2b_actual_time,
fiu.fulfill_item_unit_deliver_actual_time,
fiu.fulfill_item_unit_deliver_actual_date_key,
fiu.fulfill_item_unit_deliver_actual_time_key,
if(fiu.fulfill_item_unit_is_for_slotted_delivery = 'NotSlotted',fiu_cpd.cpd_change_created_at,fiu_sc.slot_changed_created_at) as slot_changed_created_at,
if(fiu_cpd.cpd_change_sub_reason is not null,'ekl',
if(fiu.fulfill_item_unit_is_for_slotted_delivery = 'Slotted',
if(slot_changed_created_at is not null,
if(fiu_sc.slot_change_reason in ('EKLTRIGGERED/RESCHEDULED','CUSTOMERTRIGGERED/SLOT_CHANGE','CUSTOMERTRIGGERED/RESCHEDULED','SLOT_CHANGED'),
'cust',if(fiu_sc.slot_change_reason in ('SELLERTRIGGERED/SELLER_RESCHEDULED','FLIPKARTTRIGGERED/DISPATCH_DELAYED',
'FLIPKARTTRIGGERED/DELAYED_DISPATCH','EKLTRIGGERED/PENDING_IN_TRANSIT','EKLTRIGGERED/PENDING_IN_DELIVERY','DISPATCH_BREACH','SLOT_BREACHED','SLOT_IN_ALTERNATE_WH','SLOT_UNAVAILABLE'),
'ekl','Others')),'slot_unchanged'),'slot_unchanged')) as slot_changed_by,
if(fiu.fulfill_item_unit_is_for_slotted_delivery = 'NotSlotted',fiu_cpd.cpd_change_sub_reason,fiu_sc.slot_change_reason) as slot_change_reason,

'dummy' as customer_destination_pincode,
'dummy' as slot_booking_ref_id,
'dummy' as ff_dummy1,
'dummy2' as ff_dummy2
from 
(SELECT DISTINCT 
`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_actual_time as fulfill_item_unit_dispatch_actual_time,
`data`.fulfill_item_unit_shipped_status.fulfill_item_unit_shipped_status_actual_time as fulfill_item_unit_ship_actual_time,
`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_expected_time as fulfill_item_unit_dispatch_expected_time,
`data`.fulfill_item_unit_shipped_status.fulfill_item_unit_shipped_status_expected_time as fulfill_item_unit_ship_expected_time,
`data`.fulfill_item_unit_order_item_mapping.order_item_mapping_external_id as fulfillment_order_item_id,
`data`.fulfill_item_unit_id as fulfill_item_unit_id,
`data`.fulfill_item_unit_status as fulfill_item_unit_status,
`data`.fulfill_item_unit_ekl_shipment_mapping.ekl_shipment_mapping_external_id as shipment_merchant_reference_id,
`data`.fulfill_item_unit_updated_at as fulfill_item_unit_updated_at,
`data`.fulfill_item_unit_region as fulfill_item_unit_region,
lookup_date(`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_actual_time) as fulfill_item_unit_dispatch_actual_date_key,
lookup_date(`data`.fulfill_item_unit_shipped_status.fulfill_item_unit_shipped_status_actual_time) as fulfill_item_unit_ship_actual_date_key,
lookup_date(`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_expected_time) as fulfill_item_unit_dispatch_expected_date_key,
lookup_date(`data`.fulfill_item_unit_fulfill_item.fulfill_item_order_date) as fulfill_item_unit_order_date_key ,
lookup_time(`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_actual_time) as fulfill_item_unit_dispatch_actual_time_key,
lookup_time(`data`.fulfill_item_unit_shipped_status.fulfill_item_unit_shipped_status_actual_time) as fulfill_item_unit_ship_actual_time_key,
lookup_time(`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_expected_time) as fulfill_item_unit_dispatch_expected_time_key,
lookup_time(`data`.fulfill_item_unit_fulfill_item.fulfill_item_order_date) as fulfill_item_unit_order_time_key,
`data`.fulfill_item_unit_region_type as fulfill_item_unit_region_type,
`data`.fulfill_item_unit_fulfill_item.fulfill_item_id as fulfill_item_id,
`data`.fulfill_item_unit_fulfill_item.fulfill_item_type as fulfill_item_type,
`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_after_time as fulfill_item_unit_deliver_after_time,
`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_after_time as fulfill_item_unit_dispatch_after_time,
lookup_date(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_after_time) as fulfill_item_unit_deliver_after_date_key,
lookup_time(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_after_time) as fulfill_item_unit_deliver_after_time_key,
lookup_date(`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_after_time) as fulfill_item_unit_dispatch_after_date_key,
lookup_time(`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_after_time) as fulfill_item_unit_dispatch_after_time_key,
case when `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_after_time is null then 'NotSlotted' else 'Slotted' end  as fulfill_item_unit_is_for_slotted_delivery,
`data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_actual_time as fulfill_item_unit_reserved_status_b2c_actual_time,
`data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_expected_time as fulfill_item_unit_reserved_status_expected_time,
lookup_date(`data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_actual_time) as fulfill_item_unit_reserved_status_b2c_actual_date_key,
lookup_time(`data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_actual_time) as fulfill_item_unit_reserved_status_b2c_actual_time_key,
lookup_date(`data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_expected_time) as fulfill_item_unit_reserved_status_expected_date_key,
lookup_time(`data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_expected_time) as fulfill_item_unit_reserved_status_expected_time_key,
`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time as fulfill_item_unit_delivered_status_expected_time,
lookup_date(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time) as fulfill_item_unit_delivered_status_expected_date_key,
lookup_time(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time) as fulfill_item_unit_delivered_status_expected_time_key,
`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_id as fulfill_item_unit_delivered_status_id,
`data`.Fulfill_Item_Unit_Size as fulfill_item_unit_size,
lookup_date(`data`.fulfill_item_unit_reserved_in_b2b_status.fulfill_item_unit_reserved_in_b2b_status_actual_time) as fulfill_item_unit_reserved_status_b2b_actual_date_key,
lookup_time(`data`.fulfill_item_unit_reserved_in_b2b_status.fulfill_item_unit_reserved_in_b2b_status_actual_time) as fulfill_item_unit_reserved_status_b2b_actual_time_key,
`data`.fulfill_item_unit_reserved_in_b2b_status.fulfill_item_unit_reserved_in_b2b_status_actual_time as fulfill_item_unit_reserved_status_b2b_actual_time,
`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_actual_time AS fulfill_item_unit_deliver_actual_time,
lookup_date(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_actual_time) as fulfill_item_unit_deliver_actual_date_key,
lookup_time(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_actual_time) as fulfill_item_unit_deliver_actual_time_key
from bigfoot_snapshot.dart_fkint_scp_fulfillment_fulfill_item_unit_2_view_total  A_22_1 where `data`.Fulfill_Item_Unit_Size in ('Large')) fiu
left outer join 
(select fiu_status_id,
slot_changed_created_at,
Upper(slot_change_reason) as slot_change_reason,
row_desc from 
(select
`data`.fulfill_item_unit_status_id as fiu_status_id,
`data`.created_at as slot_changed_created_at,
LAST_VALUE(`data`.reason) OVER (PARTITION BY `data`.fulfill_item_unit_status_id ORDER BY `data`.created_at rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) AS slot_change_reason,
row_number() OVER (PARTITION BY `data`.fulfill_item_unit_status_id ORDER BY `data`.created_at DESC rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as row_desc
FROM bigfoot_journal.dart_fkint_scp_fulfillment_fulfill_item_unit_status_time_change_log_1) fiu_status_change 
where fiu_status_change.row_desc = '1') fiu_sc     
ON (fiu_sc.fiu_status_id = fiu.fulfill_item_unit_delivered_status_id)

left outer join 

(select
fiu_cpd_1.fulfill_item_unit_id,
fiu_cpd_1.reason as cpd_change_reason,
lower(fiu_cpd_1.sub_reason) as cpd_change_sub_reason,
fiu_cpd_1.created_at as cpd_change_created_at,
fiu_cpd_1.new_date as new_cpd_date
from
(select
`data`.fulfill_item_unit_id,`data`.reason,`data`.created_at,`data`.new_date,
LAST_VALUE(`data`.sub_reason) OVER (PARTITION BY `data`.fulfill_item_unit_id ORDER BY `data`.created_at rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) AS sub_reason,
row_number() OVER (PARTITION BY `data`.fulfill_item_unit_id ORDER BY `data`.created_at DESC rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as row_desc
from bigfoot_journal.dart_fkint_scp_fulfillment_fulfill_item_unit_change_in_oms_customer_promised_date_1
where `data`.sub_reason in ('update_sla','Dispatch_Breach','Dispatch_Delayed','Shipped_Promise_Change','Unhold','update_lpd','SELLER_RESCHEDULED','aggressive_assigning','item_not_found','reservation_failed','Do_Fulfill_Update','not_enough_inventory','reservation_failed') ) fiu_cpd_1
where fiu_cpd_1.row_desc=1 ) fiu_cpd
on fiu_cpd.fulfill_item_unit_id = fiu.fulfill_item_unit_id

where not(isnull(fiu.fulfill_item_unit_id)) and fiu.fulfill_item_unit_region_type <> 'seller';
