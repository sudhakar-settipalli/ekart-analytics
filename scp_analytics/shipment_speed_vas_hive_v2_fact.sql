INSERT OVERWRITE TABLE shipment_speed_vas_hive_v2_fact
select  
B.order_item_unit_tracking_id as order_vendor_tracking_id,
B.order_item_unit_shipment_id as order_merchant_reference_id,
min(B.order_item_approve_date_time) as order_item_approve_sla_date_time,
min(COALESCE(A.fulfill_item_unit_dispatch_expected_time, A.fulfill_item_unit_dispatch_by_old_datetime)) as order_item_dispatch_by_sla_date_time,
min(A.fulfill_item_unit_ship_expected_time) as order_item_ship_by_sla_date_time,
min(B.order_item_unit_init_promised_date_key) as order_item_promise_sla_date_key,
min(A.fulfill_item_unit_dispatch_after_time) as fulfill_item_rtd_after_sla_date_time,
max(A.fulfill_item_unit_dispatch_actual_time) as fulfill_item_rtd_actual_o2d_date_time,
max(A.fulfill_item_unit_dispatch_actual_time) as fulfill_item_dispatch_o2d_date_time,
max(A.fulfill_item_unit_ship_actual_time) as fulfill_item_ship_o2d_date_time,
max(A.fulfill_item_unit_deliver_actual_time) as fulfill_item_deliver_o2d_date_time,
max(A.fulfill_item_unit_reserve_actual_date_key) as fulfill_item_unit_reserve_actual_date_key,
max(A.fulfill_item_unit_reserve_actual_time_key) as fulfill_item_unit_reserve_actual_time_key,
min(A.fulfill_item_unit_dispatch_service_tier) as fulfill_item_unit_dispatch_service_tier
From bigfoot_external_neo.scp_fulfillment__fulfillment_unit_hive_fact A
left outer join 
(select
max(order_item_approve_date_time) as order_item_approve_date_time,
max(order_item_unit_final_promised_date_key) as order_item_unit_final_promised_date_key ,
max(order_item_unit_init_promised_date_key) as order_item_unit_init_promised_date_key,
order_item_id,
order_item_unit_tracking_id,
order_item_unit_shipment_id
from bigfoot_external_neo.scp_oms__order_item_unit_s1_fact
where order_item_unit_tracking_id <> '0'
group by
order_item_id,
order_item_unit_tracking_id,
order_item_unit_shipment_id
) B on A.order_item_id <=> B.order_item_id and A.shipment_merchant_reference_id <=> B.order_item_unit_shipment_id
group by 
B.order_item_unit_tracking_id,
B.order_item_unit_shipment_id;
