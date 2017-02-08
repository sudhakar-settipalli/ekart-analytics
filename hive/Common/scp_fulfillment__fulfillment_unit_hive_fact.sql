INSERT overwrite TABLE fulfillment_unit_hive_fact
SELECT distinct seller_id_key,
 product_id_key,
 fulfill_item_unit_dispatch_actual_date_key,
 fulfill_item_unit_dispatch_actual_time_key,
 fulfill_item_unit_dispatch_actual_time,
 fulfill_item_unit_ship_actual_time_key,
 fulfill_item_unit_ship_actual_date_key,
 fulfill_item_unit_ship_actual_time,
 fulfill_item_unit_deliver_actual_time_key,
 fulfill_item_unit_deliver_actual_date_key,
 fulfill_item_unit_deliver_actual_time,
 fulfill_item_unit_dispatch_expected_time,
 fulfill_item_unit_dispatch_expected_date_key,
 fulfill_item_unit_dispatch_expected_time_key,
 fulfill_item_unit_ship_expected_time,
 order_item_id,
 fulunit.fulfill_item_unit_id AS fulfill_item_unit_id,
 fulfill_item_unit_shipment_movement_type,
 fulfill_item_unit_status,
 fulfill_item_unit_reserve_actual_time,
 fulfill_item_unit_reserve_actual_time_key,
 fulfill_item_unit_reserve_actual_date_key,
 coalesce(fulfill_item_unit_promise_tier, 'regular') AS fulfill_item_unit_promise_tier,
 shipment_merchant_reference_id,
 fulfill_item_unit_destination_pincode_key,
 fulfill_item_unit_updated_at,
 fulfill_item_unit_region,
 fulfill_item_unit_deliver_expected_time,
 fulfill_item_unit_deliver_expected_time_key,
 fulfill_item_unit_deliver_expected_date_key,
 fulfill_item_unit_out_for_delivery_actual_time,
 fulfill_item_unit_is_jit,
 fulfill_item_unit_breach_cost,
 fulfill_item_unit_status_modified,
 fulfill_item_unit_region_type,
 fulfill_item_unit_dispatch_cutoff_breach,
 fulfill_item_unit_is_delivered_or_ofd,
 fulfill_item_unit_customer_promise_date_breach,
 fulfill_item_unit_order_date,
 fulfill_item_unit_order_date_key,
 fulfill_item_unit_order_time_key,
 fulfill_item_unit_created_at,
 fulfill_item_id,
 listing_id_key,
 fulfill_item_sku,
 fulfill_item_sales_channel,
 fulfill_item_ship_group_id,
 fulfill_item_type,
 fulfill_item_service_profile,
 warehouse_fulfill_reference_id_customer,
 purchase_order_item_id_b2c,
 fulfill_item_unit_deliver_after_time,
 fulfill_item_unit_dispatch_after_time,
 fulfill_item_unit_picklist_confirm_time,
 fulfill_item_unit_picklist_confirm_time_key,
 fulfill_item_unit_picklist_confirm_date_key,
 fulfill_item_unit_reserve_expected_time,
 fulfill_item_unit_reserve_in_b2b_expected_time,
 fulfill_item_unit_fulfillment_done_at,
 fulfill_item_unit_region_inventory,
 fulfill_item_unit_r2d_sla,
 fulfill_item_unit_shipping_cost,
 fulfill_item_unit_shipping_sla,
 fulfill_item_unit_expected_delivery_date_internal,
 fulfill_item_unit_slotted_delivery_breach,
 fulfill_item_unit_is_for_slotted_delivery,
 fulfill_item_unit_jit_breach,
 fulfill_item_unit_out_for_delivery_actual_time_key,
 fulfill_item_unit_out_for_delivery_actual_date_key,
 fulfill_item_unit_fulfillment_done_at_date_key,
 fulfill_item_unit_fulfillment_done_at_time_key,
 fulfill_item_unit_id_b2b,
 order_item_b2b as order_item_b2b,
 warehouse_reservation_id_b2b,
 purchase_order_item_id_b2b,
 fulfill_item_unit_reserve_expected_time_key,
 fulfill_item_unit_reserve_expected_date_key,
 fulfill_item_unit_slotted_delivery_breach_ofd,
 dispatch_change_log.dispatch_change_log_all_struct.col2 AS fulfill_item_unit_dispatch_by_old_datetime,
 dispatch_change_log.dispatch_change_log_all_struct.col1 AS fulfill_item_unit_dbd_change_reason,
 deliver_change_log.deliver_change_log_all_struct.col2 AS fulfill_item_unit_deliver_by_old_datetime,
 promisechange.reason AS fulfill_item_unit_deliver_date_change_reason,
 promisechange.created_at as promise_change_done_at,
fulunit.fulfill_item_preorder_release_date as fulfill_item_preorder_release_datetime,
fulunit.fulfill_item_preorder_release_date_type as fulfill_item_preorder_release_date_type,
fulunit.fulfill_item_listing_type as fulfill_item_listing_type,
fulunit.fulfill_item_embargo as fulfill_item_embargo,
fuldone.fulfillment_done_dropship_delivery_attempted as fulfillment_done_dropship_delivery_attempted,
fuldone.fulfillment_done_fulfillment_model as fulfillment_done_fulfillment_model,
fulunit.fulfill_item_unit_dropshipment as fulfill_item_unit_dropshipment,
fulunit.fulfill_item_delivery_type as fulfill_item_delivery_type,
fulunit.fulfill_item_unit_ready_for_pickup_status_actual_time as fulfill_item_unit_ready_for_pickup_status_actual_time,
fulunit.fulfill_item_unit_ready_for_pickup_status_after_time as fulfill_item_unit_ready_for_pickup_status_after_time,
fulunit.fulfill_item_unit_ready_for_pickup_status_expected_time as fulfill_item_unit_ready_for_pickup_status_expected_time,
fulunit.fulfill_item_unit_ready_for_pickup_status_updated_at as fulfill_item_unit_ready_for_pickup_status_updated_at,
fulunit.fulfill_item_pickup_center_id as fulfill_item_pickup_center_id,
CONCAT(substring(fulunit.fulfill_item_unit_ready_for_pickup_status_actual_time,1,4), substring(fulunit.fulfill_item_unit_ready_for_pickup_status_actual_time,6,2),substring(fulunit.fulfill_item_unit_ready_for_pickup_status_actual_time,9,2)) as fulfill_item_unit_ready_for_pickup_status_actual_date_key,
CONCAT(substring(fulunit.fulfill_item_unit_ready_for_pickup_status_after_time,1,4), substring(fulunit.fulfill_item_unit_ready_for_pickup_status_after_time,6,2),substring(fulunit.fulfill_item_unit_ready_for_pickup_status_after_time,9,2)) as fulfill_item_unit_ready_for_pickup_status_after_date_key,
CONCAT(substring(fulunit.fulfill_item_unit_ready_for_pickup_status_expected_time,1,4), substring(fulunit.fulfill_item_unit_ready_for_pickup_status_expected_time,6,2),substring(fulunit.fulfill_item_unit_ready_for_pickup_status_expected_time,9,2)) as fulfill_item_unit_ready_for_pickup_status_expected_date_key,
CONCAT(substring(fulunit.fulfill_item_unit_ready_for_pickup_status_updated_at,1,4), substring(fulunit.fulfill_item_unit_ready_for_pickup_status_updated_at,6,2),substring(fulunit.fulfill_item_unit_ready_for_pickup_status_updated_at,9,2)) as fulfill_item_unit_ready_for_pickup_status_updated_date_key,
dispatch_change_log.dispatch_change_log_do_fulfill_struct.col2 AS fulfill_item_unit_dispatch_by_dofulfill_old_datetime,
deliver_change_log.deliver_change_log_do_fulfill_struct.col2 AS fulfill_item_unit_deliver_by_dofulfill_old_datetime,
ship_change_log.ship_change_log_all_struct.col2  AS fulfill_item_unit_ship_by_old_datetime,
ship_change_log.ship_change_log_do_fulfill_struct.col2 AS fulfill_item_unit_ship_by_dofulfill_old_datetime,
 CASE
 WHEN dispatch_change_log.dispatch_change_log_all_struct.col2  IS NULL THEN CAST(fulfill_item_unit_dispatch_expected_time AS TIMESTAMP)
 ELSE dispatch_change_log.dispatch_change_log_all_struct.col2 
 END AS fulfill_item_unit_dispatch_after_dofulfill_time,
 CASE
 WHEN deliver_change_log.deliver_change_log_all_struct.col2 IS NULL THEN CAST(fulfill_item_unit_deliver_expected_time AS TIMESTAMP)
 ELSE deliver_change_log.deliver_change_log_all_struct.col2 
 END AS fulfill_item_unit_deliver_after_dofulfill_time,
 CASE
 WHEN ship_change_log.ship_change_log_all_struct.col2 IS NULL THEN CAST(fulfill_item_unit_ship_expected_time AS TIMESTAMP)
 ELSE ship_change_log.ship_change_log_all_struct.col2
 END AS fulfill_item_unit_ship_after_dofulfill_time,
fulunit.fulfill_item_unit_lpe_tier as fulfill_item_unit_lpe_tier,
fulunit.fulfill_item_unit_dispatched_status_after_time as fulfill_item_unit_dispatched_status_after_time,
fulunit.fulfill_item_unit_dispatched_status_after_date_key as fulfill_item_unit_dispatched_status_after_date_key,
fulunit.fulfill_item_unit_dispatched_status_after_time_key as fulfill_item_unit_dispatched_status_after_time_key,
fulunit.fulfill_item_shipping_category as fulfill_item_shipping_category,
fulunit.fulfill_item_listing_id as fulfill_item_listing_id,
fulunit.fulfill_item_vendor_id as fulfill_item_vendor_id,
prod_attribute_product_id_key,
dispatch_change_log2.old_expected_time AS fulfill_item_unit_dispatch_by_second_old_datetime,
dispatch_change_log2.change_reason AS fulfill_item_unit_dbd_second_change_reason,
fulunit.Fulfill_Item_Unit_Dropshipment_Next_Attempt as Fulfill_Item_Unit_Dropshipment_Next_Attempt,
fulunit.Fulfill_Item_Unit_Pickup_Done_Status_Actual_Time as Fulfill_Item_Unit_Pickup_Done_Status_Actual_Time,
fulunit.Fulfill_Item_Unit_Pickup_Done_Status_After_Time as Fulfill_Item_Unit_Pickup_Done_Status_After_Time,
fulunit.Fulfill_Item_Unit_Pickup_Done_Status_Created_At as Fulfill_Item_Unit_Pickup_Done_Status_Created_At,
fulunit.Fulfill_Item_Unit_Pickup_Done_Status_Expected_Time as Fulfill_Item_Unit_Pickup_Done_Status_Expected_Time,
fulunit.Fulfill_Item_Unit_Pickup_Done_Status_ID as Fulfill_Item_Unit_Pickup_Done_Status_ID,
fulunit.Fulfill_Item_Unit_Pickup_Done_Status_Updated_At as Fulfill_Item_Unit_Pickup_Done_Status_Updated_At,
fuldone.fulfillment_done_preorder_previously as fulfillment_done_preorder_previously,
fulunit.fulfill_item_unit_delivered_status_updated_at as fulfill_item_unit_delivered_status_updated_at,
deliver_change_log.deliver_change_log_all_struct.col1 AS fulfill_item_unit_deliver_date_plan_change_reason,
deliver_change_log.deliver_change_log_all_struct.col3 as deliver_expected_plan_change_done_at,
promisechange.new_date as fulfill_item_unit_new_promise_date,
promisechange.sub_reason as fulfill_item_unit_deliver_date_change_subreason,
fulunit.fulfill_item_unit_dispatch_service_tier as fulfill_item_unit_dispatch_service_tier,
dispatch_change_log2.old_expected_time AS fulfill_item_unit_dispatch_by_second_heal_datetime, 
dispatch_change_log1.old_expected_time AS fulfill_item_unit_dispatch_by_first_heal_datetime, 
dispatch_change_log1.change_reason AS fulfill_item_unit_dbd_first_change_reason, 
fulunit.external_order_id,
fulunit.fulfill_item_seller_id,
fulunit.fulfill_item_product_id,
fulunit.fulfill_item_unit_source_pincode,
fulunit.fulfill_item_unit_destination_pincode,
lzn_result.new_lzn AS fulfill_item_unit_new_shipment_movement_type,
fulunit.dropshipment_undelivered_first_attempt_time,
fulunit.dropshipment_undelivered_first_attempt_reason,
fulunit.dropshipment_undelivered_first_attempt_next_delivery_time,
fulunit.dropshipment_undelivered_second_attempt_time,
fulunit.dropshipment_undelivered_second_attempt_reason,
fulunit.dropshipment_undelivered_second_attempt_next_delivery_time,
fulunit.dropshipment_undelivered_third_attempt_time,
fulunit.dropshipment_undelivered_third_attempt_reason,
fulunit.dropshipment_undelivered_third_attempt_next_delivery_time,
fuldone.fulfillment_done_cpd_lower_bound,
fuldone.fulfillment_done_predicted_cpd_lower_bound,
fuldone.fulfillment_done_sla_range_type,
fulunit.fulfill_item_unit_logistics_type,
fulunit.lpe_ref_id
FROM
(SELECT `data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_actual_time AS fulfill_item_unit_dispatch_actual_time,
`data`.fulfill_item_unit_shipped_status.fulfill_item_unit_shipped_status_actual_time AS fulfill_item_unit_ship_actual_time,
`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_actual_time AS fulfill_item_unit_deliver_actual_time,
`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_expected_time AS fulfill_item_unit_dispatch_expected_time,
`data`.fulfill_item_unit_shipped_status.fulfill_item_unit_shipped_status_expected_time AS fulfill_item_unit_ship_expected_time,
`data`.fulfill_item_unit_order_item_mapping.order_item_mapping_external_id AS order_item_id,
`data`.fulfill_item_unit_id AS fulfill_item_unit_id,
`data`.fulfill_item_unit_status AS fulfill_item_unit_status,
 coalesce(`data`.fulfill_item_unit_reserved_in_b2b_status.fulfill_item_unit_reserved_in_b2b_status_actual_time, `data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_actual_time) AS fulfill_item_unit_reserve_actual_time,
`data`.fulfill_item_unit_ekl_shipment_mapping.ekl_shipment_mapping_external_id AS shipment_merchant_reference_id,
`data`.fulfill_item_unit_updated_at AS fulfill_item_unit_updated_at,
`data`.fulfill_item_unit_region AS fulfill_item_unit_region,
`data`.Fulfill_Item_Unit_Dropshipment_Next_Attempt as Fulfill_Item_Unit_Dropshipment_Next_Attempt,
`data`.Fulfill_Item_Unit_Pickup_Done_Status.Fulfill_Item_Unit_Pickup_Done_Status_Actual_Time as Fulfill_Item_Unit_Pickup_Done_Status_Actual_Time,
`data`.Fulfill_Item_Unit_Pickup_Done_Status.Fulfill_Item_Unit_Pickup_Done_Status_After_Time as Fulfill_Item_Unit_Pickup_Done_Status_After_Time,
`data`.Fulfill_Item_Unit_Pickup_Done_Status.Fulfill_Item_Unit_Pickup_Done_Status_Created_At as Fulfill_Item_Unit_Pickup_Done_Status_Created_At,
`data`.Fulfill_Item_Unit_Pickup_Done_Status.Fulfill_Item_Unit_Pickup_Done_Status_Expected_Time as Fulfill_Item_Unit_Pickup_Done_Status_Expected_Time,
`data`.Fulfill_Item_Unit_Pickup_Done_Status.Fulfill_Item_Unit_Pickup_Done_Status_ID as Fulfill_Item_Unit_Pickup_Done_Status_ID,
`data`.Fulfill_Item_Unit_Pickup_Done_Status.Fulfill_Item_Unit_Pickup_Done_Status_Updated_At as Fulfill_Item_Unit_Pickup_Done_Status_Updated_At,
lookup_date(`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_actual_time) AS fulfill_item_unit_dispatch_actual_date_key,
 lookup_date(`data`.fulfill_item_unit_shipped_status.fulfill_item_unit_shipped_status_actual_time) AS fulfill_item_unit_ship_actual_date_key,
lookup_date(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_actual_time) AS fulfill_item_unit_deliver_actual_date_key,
 lookup_date(`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_expected_time) AS fulfill_item_unit_dispatch_expected_date_key,
lookup_date(coalesce(`data`.fulfill_item_unit_reserved_in_b2b_status.fulfill_item_unit_reserved_in_b2b_status_actual_time,`data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_actual_time)) AS fulfill_item_unit_reserve_actual_date_key,
lookup_date(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time) AS fulfill_item_unit_deliver_expected_date_key,
 lookup_date(`data`.fulfill_item_unit_fulfill_item.fulfill_item_order_date) AS fulfill_item_unit_order_date_key,
 lookup_date(CASE WHEN `data`.fulfill_item_unit_reserved_in_b2b_status.fulfill_item_unit_reserved_in_b2b_status_actual_time IS NOT NULL THEN `data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_actual_time END) AS fulfill_item_unit_picklist_confirm_date_key,
lookup_date(`data`.fulfill_item_unit_tags.out_for_delivery_actual_time) AS fulfill_item_unit_out_for_delivery_actual_date_key,
 lookup_date(`data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_expected_time) AS fulfill_item_unit_reserve_expected_date_key,
`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time AS fulfill_item_unit_deliver_expected_time,
lookup_time(`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_actual_time) AS fulfill_item_unit_dispatch_actual_time_key,
 lookup_time(`data`.fulfill_item_unit_shipped_status.fulfill_item_unit_shipped_status_actual_time) AS fulfill_item_unit_ship_actual_time_key,
lookup_time(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_actual_time) AS fulfill_item_unit_deliver_actual_time_key,
 lookup_time(`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_expected_time) AS fulfill_item_unit_dispatch_expected_time_key,
lookup_time(CASE WHEN `data`.fulfill_item_unit_reserved_in_b2b_status.fulfill_item_unit_reserved_in_b2b_status_actual_time IS NULL THEN `data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_actual_time ELSE `data`.fulfill_item_unit_reserved_in_b2b_status.fulfill_item_unit_reserved_in_b2b_status_actual_time END) AS fulfill_item_unit_reserve_actual_time_key,
lookup_time(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time) AS fulfill_item_unit_deliver_expected_time_key,
 lookup_time(`data`.fulfill_item_unit_fulfill_item.fulfill_item_order_date) AS fulfill_item_unit_order_time_key,
 lookup_time(CASE WHEN `data`.fulfill_item_unit_reserved_in_b2b_status.fulfill_item_unit_reserved_in_b2b_status_actual_time IS NOT NULL THEN `data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_actual_time END) AS fulfill_item_unit_picklist_confirm_time_key,
lookup_time(`data`.fulfill_item_unit_tags.out_for_delivery_actual_time) AS fulfill_item_unit_out_for_delivery_actual_time_key,
 lookup_time(`data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_expected_time) AS fulfill_item_unit_reserve_expected_time_key,
lookupkey('seller_id', `data`.fulfill_item_unit_fulfill_item.fulfill_item_seller_id) AS seller_id_key,
lookupkey('product_id ', `data`.fulfill_item_unit_fulfill_item.fulfill_item_fsn) AS product_id_key,
lookupkey('product_id ', `data`.fulfill_item_unit_fulfill_item.fulfill_item_fsn) AS prod_attribute_product_id_key,
lookupkey('listing_id', `data`.fulfill_item_unit_fulfill_item.fulfill_item_listing_id) AS listing_id_key,
`data`.fulfill_item_unit_tags.out_for_delivery_actual_time AS fulfill_item_unit_out_for_delivery_actual_time,
CASE
WHEN `data`.fulfill_item_unit_region_type = 'proc' THEN 'jit'
ELSE 'inventory'
END AS fulfill_item_unit_is_jit,
 `data`.fulfill_item_unit_fulfill_item.fulfill_item_breach_cost AS fulfill_item_unit_breach_cost,
 CASE
 WHEN `data`.fulfill_item_unit_region_type != 'proc'
AND `data`.fulfill_item_unit_status = 'procuring' THEN 'reserved'
 WHEN `data`.fulfill_item_unit_status = 'shipped'
AND `data`.fulfill_item_unit_tags.out_for_delivery_actual_time IS NOT NULL THEN 'delivery_attempted'
 ELSE `data`.fulfill_item_unit_status
 END AS fulfill_item_unit_status_modified,
`data`.fulfill_item_unit_region_type AS fulfill_item_unit_region_type,
CASE
WHEN `data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_actual_time IS NULL
 AND `data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_expected_time <= from_unixtime(unix_timestamp()) THEN 'Pending-Breach'
WHEN `data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_actual_time IS NULL
 AND `data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_expected_time > from_unixtime(unix_timestamp()) THEN 'Pending-NonBreach'
WHEN `data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_expected_time < `data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_actual_time THEN 'Breach'
WHEN `data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_expected_time > `data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_actual_time THEN 'Ahead'
WHEN `data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_expected_time = `data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_actual_time THEN 'Met'
END AS fulfill_item_unit_dispatch_cutoff_breach,
 CASE
 WHEN (`data`.fulfill_item_unit_status = 'shipped'
 AND `data`.fulfill_item_unit_tags.out_for_delivery_actual_time IS NOT NULL)
OR `data`.fulfill_item_unit_status = 'delivered' THEN 1
 ELSE 0
 END AS fulfill_item_unit_is_delivered_or_ofd,
CASE
WHEN `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_actual_time IS NULL
 AND to_date(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time) <= to_date(from_unixtime(unix_timestamp())) THEN 'Pending-Breach'
WHEN `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_actual_time IS NULL
 AND to_date(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time) > to_date(from_unixtime(unix_timestamp())) THEN 'Pending-NonBreach'
WHEN to_date(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time) < to_date(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_actual_time) THEN 'Breach'
WHEN to_date(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time) > to_date(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_actual_time) THEN 'Ahead'
WHEN to_date(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time) = to_date(`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_actual_time) THEN 'Met'
END AS fulfill_item_unit_customer_promise_date_breach,
`data`.fulfill_item_unit_fulfill_item.fulfill_item_order_date AS fulfill_item_unit_order_date,
`data`.fulfill_item_unit_created_at AS fulfill_item_unit_created_at,
`data`.fulfill_item_unit_fulfill_item.fulfill_item_id AS fulfill_item_id,
`data`.fulfill_item_unit_fulfill_item.fulfill_item_sku AS fulfill_item_sku,
`data`.fulfill_item_unit_fulfill_item.fulfill_item_sales_channel AS fulfill_item_sales_channel,
`data`.fulfill_item_unit_fulfill_item.fulfill_item_ship_group_id AS fulfill_item_ship_group_id,
`data`.fulfill_item_unit_fulfill_item.fulfill_item_type AS fulfill_item_type,
`data`.fulfill_item_unit_fulfill_item.fulfill_item_service_profile AS fulfill_item_service_profile,
CASE
WHEN `data`.fulfill_item_unit_warehouse_reference_mapping.Warehouse_Reference_Mapping_Is_Deleted = 0 THEN `data`.fulfill_item_unit_warehouse_reference_mapping.warehouse_reference_mapping_external_id
ELSE NULL
END AS warehouse_fulfill_reference_id_customer,
CASE
WHEN `data`.fulfill_item_unit_procurement_reference_mapping.Procurement_Reference_Mapping_Is_Deleted = 0 THEN `data`.fulfill_item_unit_procurement_reference_mapping.procurement_reference_mapping_external_id
ELSE NULL
END AS purchase_order_item_id_b2c,
`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_after_time AS fulfill_item_unit_deliver_after_time,
`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_after_time AS fulfill_item_unit_dispatch_after_time,
CASE
WHEN `data`.fulfill_item_unit_reserved_in_b2b_status.fulfill_item_unit_reserved_in_b2b_status_actual_time IS NOT NULL THEN `data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_actual_time
END AS fulfill_item_unit_picklist_confirm_time,
`data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_expected_time AS fulfill_item_unit_reserve_expected_time,
`data`.fulfill_item_unit_reserved_in_b2b_status.fulfill_item_unit_reserved_in_b2b_status_expected_time AS fulfill_item_unit_reserve_in_b2b_expected_time,
CASE
WHEN `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_after_time IS NULL THEN 'NotSlottedDelivery'
WHEN `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_actual_time IS NULL
AND `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time <= from_unixtime(unix_timestamp()) THEN 'Pending-Breach'
WHEN `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_actual_time IS NULL
 AND `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time > from_unixtime(unix_timestamp()) THEN 'Pending-NonBreach'
WHEN `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time <= `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_actual_time THEN 'Breach-After'
WHEN `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time > `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_actual_time
 AND `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_after_time <= `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_actual_time THEN 'Met'
WHEN `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_after_time > `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_actual_time THEN 'Breach_Before'
END AS fulfill_item_unit_slotted_delivery_breach,
 CASE
 WHEN `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_after_time IS NULL THEN 'NotSlotted'
 ELSE 'Slotted'
 END AS fulfill_item_unit_is_for_slotted_delivery,
CASE
WHEN `data`.fulfill_item_unit_region_type IS NULL THEN NULL
WHEN `data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_actual_time IS NULL
 AND `data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_expected_time < from_unixtime(unix_timestamp()) THEN 'Pending-Breach'
WHEN `data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_actual_time IS NULL
 AND `data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_expected_time >= from_unixtime(unix_timestamp()) THEN 'Pending-NonBreach'
WHEN `data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_expected_time < `data`.fulfill_item_unit_reserved_status.fulfill_item_unit_reserved_status_actual_time THEN 'Breach'
ELSE 'Met'
END AS fulfill_item_unit_jit_breach,
 NULL AS fulfill_item_unit_id_b2b,
 NULL AS order_item_b2b,
 NULL AS warehouse_reservation_id_b2b,
 NULL AS purchase_order_item_id_b2b,
 CASE
 WHEN `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_after_time IS NULL THEN 'NotSlottedDelivery'
 WHEN `data`.fulfill_item_unit_tags.out_for_delivery_actual_time IS NULL
AND `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time <= from_unixtime(unix_timestamp()) THEN 'Pending-Breach'
 WHEN `data`.fulfill_item_unit_tags.out_for_delivery_actual_time IS NULL
AND `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time > from_unixtime(unix_timestamp()) THEN 'Pending-NonBreach'
 WHEN `data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_expected_time <= `data`.fulfill_item_unit_tags.out_for_delivery_actual_time THEN 'Breach'
 ELSE 'Met'
 END AS fulfill_item_unit_slotted_delivery_breach_ofd,
`data`.fulfill_item_unit_dispatched_status.fulfill_item_unit_dispatched_status_id AS fulfill_item_unit_dispatched_status_id,
`data`.fulfill_item_unit_delivered_status.fulfill_item_unit_delivered_status_id AS fulfill_item_unit_delivered_status_id,
`data`.fulfill_item_unit_shipped_status.fulfill_item_unit_shipped_status_id AS fulfill_item_unit_shipped_status_id,
CASE
WHEN `data`.fulfill_item_unit_fulfill_item.fulfill_item_breach_cost >= 20000 THEN 'sdd'
WHEN `data`.fulfill_item_unit_fulfill_item.fulfill_item_breach_cost >= 1000 THEN 'ndd'
ELSE 'regular'
END AS promise_tier_temp,
`data`.fulfill_item_unit_fulfill_item.fulfill_item_preorder_release_date as fulfill_item_preorder_release_date,
`data`.fulfill_item_unit_fulfill_item.fulfill_item_preorder_release_date_type as fulfill_item_preorder_release_date_type,
`data`.fulfill_item_unit_fulfill_item.fulfill_item_listing_type as fulfill_item_listing_type,
`data`.fulfill_item_unit_fulfill_item.fulfill_item_embargo as fulfill_item_embargo,
`data`.Fulfill_Item_Unit_Dropshipment as fulfill_item_unit_dropshipment,
`data`.fulfill_item_unit_fulfill_item.Fulfill_Item_Delivery_Type as fulfill_item_delivery_type,
`data`.Fulfill_Item_Unit_Ready_For_Pickup_Status.Fulfill_Item_Unit_Ready_For_Pickup_Status_Actual_Time as fulfill_item_unit_ready_for_pickup_status_actual_time,
`data`.Fulfill_Item_Unit_Ready_For_Pickup_Status.Fulfill_Item_Unit_Ready_For_Pickup_Status_After_Time as fulfill_item_unit_ready_for_pickup_status_after_time,
`data`.Fulfill_Item_Unit_Ready_For_Pickup_Status.Fulfill_Item_Unit_Ready_For_Pickup_Status_Expected_Time as fulfill_item_unit_ready_for_pickup_status_expected_time,
`data`.Fulfill_Item_Unit_Ready_For_Pickup_Status.Fulfill_Item_Unit_Ready_For_Pickup_Status_Updated_At as fulfill_item_unit_ready_for_pickup_status_updated_at,
`data`.fulfill_item_unit_fulfill_item.Fulfill_Item_Pickup_Center_Id as fulfill_item_pickup_center_id,
`data`.Fulfill_Item_Unit_LPE_Tier as fulfill_item_unit_lpe_tier,
`data`.Fulfill_Item_Unit_Dispatch_Service_Tier as fulfill_item_unit_dispatch_service_tier,
`data`.fulfill_item_unit_dispatched_status.Fulfill_Item_Unit_Dispatched_Status_After_Time AS fulfill_item_unit_dispatched_status_after_time,
lookup_date(`data`.fulfill_item_unit_dispatched_status.Fulfill_Item_Unit_Dispatched_Status_After_Time) AS fulfill_item_unit_dispatched_status_after_date_key,
lookup_time(`data`.fulfill_item_unit_dispatched_status.Fulfill_Item_Unit_Dispatched_Status_After_Time) AS fulfill_item_unit_dispatched_status_after_time_key,
`data`.Fulfill_Item_Unit_Shipping_Category as fulfill_item_shipping_category,
`data`.fulfill_item_unit_fulfill_item.Fulfill_Item_Listing_ID as fulfill_item_listing_id,
`data`.Fulfill_Item_Unit_Vendor_ID as fulfill_item_vendor_id,
`data`.fulfill_item_unit_delivered_status.Fulfill_Item_Unit_Delivered_Status_Updated_At as fulfill_item_unit_delivered_status_updated_at,
`data`.fulfill_item_unit_external_order_mapping.external_order_mapping_external_id AS external_order_id,
`data`.fulfill_item_unit_fulfill_item.fulfill_item_seller_id,
`data`.fulfill_item_unit_fulfill_item.fulfill_item_fsn as fulfill_item_product_id,
`data`.fulfill_item_unit_source_pincode,
`data`.fulfill_item_unit_destination_pincode,
if ( size(`data`.fulfill_item_unit_dropshipment_undelivered_attempts) > 0 , `data`.fulfill_item_unit_dropshipment_undelivered_attempts[0].attempt_time, NULL) AS dropshipment_undelivered_first_attempt_time,
if ( size(`data`.fulfill_item_unit_dropshipment_undelivered_attempts) > 0 , `data`.fulfill_item_unit_dropshipment_undelivered_attempts[0].attempt_reason, NULL) AS dropshipment_undelivered_first_attempt_reason,
if ( size(`data`.fulfill_item_unit_dropshipment_undelivered_attempts) > 0 , `data`.fulfill_item_unit_dropshipment_undelivered_attempts[0].next_delivery_time, NULL) AS dropshipment_undelivered_first_attempt_next_delivery_time,
if ( size(`data`.fulfill_item_unit_dropshipment_undelivered_attempts) > 1 , `data`.fulfill_item_unit_dropshipment_undelivered_attempts[1].attempt_time, NULL) AS dropshipment_undelivered_second_attempt_time,
if ( size(`data`.fulfill_item_unit_dropshipment_undelivered_attempts) > 1 , `data`.fulfill_item_unit_dropshipment_undelivered_attempts[1].attempt_reason, NULL) AS dropshipment_undelivered_second_attempt_reason,
if ( size(`data`.fulfill_item_unit_dropshipment_undelivered_attempts) > 1 , `data`.fulfill_item_unit_dropshipment_undelivered_attempts[1].next_delivery_time, NULL) AS dropshipment_undelivered_second_attempt_next_delivery_time,
if ( size(`data`.fulfill_item_unit_dropshipment_undelivered_attempts) > 2 , `data`.fulfill_item_unit_dropshipment_undelivered_attempts[2].attempt_time, NULL) AS dropshipment_undelivered_third_attempt_time,
if ( size(`data`.fulfill_item_unit_dropshipment_undelivered_attempts) > 2 , `data`.fulfill_item_unit_dropshipment_undelivered_attempts[2].attempt_reason, NULL) AS dropshipment_undelivered_third_attempt_reason,
if ( size(`data`.fulfill_item_unit_dropshipment_undelivered_attempts) > 2 , `data`.fulfill_item_unit_dropshipment_undelivered_attempts[2].next_delivery_time, NULL) AS dropshipment_undelivered_third_attempt_next_delivery_time,
`data`.fulfill_item_unit_logistics_type,
`data`.fulfill_item_unit_lpe_ref_id_mapping.lpe_ref_id_mapping_external_id AS lpe_ref_id
FROM bigfoot_snapshot.dart_fkint_scp_fulfillment_fulfill_item_unit_2_view fiu) fulunit
LEFT JOIN
(SELECT fulfillment_done_fulfill_item_unit_id,
 fulfill_item_unit_shipment_movement_type,
 fulfill_item_unit_promise_tier,
 fulfill_item_unit_destination_pincode_key,
 fulfill_item_unit_fulfillment_done_at,
 fulfill_item_unit_region_inventory,
 fulfill_item_unit_r2d_sla,
 fulfill_item_unit_shipping_cost,
 fulfill_item_unit_shipping_sla,
 fulfill_item_unit_expected_delivery_date_internal,
 fulfill_item_unit_fulfillment_done_at_date_key,
 fulfill_item_unit_fulfillment_done_at_time_key,
fulfillment_done_dropship_delivery_attempted,
fulfillment_done_fulfillment_model,
fulfillment_done_preorder_previously,
fulfillment_done_cpd_lower_bound,
fulfillment_done_predicted_cpd_lower_bound,
fulfillment_done_sla_range_type
 FROM
(SELECT fulfillment_done_fulfill_item_unit_id,
 fulfill_item_unit_shipment_movement_type,
 fulfill_item_unit_promise_tier,
 fulfill_item_unit_destination_pincode_key,
 fulfill_item_unit_fulfillment_done_at,
 fulfill_item_unit_region_inventory,
 fulfill_item_unit_r2d_sla,
 fulfill_item_unit_shipping_cost,
 fulfill_item_unit_shipping_sla,
 fulfill_item_unit_expected_delivery_date_internal,
 fulfill_item_unit_fulfillment_done_at_date_key,
 fulfill_item_unit_fulfillment_done_at_time_key,
fulfillment_done_dropship_delivery_attempted,
fulfillment_done_fulfillment_model, 
fulfillment_done_preorder_previously,
 ROW_NUMBER() OVER (PARTITION BY fulfillment_done_fulfill_item_unit_id,fulfill_item_unit_promise_tier
ORDER BY fulfill_item_unit_fulfillment_done_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )  as rank,
fulfillment_done_cpd_lower_bound,
fulfillment_done_predicted_cpd_lower_bound,
fulfillment_done_sla_range_type
FROM  ( SELECT `data`.fulfillment_done_fulfill_item_unit_id AS fulfillment_done_fulfill_item_unit_id,
tier.fulfillment_tier_shipment_movement_type AS fulfill_item_unit_shipment_movement_type,
case 
when tier.fulfillment_tier_promise_tier in ( 'REGULAR' ,'ECONOMY', 'economy', 'regular' ) THEN 'regular'
when tier.fulfillment_tier_promise_tier in ('EXPRESS','ndd' , 'NDD' , 'express') THEN 'ndd'
when tier.fulfillment_tier_promise_tier in  ( 'sdd', 'SDD' , 'PREMIUM','premium') THEN 'sdd' 
end as fulfill_item_unit_promise_tier,
lookupkey('pincode', `data`.fulfillment_done_destination_pincode) AS fulfill_item_unit_destination_pincode_key,
`data`.fulfillment_done_at AS fulfill_item_unit_fulfillment_done_at,
`data`.Fulfillment_Done_Assigned_Region.fulfillment_region_inventory AS fulfill_item_unit_region_inventory,
tier.fulfillment_tier_r2d AS fulfill_item_unit_r2d_sla,
tier.fulfillment_tier_shipping_cost AS fulfill_item_unit_shipping_cost,
tier.fulfillment_tier_shipping_sla AS fulfill_item_unit_shipping_sla,
tier.fulfillment_tier_expected_delivery_date AS fulfill_item_unit_expected_delivery_date_internal,
lookup_date(`data`.fulfillment_done_at) AS fulfill_item_unit_fulfillment_done_at_date_key,
lookup_time(`data`.fulfillment_done_at) AS fulfill_item_unit_fulfillment_done_at_time_key,
`data`.fulfillment_done_dropship_delivery_attempted as fulfillment_done_dropship_delivery_attempted,
`data`.fulfillment_done_fulfillment_model as fulfillment_done_fulfillment_model,
`data`.Fulfillment_Done_Preorder_Previously as fulfillment_done_preorder_previously,
`data`.Fulfillment_Done_Fiu_Cpd_Lower_Bound as fulfillment_done_cpd_lower_bound,
`data`.Fulfillment_Done_Fiu_Cpd_Predicted_Lower_Bound as fulfillment_done_predicted_cpd_lower_bound,
`data`.Fulfillment_Done_Fiu_Sla_Range_Type as fulfillment_done_sla_range_type
FROM bigfoot_journal.dart_fkint_scp_fulfillment_fulfillment_done_6_view fd LATERAL VIEW explode(`data`.Fulfillment_Done_Assigned_Region.fulfillment_region_tier) exp_table2 AS tier)  TEST )  
ranked where ranked.rank = 1) fuldone 
ON fuldone.fulfillment_done_fulfill_item_unit_id = fulunit.fulfill_item_unit_id AND fulunit.promise_tier_temp = fuldone.fulfill_item_unit_promise_tier
LEFT JOIN
( select source, destination, new_lzn from bigfoot_common.lzn_tab ) lzn_result 
on lzn_result.source = fulunit.fulfill_item_unit_source_pincode and lzn_result.destination = fulunit.fulfill_item_unit_destination_pincode
LEFT JOIN 
( SELECT 
`data`.fulfill_item_unit_status_id  as fulfill_item_unit_status_id,
min( if (if (`data`.reason is null, '', `data`.reason ) <> 'do_fulfill', struct( `data`.created_at , struct ( `data`.reason, `data`.expected_time , `data`.created_at )), null)).col2 as dispatch_change_log_all_struct,
min( if ( `data`.reason = 'do_fulfill', struct(`data`.created_at , struct( `data`.reason, `data`.expected_time , `data`.created_at ) ), null)).col2 as dispatch_change_log_do_fulfill_struct
from bigfoot_journal.dart_fkint_scp_fulfillment_fulfill_item_unit_status_time_change_log_1_view
group by `data`.fulfill_item_unit_status_id ) dispatch_change_log 
ON fulunit.fulfill_item_unit_dispatched_status_id = dispatch_change_log.fulfill_item_unit_status_id
LEFT JOIN
(SELECT `data`.fulfill_item_unit_status_id AS fulfill_item_unit_status_id, `data`.reason as change_reason, `data`.expected_time AS old_expected_time, `data`.created_at as promise_change_done_at 
from (select  data , ROW_NUMBER()  OVER (PARTITION BY `data`.fulfill_item_unit_status_id ORDER BY `data`.created_at ASC) as rank  
FROM bigfoot_journal.dart_fkint_scp_fulfillment_fulfill_item_unit_status_time_change_log_1_view WHERE `data`.reason  = 'dispatch_breach' ) ranked 
where ranked.rank = 1 ) dispatch_change_log1
ON fulunit.fulfill_item_unit_dispatched_status_id = dispatch_change_log1.fulfill_item_unit_status_id
LEFT JOIN
(SELECT `data`.fulfill_item_unit_status_id AS fulfill_item_unit_status_id, `data`.reason as change_reason, `data`.expected_time AS old_expected_time, `data`.created_at as promise_change_done_at 
from (select  data , ROW_NUMBER()  OVER (PARTITION BY `data`.fulfill_item_unit_status_id ORDER BY `data`.created_at ASC) as rank  
FROM bigfoot_journal.dart_fkint_scp_fulfillment_fulfill_item_unit_status_time_change_log_1_view WHERE `data`.reason  = 'dispatch_breach' ) ranked 
where ranked.rank = 2 ) dispatch_change_log2
ON fulunit.fulfill_item_unit_dispatched_status_id = dispatch_change_log2.fulfill_item_unit_status_id
LEFT JOIN 
( select `data`.fulfill_item_unit_status_id  as fulfill_item_unit_status_id,
min( if ( if(`data`.reason is null, '', `data`.reason ) <> 'do_fulfill', struct(`data`.created_at , struct( `data`.reason, `data`.expected_time , `data`.created_at ) ), null)).col2 as deliver_change_log_all_struct,
min( if( `data`.reason = 'do_fulfill',  struct(`data`.created_at , struct( `data`.reason, `data`.expected_time , `data`.created_at ) ), null )).col2 as deliver_change_log_do_fulfill_struct
from bigfoot_journal.dart_fkint_scp_fulfillment_fulfill_item_unit_status_time_change_log_1_view
group by `data`.fulfill_item_unit_status_id) deliver_change_log
ON fulunit.fulfill_item_unit_delivered_status_id = deliver_change_log.fulfill_item_unit_status_id
LEFT JOIN 
( select `data`.fulfill_item_unit_status_id  as fulfill_item_unit_status_id,
min( if( if(`data`.reason is null, '', `data`.reason ) <> 'do_fulfill',  struct(`data`.created_at , struct( `data`.reason, `data`.expected_time , `data`.created_at ) ), null)).col2 as ship_change_log_all_struct,
min( if ( `data`.reason = 'do_fulfill' , struct(`data`.created_at , struct( `data`.reason, `data`.expected_time , `data`.created_at ) ), null)).col2 as ship_change_log_do_fulfill_struct
from bigfoot_journal.dart_fkint_scp_fulfillment_fulfill_item_unit_status_time_change_log_1_view
group by `data`.fulfill_item_unit_status_id) ship_change_log
ON fulunit.fulfill_item_unit_shipped_status_id = ship_change_log.fulfill_item_unit_status_id
left join (
select
fiu,
created_at,
new_date,
reason,
sub_reason
from(select 
`data`.fulfill_item_unit_id as fiu,
`data`.created_at as created_at,
`data`.new_date as new_date,
`data`.reason as reason,
`data`.sub_reason as sub_reason,
ROW_NUMBER() over (partition by `data`.fulfill_item_unit_id order by `data`.created_at asc) as rnk
from bigfoot_journal.dart_fkint_scp_fulfillment_fulfill_item_unit_change_in_oms_customer_promised_date_1_0) tempevent where rnk=1) promisechange on promisechange.fiu=fulunit.fulfill_item_unit_id;
