INSERT overwrite table fulfillment_liteshipmentstatusevent_base_fact

select shipment_event.sr_id,
 shipment_event.shipment_reference_ids,
 max (case when rank_current_state = 1 then shipment_event.status else null end) as shipment_current_status,
 max (case when rank_current_state = 1 then shipment_event.secondary_status else null end) as shipment_current_secondary_status,
 max (case when rank_current_state = 1 then shipment_event.status_date_time else null end) as shipment_current_status_date_time,
 max (case when rank_current_state = 1 then shipment_event.status_location else null end) as shipment_current_location,
 max (case when rank_current_state = 1 then shipment_event.remarks else null end) as shipment_current_remarks,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_created' then shipment_event.status_date_time else null end) as pickup_created_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'out_for_pickup' then shipment_event.status_date_time else null end) as out_for_pickup_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_completed' then shipment_event.status_date_time else null end) as pickup_completed_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_failed' then shipment_event.status_date_time else null end) as pickup_failed_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_cancelled' then shipment_event.status_date_time else null end) as pickup_cancelled_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'in_scan_at_hub' then shipment_event.status_date_time else null end) as in_scan_at_hub_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'in_transit' then shipment_event.status_date_time else null end) as in_transit_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'received_at_delivery_hub' then shipment_event.status_date_time else null end) as received_at_delivery_hub_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'out_for_delivery' then shipment_event.status_date_time else null end) as out_for_delivery_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.status_date_time else null end) as undelivered_attempted_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'delivered' then shipment_event.status_date_time else null end) as delivered_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'damaged' then shipment_event.status_date_time else null end) as damaged_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'lost' then shipment_event.status_date_time else null end) as lost_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'update_lpd' then shipment_event.status_date_time else null end) as update_lpd_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'ready_to_dispatch' then shipment_event.status_date_time else null end) as ready_to_dispatch_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_scheduled' then shipment_event.status_date_time else null end) as rvp_scheduled_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_out_for_pickup' then shipment_event.status_date_time else null end) as rvp_out_for_pickup_first_datetime,
min (case when rank_asc = 1  and (lower(shipment_event.status) = 'rvp_pickup_completed' or lower(shipment_event.status) = 'rvp_pickup_complete') then shipment_event.status_date_time else null end) as rvp_pickup_completed_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_pickup_failed' then shipment_event.status_date_time else null end) as rvp_pickup_failed_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_cancelled' then shipment_event.status_date_time else null end) as rvp_cancelled_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_in_scan_at_hub' then shipment_event.status_date_time else null end) as rvp_in_scan_at_hub_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_in_transit' then shipment_event.status_date_time else null end) as rvp_in_transit_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_received_at_delivery_hub' then shipment_event.status_date_time else null end) as rvp_received_at_delivery_hub_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_out_for_delivery' then shipment_event.status_date_time else null end) as rvp_out_for_delivery_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_undelivered_attempted' then shipment_event.status_date_time else null end) as rvp_undelivered_attempted_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_completed' then shipment_event.status_date_time else null end) as rvp_completed_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rto_confirmed' then shipment_event.status_date_time else null end) as rto_confirmed_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rto_received_at_delivery_hub' then shipment_event.status_date_time else null end) as rto_received_at_delivery_hub_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rto_out_for_delivery' then shipment_event.status_date_time else null end) as rto_out_for_delivery_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rto_undelivered_attempted' then shipment_event.status_date_time else null end) as rto_undelivered_attempted_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rto_completed' then shipment_event.status_date_time else null end) as rto_completed_first_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_failed' then shipment_event.secondary_status  else null end) as pickup_failed_first_secondary_status,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.secondary_status  else null end) as undelivered_attempted_first_secondary_status,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_pickup_failed' then shipment_event.secondary_status  else null end) as rvp_pickup_failed_first_secondary_status,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_undelivered_attempted' then shipment_event.secondary_status  else null end) as rvp_undelivered_attempted_first_secondary_status,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rto_undelivered_attempted' then shipment_event.secondary_status  else null end) as rto_undelivered_attempted_first_secondary_status,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_failed' then shipment_event.remarks  else null end) as pickup_failed_first_remarks,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.remarks  else null end) as undelivered_attempted_first_remarks,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_pickup_failed' then shipment_event.remarks  else null end) as rvp_pickup_failed_first_remarks,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_undelivered_attempted' then shipment_event.remarks  else null end) as rvp_undelivered_attempted_first_remarks,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rto_undelivered_attempted' then shipment_event.remarks  else null end) as rto_undelivered_attempted_first_remarks,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_created' then shipment_event.status_location else null end) as pickup_created_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'out_for_pickup' then shipment_event.status_location else null end) as out_for_pickup_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_completed' then shipment_event.status_location else null end) as pickup_completed_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_failed' then shipment_event.status_location else null end) as pickup_failed_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_cancelled' then shipment_event.status_location else null end) as pickup_cancelled_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'in_scan_at_hub' then shipment_event.status_location else null end) as in_scan_at_hub_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'in_transit' then shipment_event.status_location else null end) as in_transit_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'received_at_delivery_hub' then shipment_event.status_location else null end) as received_at_delivery_hub_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'out_for_delivery' then shipment_event.status_location else null end) as out_for_delivery_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.status_location else null end) as undelivered_attempted_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'delivered' then shipment_event.status_location else null end) as delivered_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'damaged' then shipment_event.status_location else null end) as damaged_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'lost' then shipment_event.status_location else null end) as lost_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'update_lpd' then shipment_event.status_location else null end) as update_lpd_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'ready_to_dispatch' then shipment_event.status_location else null end) as ready_to_dispatch_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_scheduled' then shipment_event.status_location else null end) as rvp_scheduled_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_out_for_pickup' then shipment_event.status_location else null end) as rvp_out_for_pickup_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_pickup_completed' then shipment_event.status_location else null end) as rvp_pickup_completed_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_pickup_failed' then shipment_event.status_location else null end) as rvp_pickup_failed_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_cancelled' then shipment_event.status_location else null end) as rvp_cancelled_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_in_scan_at_hub' then shipment_event.status_location else null end) as rvp_in_scan_at_hub_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_in_transit' then shipment_event.status_location else null end) as rvp_in_transit_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_received_at_delivery_hub' then shipment_event.status_location else null end) as rvp_received_at_delivery_hub_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_out_for_delivery' then shipment_event.status_location else null end) as rvp_out_for_delivery_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_undelivered_attempted' then shipment_event.status_location else null end) as rvp_undelivered_attempted_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_completed' then shipment_event.status_location else null end) as rvp_completed_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rto_confirmed' then shipment_event.status_location else null end) as rto_confirmed_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rto_received_at_delivery_hub' then shipment_event.status_location else null end) as rto_received_at_delivery_hub_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rto_out_for_delivery' then shipment_event.status_location else null end) as rto_out_for_delivery_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rto_undelivered_attempted' then shipment_event.status_location else null end) as rto_undelivered_attempted_location,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rto_completed' then shipment_event.status_location else null end) as rto_completed_location,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'out_for_pickup' then shipment_event.status_date_time else null end) as out_for_pickup_last_datetime,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'pickup_failed' then shipment_event.status_date_time else null end) as pickup_failed_last_datetime,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'in_transit' then shipment_event.status_date_time else null end) as in_transit_last_datetime,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'out_for_delivery' then shipment_event.status_date_time else null end) as out_for_delivery_last_datetime,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.status_date_time else null end) as undelivered_attempted_last_datetime,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'rvp_out_for_pickup' then shipment_event.status_date_time else null end) as rvp_out_for_pickup_last_datetime,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'rvp_pickup_failed' then shipment_event.status_date_time else null end) as rvp_pickup_failed_last_datetime,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'rvp_in_transit' then shipment_event.status_date_time else null end) as rvp_in_transit_last_datetime,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'rvp_out_for_delivery' then shipment_event.status_date_time else null end) as rvp_out_for_delivery_last_datetime,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'rvp_undelivered_attempted' then shipment_event.status_date_time else null end) as rvp_undelivered_attempted_last_datetime,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'rto_out_for_delivery' then shipment_event.status_date_time else null end) as rto_out_for_delivery_last_datetime,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'rto_undelivered_attempted' then shipment_event.status_date_time else null end) as rto_undelivered_attempted_last_datetime,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'pickup_failed' then shipment_event.secondary_status else null end) as pickup_failed_last_secondary_status,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.secondary_status else null end) as undelivered_attempted_last_secondary_status,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'rvp_pickup_failed' then shipment_event.secondary_status else null end) as rvp_pickup_failed_last_secondary_status,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'rvp_undelivered_attempted' then shipment_event.secondary_status else null end) as rvp_undelivered_attempted_last_secondary_status,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'rto_undelivered_attempted' then shipment_event.secondary_status else null end) as rto_undelivered_attempted_last_secondary_status,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'pickup_failed' then shipment_event.remarks  else null end) as pickup_failed_last_remarks,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.remarks  else null end) as undelivered_attempted_last_remarks,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'rvp_pickup_failed' then shipment_event.remarks  else null end) as rvp_pickup_failed_last_remarks,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'rvp_undelivered_attempted' then shipment_event.remarks  else null end) as rvp_undelivered_attempted_last_remarks,
min(case when rank_desc = 1  and lower(shipment_event.status) = 'rto_undelivered_attempted' then shipment_event.remarks  else null end) as rto_undelivered_attempted_last_remarks,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'out_for_pickup' then shipment_event.status_date_time else null end) as out_for_pickup_second_datetime,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'pickup_failed' then shipment_event.status_date_time else null end) as pickup_failed_second_datetime,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'out_for_delivery' then shipment_event.status_date_time else null end) as out_for_delivery_second_datetime,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.status_date_time else null end) as undelivered_attempted_second_datetime,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'rvp_out_for_pickup' then shipment_event.status_date_time else null end) as rvp_out_for_pickup_second_datetime,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'rvp_pickup_failed' then shipment_event.status_date_time else null end) as rvp_pickup_failed_second_datetime,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'rvp_out_for_delivery' then shipment_event.status_date_time else null end) as rvp_out_for_delivery_second_datetime,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'rvp_undelivered_attempted' then shipment_event.status_date_time else null end) as rvp_undelivered_attempted_second_datetime,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'rto_out_for_delivery' then shipment_event.status_date_time else null end) as rto_out_for_delivery_second_datetime,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'rto_undelivered_attempted' then shipment_event.status_date_time else null end) as rto_undelivered_attempted_second_datetime,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'pickup_failed' then shipment_event.secondary_status else null end) as pickup_failed_second_secondary_status,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.secondary_status else null end) as undelivered_attempted_second_secondary_status,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'rvp_pickup_failed' then shipment_event.secondary_status else null end) as rvp_pickup_failed_second_secondary_status,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'rvp_undelivered_attempted' then shipment_event.secondary_status else null end) as rvp_undelivered_attempted_second_secondary_status,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'rto_undelivered_attempted' then shipment_event.secondary_status else null end) as rto_undelivered_attempted_second_secondary_status,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'pickup_failed' then shipment_event.remarks else null end) as pickup_failed_second_remarks,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.remarks else null end) as undelivered_attempted_second_remarks,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'rvp_pickup_failed' then shipment_event.remarks else null end) as rvp_pickup_failed_second_remarks,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'rvp_undelivered_attempted' then shipment_event.remarks else null end) as rvp_undelivered_attempted_second_remarks,
min(case when rank_asc = 2  and lower(shipment_event.status) = 'rto_undelivered_attempted' then shipment_event.remarks else null end) as rto_undelivered_attempted_second_remarks,

min (case when rank_asc = 3  and lower(shipment_event.status) = 'out_for_pickup' then shipment_event.status_date_time else null end) as out_for_pickup_third_datetime,
min (case when rank_asc = 3  and lower(shipment_event.status) = 'pickup_failed' then shipment_event.status_date_time else null end) as pickup_failed_third_datetime,
min (case when rank_asc = 3  and lower(shipment_event.status) = 'pickup_failed' then shipment_event.secondary_status  else null end) as pickup_failed_third_secondary_status,
min (case when rank_asc = 4  and lower(shipment_event.status) = 'out_for_pickup' then shipment_event.status_date_time else null end) as out_for_pickup_fourth_datetime,
min (case when rank_asc = 4  and lower(shipment_event.status) = 'pickup_failed' then shipment_event.status_date_time else null end) as pickup_failed_fourth_datetime,
min (case when rank_asc = 4  and lower(shipment_event.status) = 'pickup_failed' then shipment_event.secondary_status  else null end) as pickup_failed_fourth_secondary_status,

min (case when rank_asc = 5  and lower(shipment_event.status) = 'out_for_pickup' then shipment_event.status_date_time else null end) as out_for_pickup_fifth_datetime,
min (case when rank_asc = 5  and lower(shipment_event.status) = 'pickup_failed' then shipment_event.status_date_time else null end) as pickup_failed_fifth_datetime,
min (case when rank_asc = 5  and lower(shipment_event.status) = 'pickup_failed' then shipment_event.secondary_status  else null end) as pickup_failed_fifth_secondary_status,

min (case when rank_asc = 6  and lower(shipment_event.status) = 'out_for_pickup' then shipment_event.status_date_time else null end) as out_for_pickup_sixth_datetime,
min (case when rank_asc = 6  and lower(shipment_event.status) = 'pickup_failed' then shipment_event.status_date_time else null end) as pickup_failed_sixth_datetime,
min (case when rank_asc = 6  and lower(shipment_event.status) = 'pickup_failed' then shipment_event.secondary_status  else null end) as pickup_failed_sixth_secondary_status,

min(case when rank_asc = 3  and lower(shipment_event.status) = 'out_for_delivery' then shipment_event.status_date_time else null end) as out_for_delivery_third_datetime,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'dispatched_to_facility' then shipment_event.status_date_time else null end) as dispatched_to_facility_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'dispatched_to_facility' then shipment_event.status_location else null end) as dispatched_to_facility_first_status_location,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'dispatched_to_merchant' then shipment_event.status_date_time else null end) as dispatched_to_merchant_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'dispatched_to_merchant' then shipment_event.status_location else null end) as dispatched_to_merchant_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'dispatched_to_seller' then shipment_event.status_date_time else null end) as dispatched_to_seller_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'dispatched_to_seller' then shipment_event.status_location else null end) as dispatched_to_seller_first_status_location,


min(case when rank_asc = 1  and lower(shipment_event.status) = 'dispatched_to_tc' then shipment_event.status_date_time else null end) as dispatched_to_tc_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'dispatched_to_tc' then shipment_event.status_location else null end) as dispatched_to_tc_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'dispatched_to_vendor' then shipment_event.status_date_time else null end) as dispatched_to_vendor_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'dispatched_to_vendor' then shipment_event.status_location else null end) as dispatched_to_vendor_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'dispatch_failed' then shipment_event.status_date_time else null end) as dispatch_failed_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'dispatch_failed' then shipment_event.status_location else null end) as dispatch_failed_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'expected' then shipment_event.status_date_time else null end) as expected_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'expected' then shipment_event.status_location else null end) as expected_first_status_location,


min(case when rank_asc = 1  and lower(shipment_event.status) = 'marked_for_merchant_dispatch' then shipment_event.status_date_time else null end) as marked_for_merchant_dispatch_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'marked_for_merchant_dispatch' then shipment_event.status_location else null end) as marked_for_merchant_dispatch_first_status_location,


min(case when rank_asc = 1  and lower(shipment_event.status) = 'marked_for_reshipment' then shipment_event.status_date_time else null end) as marked_for_reshipment_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'marked_for_reshipment' then shipment_event.status_location else null end) as marked_for_reshipment_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'marked_for_seller_return' then shipment_event.status_date_time else null end) as marked_for_seller_return_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'marked_for_seller_return' then shipment_event.status_location else null end) as marked_for_seller_return_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'marked_reshipment_approved' then shipment_event.status_date_time else null end) as marked_reshipment_approved_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'marked_reshipment_approved' then shipment_event.status_location else null end) as marked_reshipment_approved_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'not_received' then shipment_event.status_date_time else null end) as not_received_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'not_received' then shipment_event.status_location else null end) as not_received_first_status_location,




min(case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_complete' then shipment_event.status_date_time else null end) as pickup_complete_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_complete' then shipment_event.status_location else null end) as pickup_complete_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_leg_completed' then shipment_event.status_date_time else null end) as pickup_leg_completed_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_leg_completed' then shipment_event.status_location else null end) as pickup_leg_completed_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_out_for_pickup' then shipment_event.status_date_time else null end) as pickup_out_for_pickup_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_out_for_pickup' then shipment_event.status_location else null end) as pickup_out_for_pickup_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_reattempt' then shipment_event.status_date_time else null end) as pickup_reattempt_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_reattempt' then shipment_event.status_location else null end) as pickup_reattempt_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_scheduled' then shipment_event.status_date_time else null end) as pickup_scheduled_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_scheduled' then shipment_event.status_location else null end) as pickup_scheduled_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'ready_for_pickup' then shipment_event.status_date_time else null end) as ready_for_pickup_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'ready_for_pickup' then shipment_event.status_location else null end) as ready_for_pickup_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'received' then shipment_event.status_date_time else null end) as received_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'received' then shipment_event.status_location else null end) as received_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'received_by_merchant' then shipment_event.status_date_time else null end) as received_by_merchant_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'received_by_merchant' then shipment_event.status_location else null end) as received_by_merchant_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'received_by_seller' then shipment_event.status_date_time else null end) as received_by_seller_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'received_by_seller' then shipment_event.status_location else null end) as received_by_seller_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'received_with_error' then shipment_event.status_date_time else null end) as received_with_error_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'received_with_error' then shipment_event.status_location else null end) as received_with_error_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'request_for_cancellation' then shipment_event.status_date_time else null end) as request_for_cancellation_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'request_for_cancellation' then shipment_event.status_location else null end) as request_for_cancellation_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'request_for_reschedule' then shipment_event.status_date_time else null end) as request_for_reschedule_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'request_for_reschedule' then shipment_event.status_location else null end) as request_for_reschedule_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'reshipped' then shipment_event.status_date_time else null end) as reshipped_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'reshipped' then shipment_event.status_location else null end) as reshipped_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'returned' then shipment_event.status_date_time else null end) as returned_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'returned' then shipment_event.status_location else null end) as returned_first_status_location,


min(case when rank_asc = 1  and lower(shipment_event.status) = 'returned_to_seller' then shipment_event.status_date_time else null end) as returned_to_seller_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'returned_to_seller' then shipment_event.status_location else null end) as returned_to_seller_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'reverse_pickup_delivered' then shipment_event.status_date_time else null end) as reverse_pickup_delivered_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'reverse_pickup_delivered' then shipment_event.status_location else null end) as reverse_pickup_delivered_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'reverse_pickup_scheduled' then shipment_event.status_date_time else null end) as reverse_pickup_scheduled_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'reverse_pickup_scheduled' then shipment_event.status_location else null end) as reverse_pickup_scheduled_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'rto_delivered' then shipment_event.status_date_time else null end) as rto_delivered_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'rto_delivered' then shipment_event.status_location else null end) as rto_delivered_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'scheduled' then shipment_event.status_date_time else null end) as scheduled_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'scheduled' then shipment_event.status_location else null end) as scheduled_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'undelivered' then shipment_event.status_date_time else null end) as undelivered_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'undelivered' then shipment_event.status_location else null end) as undelivered_first_status_location,


min(case when rank_asc = 1  and lower(shipment_event.status) = 'rto_request' then shipment_event.status_date_time else null end) as rto_request_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'rto_request' then shipment_event.status_location else null end) as rto_request_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'undelivered_unattempted' then shipment_event.status_date_time else null end) as undelivered_unattempted_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'undelivered_unattempted' then shipment_event.status_location else null end) as undelivered_unattempted_first_status_location,

min(case when rank_asc = 1  and lower(shipment_event.status) = 'vendor_received' then shipment_event.status_date_time else null end) as vendor_received_first_datetime,
min(case when rank_asc = 1  and lower(shipment_event.status) = 'vendor_received' then shipment_event.status_location else null end) as vendor_received_first_status_location,

min(case when rank_desc = 1  and lower(shipment_event.status) = 'pickup_reattempt' then shipment_event.status_date_time else null end) as pickup_reattempt_last_datetime,

min(case when rank_asc = 2  and lower(shipment_event.status) = 'pickup_reattempt' then shipment_event.status_date_time else null end) as pickup_reattempt_second_datetime,
min(case when rank_asc = 3  and lower(shipment_event.status) = 'pickup_reattempt' then shipment_event.status_date_time else null end) as pickup_reattempt_third_datetime,

min (case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_reattempt' then shipment_event.secondary_status  else null end) as pickup_reattempt_first_secondary_status,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_reattempt' then shipment_event.remarks  else null end) as pickup_reattempt_first_remarks,

min (case when rank_asc = 1  and lower(shipment_event.status) = 'delivered' then shipment_event.status_received_time else null end) as delivered_received_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'pickup_completed' then shipment_event.status_received_time else null end) as pickup_completed_received_datetime,

min (case when rank_asc = 1  and lower(shipment_event.status) = 'rto_handover_completed' then shipment_event.status_received_time else null end) as rto_handover_completed_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_handover_completed' then shipment_event.status_received_time else null end) as rvp_handover_completed_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rto_handover_initiated' then shipment_event.status_received_time else null end) as rto_handover_initiated_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rvp_handover_initiated' then shipment_event.status_received_time else null end) as rvp_handover_initiated_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rto_confirmed' then shipment_event.secondary_status else null end) as rto_confirmed_first_secondary_status,
min (case when rank_asc = 2  and lower(shipment_event.status) = 'rto_confirmed' then shipment_event.secondary_status else null end) as rto_confirmed_second_secondary_status,
min (case when rank_desc = 1  and lower(shipment_event.status) = 'rto_confirmed' then shipment_event.secondary_status else null end) as rto_confirmed_last_secondary_status,

min(case when rank_asc = 4  and lower(shipment_event.status) = 'out_for_delivery' then shipment_event.status_date_time else null end) as out_for_delivery_fourth_datetime,
min(case when rank_asc = 5  and lower(shipment_event.status) = 'out_for_delivery' then shipment_event.status_date_time else null end) as out_for_delivery_fifth_datetime,
min(case when rank_asc = 6  and lower(shipment_event.status) = 'out_for_delivery' then shipment_event.status_date_time else null end) as out_for_delivery_sixth_datetime,
min(case when rank_asc = 7  and lower(shipment_event.status) = 'out_for_delivery' then shipment_event.status_date_time else null end) as out_for_delivery_seventh_datetime,

min (case when rank_asc = 3  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.status_date_time else null end) as undelivered_attempted_third_datetime,
min (case when rank_asc = 4  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.status_date_time else null end) as undelivered_attempted_fourth_datetime,
min (case when rank_asc = 5  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.status_date_time else null end) as undelivered_attempted_fifth_datetime,
min (case when rank_asc = 6  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.status_date_time else null end) as undelivered_attempted_sixth_datetime,
min (case when rank_asc = 7  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.status_date_time else null end) as undelivered_attempted_seventh_datetime,
min (case when rank_asc = 3  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.secondary_status  else null end) as undelivered_attempted_third_secondary_status,
min (case when rank_asc = 4  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.secondary_status  else null end) as undelivered_attempted_fourth_secondary_status,
min (case when rank_asc = 5  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.secondary_status  else null end) as undelivered_attempted_fifth_secondary_status,
min (case when rank_asc = 6  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.secondary_status  else null end) as undelivered_attempted_sixth_secondary_status,
min (case when rank_asc = 7  and lower(shipment_event.status) = 'undelivered_attempted' then shipment_event.secondary_status  else null end) as undelivered_attempted_seventh_secondary_status,
min (case when rank_asc = 2  and lower(shipment_event.status) = 'rto_confirmed' then shipment_event.status_date_time else null end) as rto_confirmed_second_datetime,
min (case when rank_desc = 1  and lower(shipment_event.status) = 'rto_confirmed' then shipment_event.status_date_time else null end) as rto_confirmed_last_datetime,
min (case when rank_asc = 1  and lower(shipment_event.status) = 'rto_cancelled' then shipment_event.status_date_time else null end) as rto_cancelled_first_datetime,
min (case when rank_asc = 2  and lower(shipment_event.status) = 'rto_cancelled' then shipment_event.status_date_time else null end) as rto_cancelled_second_datetime,
min (case when rank_desc = 1  and lower(shipment_event.status) = 'rto_cancelled' then shipment_event.status_date_time else null end) as rto_cancelled_last_datetime


from ( 
select eventid as eventid ,
`data`.associated_sr_id as sr_id,
`data`.handshake_details.received_by as handshake_received_by,
`data`.remarks as remarks,
`data`.secondary_status as secondary_status,
`data`.shipment_reference_ids as shipment_reference_ids,
`data`.status as status,
from_unixtime(UNIX_TIMESTAMP(`data`.status_date_time, "yyyy-MM-dd'T'HH:mm:ss"),"yyyy-MM-dd HH:mm:ss") as status_date_time,
`data`.status_location as status_location,
`data`.status_received_time as status_received_time,
 
 rank() over (partition by `data`.associated_sr_id,`data`.status order by `data`.status_date_time desc) as rank_desc,
 rank() over (partition by `data`.associated_sr_id,`data`.status order by `data`.status_date_time asc) as rank_asc,
 rank() over (partition by `data`.associated_sr_id order by `data`.status_date_time desc) as rank_current_state

from bigfoot_journal.dart_fkint_scp_fulfillment_liteshipmentstatusevent_3 ) shipment_event

group by 
shipment_event.sr_id,
 shipment_event.shipment_reference_ids;
