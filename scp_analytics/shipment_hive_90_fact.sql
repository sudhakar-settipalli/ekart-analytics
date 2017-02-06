INSERT OVERWRITE TABLE shipment_hive_90_fact
SELECT DISTINCT Shipment.shipment_id AS shipment_id,
Shipment.vendor_tracking_id AS vendor_tracking_id,
if(Shipment.ekl_shipment_type IN ('merchant_return'),MR.vendor_tracking_id,NULL) AS original_tracking_id,
Shipment.merchant_reference_id AS merchant_reference_id,
Shipment.merchant_id AS merchant_id,
Shipment.seller_id AS seller_id,
OrderItem.order_id AS order_id,
OrderItem.order_external_id AS order_external_id,
runtask.document_id AS actioned_runsheet_id,
Shipment.shipment_current_status AS shipment_current_status,
Shipment.first_undelivery_status AS first_undelivery_status,
Shipment.last_undelivery_status AS last_undelivery_status,
--PendingLocationMapping.rule AS RULE,
NULL as RULE,
Return.refund_status AS refund_status,
Return.return_reason AS return_reason,
Return.return_sub_reason AS return_sub_reason,
Return.refund_reason AS refund_reason,
Return.return_action AS return_action,
Return.return_status AS return_status,
Return.refund_mode AS refund_mode,
Return.return_type AS return_type,
Return.reject_reason AS reject_reason,
Return.reject_sub_reason AS reject_sub_reason,
Shipment.payment_type AS payment_type,
Shipment.payment_mode AS payment_mode,
Shipment.transaction_id AS transaction_id,
Shipment.amount_collected AS amount_collected,
Shipment.seller_type AS seller_type,
Shipment.packing_box_type AS packing_box_type,
Shipment.item_quantity AS item_quantity,
Shipment.shipping_category AS shipping_category,
Shipment.shipment_dg_flag AS shipment_dg_flag,
Shipment.shipment_flash_flag AS shipment_flash_flag,
Shipment.shipment_fragile_flag AS shipment_fragile_flag,
Shipment.shipment_priority_flag AS shipment_priority_flag,
Shipment.service_tier AS service_tier,
Shipment.surface_mandatory_flag AS surface_mandatory_flag,
if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.mp_pickup_compliance,MP.mp_pickup_compliance)

--  MP.mp_pickup_compliance 
AS mp_pickup_compliance,

Shipment.shipment_size_flag AS shipment_size_flag,
Shipment.ekl_shipment_type AS ekl_shipment_type,
Shipment.reverse_shipment_type AS reverse_shipment_type,
Tracking.national_hop_breach_score AS national_hop_breach_score,
Tracking.number_of_hops AS number_of_hops,
Tracking.number_of_air_hops AS number_of_air_hops,
Tracking.line_haul_breach_score AS line_haul_breach_score,
Tracking.number_of_ftl_hops AS number_of_ftl_hops,
Tracking.tc_connection_breach_score AS tc_connection_breach_score,
Tracking.number_of_offloads AS number_of_offloads,
Tracking.air_hop_breach_score AS air_hop_breach_score,
breach.dh_first_bag_received_datetime AS actual_route_map,
If(lower(Shipment.shipment_current_status) = 'lost','03 Lost', If(Shipment.ekl_shipment_type = 'forward','01 Forward','02 Reverse')) AS shipment_direction,
Shipment.ekl_fin_zone AS ekl_fin_zone,
Shipment.ekart_lzn_flag AS ekart_lzn_flag,
Shipment.shipment_fa_flag AS shipment_fa_flag,
Shipment.shipment_carrier AS shipment_carrier,
Shipment.shipment_pending_flag AS shipment_pending_flag,
NULL AS goods_recon_pending_flag,
Shipment.shipment_num_of_pk_attempt AS shipment_num_of_pk_attempt,
--Shipment.fsd_number_of_ofd_attempts AS fsd_number_of_ofd_attempts,
runtask.task_type as fsd_number_of_ofd_attempts,
Shipment.shipment_rvp_pk_number_of_attempts AS shipment_rvp_pk_number_of_attempts,
Shipment.shipment_weight AS shipment_weight,
Shipment.sender_weight AS sender_weight,
Shipment.system_weight AS system_weight,
Shipment.volumetric_weight_source AS volumetric_weight_source,
Shipment.billable_weight AS billable_weight,
Shipment.billable_weight_type AS billable_weight_type,
Shipment.cost_of_breach AS cost_of_breach,
Shipment.shipment_value AS shipment_value,
Shipment.cod_amount_to_collect AS cod_amount_to_collect,
Shipment.shipment_charge AS shipment_charge,
lookupkey('vendor_id',Shipment.vendor_id) AS vendor_id_key,
lookupkey('device_id',CAST(SPLIT(Shipment.pos_id,"-")[1] AS INT)) AS pos_id_key,
lookupkey('seller_id',Shipment.seller_id) AS seller_id_key,
lookupkey('product_id',fsn_table.fsn_id) AS primary_product_key,
lookupkey('agent_id',Shipment.agent_id) AS agent_id_key,
lookupkey('pincode',Shipment.source_address_pincode) AS source_pincode_key,
lookupkey('pincode',Shipment.destination_address_pincode) AS destination_pincode_key,
lookupkey('address_id',Shipment.customer_address_id) AS customer_address_id_key,
Shipment.customer_address_id AS customer_address_id,
If(service.facility_id IS NOT NULL,'shared','independent') AS vendor_service_type,
Shipment.cs_notes AS cs_notes,
Shipment.hub_notes AS hub_notes,
NULL as shipment_pending_age,
(UNIX_TIMESTAMP() - UNIX_TIMESTAMP(Shipment.shipment_last_receive_datetime))/3600 AS asset_pending_age,

NULL AS accountable_function_pending_age_in_days,
NULL AS recon_pending_age_in_days,
NULL AS goods_recon_l1_flag,
NULL AS goods_recon_l2_flag,
NULL AS goods_recon_l3_flag,
NULL AS accountable_function,
NULL AS shipment_pending_location,
NULL AS pending_status,
NULL AS goods_recon_l2_bucket,
NULL AS goods_recon_l3_bucket,
NULL AS return_bucket,
NULL AS return_sub_bucket,
CAST(breach.customer_dependency_flag AS int) AS customer_dependency_flag,
bags.shipment_first_bag_tracking_id AS shipment_first_bag_tracking_id,
bags.shipment_last_bag_tracking_id AS shipment_last_bag_tracking_id,
cons.shipment_first_consignment_id AS shipment_first_consignment_id,
cons.shipment_last_consignment_id AS shipment_last_consignment_id,
bags.shipment_last_bag_status AS shipment_last_bag_status,
cons.shipment_first_consignment_type AS shipment_first_consignment_type,
cons.shipment_first_consignment_mode AS shipment_first_consignment_mode,
cons.shipment_first_consignment_awb_number AS shipment_first_consignment_awb_number,
cons.shipment_first_consignment_movement_flag AS shipment_first_consignment_movement_flag,
cons.shipment_first_consignment_co_loader AS shipment_first_consignment_co_loader,
cons.shipment_last_consignment_status AS shipment_last_consignment_status,
cons.shipment_last_consignment_type AS shipment_last_consignment_type,
cons.shipment_last_consignment_mode AS shipment_last_consignment_mode,
cons.shipment_last_consignment_awb_number AS shipment_last_consignment_awb_number,
cons.shipment_last_consignment_movement_flag AS shipment_last_consignment_movement_flag,
cons.shipment_last_consignment_co_loader AS shipment_last_consignment_co_loader,
CAST(cons.shipment_last_consignment_breach_flag AS int)AS shipment_last_consignment_breach_flag,
CAST(breach.mh_breach_flag AS int) AS mh_breach_flag,
CAST(breach.mh_connection_breach_flag AS int) AS mh_connection_breach_flag,
CAST(breach.first_mile_breach_flag AS int) AS first_mile_breach_flag,
CAST(breach.tpt_breach_flag AS int) AS tpt_breach_flag,
CAST(breach.tpt_breach_score AS int) AS tpt_breach_score,
CAST(breach.last_mile_breach_flag AS int) AS last_mile_breach_flag,
CAST(breach.misroute_flag AS int) AS misroute_flag,
CAST(breach.customer_misroute_flag AS int) AS customer_misroute_flag,
Shipment.profiler_flag AS profiler_flag,
breach.logistics_fulfillment_bucket AS logistics_fulfillment_bucket,
breach.logistics_breach_bucket AS logistics_breach_bucket,
breach.customer_fulfillment_bucket AS customer_fulfillment_bucket,
breach.customer_breach_bucket AS customer_breach_bucket,
lookupkey('facility_id',Shipment.shipment_origin_facility_id) AS shipment_origin_facility_id_key,
lookupkey('facility_id',Shipment.shipment_origin_mh_facility_id) AS shipment_origin_mh_facility_id_key,
lookupkey('facility_id',Shipment.shipment_destination_mh_facility_id) AS shipment_destination_mh_facility_id_key,
lookupkey('facility_id',Shipment.shipment_destination_facility_id) AS shipment_destination_facility_id_key,
lookupkey('facility_id',Shipment.shipment_first_received_dh_id) AS shipment_first_received_dh_id_key,
lookupkey('facility_id',Shipment.shipment_last_received_dh_id) AS shipment_last_received_dh_id_key,
lookupkey('facility_id',Shipment.shipment_first_received_pc_id) AS shipment_first_received_pc_id_key,
lookupkey('facility_id',Shipment.shipment_last_received_pc_id)AS shipment_last_received_pc_id_key,
lookupkey('facility_id',Shipment.shipment_first_received_hub_id) AS shipment_first_received_hub_id_key,
lookupkey('facility_id',Shipment.rto_received_hub_id) AS rto_received_hub_id_key,
lookupkey('facility_id',Shipment.shipment_last_received_hub_id) AS shipment_last_received_hub_id_key,
lookupkey('facility_id',Shipment.shipment_first_received_mh_id) AS shipment_first_received_mh_id_key,
lookupkey('facility_id',Shipment.shipment_last_received_mh_id) AS shipment_last_received_mh_id_key,
lookupkey('facility_id',Shipment.fsd_assigned_hub_id) AS fsd_assigned_hub_id_key,
lookupkey('facility_id',Shipment.reverse_pickup_hub_id) AS reverse_pickup_hub_id_key,
If(Shipment.shipment_carrier = '3PL',lookupkey('facility_id',service.facility_id),lookupkey('facility_id',Shipment.fsd_assigned_hub_id)) AS shipment_ideal_dh_key,
lookupkey('facility_id',Shipment.shipment_current_hub_id) AS shipment_current_hub_id_key,
Shipment.shipment_current_hub_type AS shipment_current_hub_type,


if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.mp_pickup_hub_key,MP.mp_pickup_hub_key) AS mp_pickup_hub_key,


lookupkey('facility_id',cons.shipment_last_consignment_source_hub_id) AS shipment_last_consignment_source_hub_id_key,
lookupkey('facility_id',cons.shipment_first_consignment_destination_hub_id) AS shipment_first_consignment_destination_hub_id_key,
lookupkey('facility_id',cons.shipment_last_consignment_destination_hub_id) AS shipment_last_consignment_destination_hub_id_key,
lookupkey('facility_id',bags.shipment_first_bag_created_hub_id) AS shipment_first_bag_created_hub_id_key,
lookupkey('facility_id',bags.shipment_first_bag_final_hub_id) AS shipment_first_bag_final_hub_id_key,
lookupkey('facility_id',bags.shipment_last_bag_created_hub_id) AS shipment_last_bag_created_hub_id_key,
lookupkey('facility_id',bags.shipment_last_bag_hub_id) AS shipment_last_bag_final_hub_id_key,
lookupkey('facility_id',bags.shipment_last_bag_hub_id) AS shipment_last_bag_hub_id_key,
-- lookupkey('facility_id',If(PendingLocationMapping.pending_status LIKE '%BRSNR%',bags.shipment_last_bag_created_hub_id,If(Shipment.shipment_last_received_hub_id IS NULL,Shipment.shipment_current_hub_id,Shipment.shipment_last_received_hub_id))) AS accountable_hub_id_key,
lookupkey('facility_id',If(Shipment.shipment_last_received_hub_id IS NULL,Shipment.shipment_current_hub_id,Shipment.shipment_last_received_hub_id)) AS accountable_hub_id_key,
lookupkey('facility_id',Shipment.profiled_hub_id) AS profiled_hub_id_key,
If(If(Shipment.shipment_delivered_at_datetime IS NOT NULL,cast(unix_timestamp(Shipment.shipment_delivered_at_datetime)/1000 AS INT),unix_timestamp()) > unix_timestamp(Shipment.customer_promise_datetime),1,0) AS customer_promise_breach,
If(If(Shipment.shipment_delivered_at_datetime IS NOT NULL,cast(unix_timestamp(Shipment.shipment_delivered_at_datetime)/1000 AS INT),unix_timestamp()) > unix_timestamp(Shipment.logistics_promise_datetime),1,0) AS logistics_promise_breach,

-- lookup_date(if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
-- AND Shipment.shipment_carrier='3PL'
-- AND Shipment.seller_type='Non-FA',MPN.mp_first_out_for_pickup_date,MP.mp_first_out_for_pickup_date) ) 
if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.mp_first_out_for_pickup_date_key,MP.mp_first_out_for_pickup_date_key) 

--  lookup_date(MP.mp_first_out_for_pickup_date) 
AS shipment_first_out_for_pickup_date_key,

-- lookup_time(if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
-- AND Shipment.shipment_carrier='3PL'
-- AND Shipment.seller_type='Non-FA',MPN.mp_first_out_for_pickup_date,MP.mp_first_out_for_pickup_date) ) 
if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.mp_first_out_for_pickup_time_key,MP.mp_first_out_for_pickup_time_key) 

--  lookup_time(MP.mp_first_out_for_pickup_date) 
AS shipment_first_out_for_pickup_time_key,

-- lookup_date(if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
-- AND Shipment.shipment_carrier='3PL'
-- AND Shipment.seller_type='Non-FA',MPN.mp_last_out_for_pickup_date,MP.mp_last_out_for_pickup_date) ) 
lookup_date(if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.mp_last_out_for_pickup_date_time,MP.mp_last_out_for_pickup_date_time)) 

--  lookup_date(MP.mp_last_out_for_pickup_date) 
AS shipment_last_out_for_pickup_date_key,

-- lookup_time(if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
-- AND Shipment.shipment_carrier='3PL'
-- AND Shipment.seller_type='Non-FA',MPN.mp_last_out_for_pickup_date,MP.mp_last_out_for_pickup_date) ) 
lookup_time(if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.mp_last_out_for_pickup_date_time,MP.mp_last_out_for_pickup_date_time) ) 

--  lookup_time(MP.mp_last_out_for_pickup_date) 
AS shipment_last_out_for_pickup_time_key,

if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.mp_received_at_origin_date_key,MP.mp_received_at_origin_date_key) 

--  MP.mp_received_at_origin_date_key 
AS shipment_received_at_origin_date_key,

if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.mp_received_at_origin_time_key,MP.mp_received_at_origin_time_key) 

--  MP.mp_received_at_origin_time_key 
AS shipment_received_at_origin_time_key,


if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.mp_dispatched_to_tc_date_key,MP.mp_dispatched_to_tc_date_key) 

--  MP.mp_dispatched_to_tc_date_key 
AS shipment_dispatched_to_tc_date_key,

if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.mp_dispatched_to_tc_time_key,MP.mp_dispatched_to_tc_time_key) 

--  MP.mp_dispatched_to_tc_time_key 
AS shipment_dispatched_to_tc_time_key,

if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.mp_received_at_tc_date_key,MP.mp_received_at_tc_date_key) 

--  MP.mp_received_at_tc_date_key 
AS shipment_received_at_tc_date_key,


if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.mp_received_at_tc_time_key,MP.mp_received_at_tc_time_key) 
--  MP.mp_received_at_tc_time_key 
AS shipment_received_at_tc_time_key,



lookup_date(Shipment.shipment_created_at_datetime) AS shipment_created_at_date_key,
lookup_time(Shipment.shipment_created_at_datetime) AS shipment_created_at_time_key,
lookup_date(Shipment.shipment_dispatch_datetime) AS shipment_dispatch_date_key,
lookup_time(Shipment.shipment_dispatch_datetime) AS shipment_dispatch_time_key,
lookup_date(Shipment.shipment_first_received_at_datetime) AS shipment_first_received_date_key,
lookup_time(Shipment.shipment_first_received_at_datetime) AS shipment_first_received_time_key,
lookup_date(Shipment.shipment_delivered_at_datetime) AS shipment_delivered_at_date_key,
lookup_time(Shipment.shipment_delivered_at_datetime) AS shipment_delivered_at_time_key,
lookup_date(Shipment.shipment_last_delivered_at_datetime) AS shipment_last_delivered_at_date_key,--new column v1
lookup_time(Shipment.shipment_last_delivered_at_datetime) AS shipment_last_delivered_at_time_key,--new column v1
lookup_date(Shipment.shipment_first_delivery_update_datetime) AS shipment_first_delivery_update_date_key,
lookup_time(Shipment.shipment_first_delivery_update_datetime) AS shipment_first_delivery_update_time_key,
lookup_date(Shipment.shipment_last_delivery_update_datetime) AS shipment_last_delivery_update_date_key,
lookup_time(Shipment.shipment_last_delivery_update_datetime) AS shipment_last_delivery_update_time_key,
lookup_date(Shipment.shipment_first_dispatched_to_merchant_datetime) AS shipment_first_dispatched_to_merchant_date_key,
lookup_time(Shipment.shipment_first_dispatched_to_merchant_datetime) AS shipment_first_dispatched_to_merchant_time_key,
lookup_date(Shipment.shipment_current_status_datetime) AS fsd_last_update_date_key,
lookup_time(Shipment.shipment_current_status_datetime) AS fsd_last_update_time_key,
lookup_date(Shipment.shipment_last_receive_datetime) AS fsd_last_receive_date_key,
lookup_time(Shipment.shipment_last_receive_datetime) AS fsd_last_receive_time_key,
lookup_date(Shipment.customer_promise_datetime) AS customer_promise_date_key,
lookup_time(Shipment.customer_promise_datetime) AS customer_promise_time_key,
lookup_date(Shipment.logistics_promise_datetime) AS logistics_promise_date_key,
lookup_time(Shipment.logistics_promise_datetime) AS logistics_promise_time_key,
lookup_date(Shipment.new_customer_promise_datetime) AS new_customer_promise_date_key,
lookup_time(Shipment.new_customer_promise_datetime) AS new_customer_promise_time_key,
lookup_date(Shipment.new_logistics_promise_datetime) AS new_logistics_promise_date_key,
lookup_time(Shipment.new_logistics_promise_datetime) AS new_logistics_promise_time_key,
lookup_date(shipment.vendor_dispatch_datetime) AS ekl_vendor_dispatch_date_key,
lookup_time(shipment.vendor_dispatch_datetime) AS ekl_vendor_dispatch_time_key,
lookup_date(Shipment.fsd_first_dh_received_datetime) AS fsd_first_dh_received_date_key,
lookup_time(Shipment.fsd_first_dh_received_datetime) AS fsd_first_dh_received_time_key,
lookup_date(Shipment.fsd_last_dh_received_datetime) AS fsd_last_dh_received_date_key,
lookup_time(Shipment.fsd_last_dh_received_datetime) AS fsd_last_dh_received_time_key,
lookup_date(Shipment.shipment_first_received_pc_datetime) AS shipment_first_received_pc_date_key,
lookup_time(Shipment.shipment_first_received_pc_datetime) shipment_first_received_pc_time_key,
lookup_date(Shipment.shipment_last_received_pc_datetime) AS shipment_lat_received_pc_date_key,
lookup_time(Shipment.shipment_last_received_pc_datetime) shipment_last_received_pc_time_key,
lookup_date(Shipment.fsd_first_ofd_datetime) AS fsd_first_ofd_date_key,
lookup_time(Shipment.fsd_first_ofd_datetime) AS fsd_first_ofd_time_key,
lookup_date(Shipment.fsd_last_ofd_datetime) AS fsd_last_ofd_date_key,
lookup_time(Shipment.fsd_last_ofd_datetime) AS fsd_last_ofd_time_key,
lookup_date(Shipment.shipment_first_rfp_datetime) AS shipment_first_rfp_date_key,
lookup_time(Shipment.shipment_first_rfp_datetime) AS shipment_first_rfp_time_key,
lookup_date(Shipment.shipment_last_rfp_datetime) AS shipment_last_rfp_date_key,
lookup_time(Shipment.shipment_last_rfp_datetime) AS shipment_last_rfp_time_key,
lookup_date(Shipment.shipment_first_picksheet_creation_time) AS shipment_first_picksheet_creation_date_key,--new col from l0
lookup_time(Shipment.shipment_first_picksheet_creation_time) AS shipment_first_picksheet_creation_time_key,--new col from l0
lookup_date(Shipment.shipment_last_picksheet_creation_time) AS shipment_last_picksheet_creation_date_key,--new col from l0
lookup_time(Shipment.shipment_last_picksheet_creation_time) AS shipment_last_picksheet_creation_time_key,--new col from l0
lookup_date(Shipment.shipment_first_rvp_pickup_time) AS shipment_first_rvp_pickup_date_key,
lookup_time(Shipment.shipment_first_rvp_pickup_time) AS shipment_first_rvp_pickup_time_key,
lookup_date(Shipment.shipment_last_rvp_pickup_time) AS shipment_last_rvp_pickup_date_key,
lookup_time(Shipment.shipment_last_rvp_pickup_time) AS shipment_last_rvp_pickup_time_key,

if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.mp_requested_date_key,MP.mp_requested_date_key)

--  MP.mp_requested_date_key 
AS mp_requested_date_key,

if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.mp_requested_time_key,MP.mp_requested_time_key)

--  MP.mp_requested_time_key 
AS mp_requested_time_key,

if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.mp_day,MP.mp_day)

--  MP.mp_day 
AS mp_day,

if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.mp_reqested_date_hour,MP.mp_reqested_date_hour)

--  MP.reqested_hour 
AS reqested_hour,

-- lookup_date(if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
-- AND Shipment.shipment_carrier='3PL'
-- AND Shipment.seller_type='Non-FA',MPN.mp_pickup_promise_date,MP.mp_pickup_promise_date) ) 
if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.new_promise_key,MP.new_promise_key)

--  lookup_date(MP.mp_pickup_promise_date) 
AS mp_pickup_promise_date_key,

-- lookup_time(if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
-- AND Shipment.shipment_carrier='3PL'
-- AND Shipment.seller_type='Non-FA',MPN.mp_pickup_promise_date,MP.mp_pickup_promise_date) )
if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.mp_pickup_promise_time_key,MP.mp_pickup_promise_time_key)

--  lookup_time(MP.mp_pickup_promise_date) 
AS mp_pickup_promise_time_key,

lookup_date(Shipment.shipment_current_status_datetime) AS shipment_current_status_date_key,
lookup_time(Shipment.shipment_current_status_datetime) AS shipment_current_status_time_key,
lookup_date(IF(Shipment.ekl_shipment_type='rvp',Return.order_date_time,OrderItem.order_item_date)) AS order_item_date_key,
lookup_time(IF(Shipment.ekl_shipment_type='rvp',Return.order_date_time,OrderItem.order_item_date)) AS order_item_time_key,
--shipment_speed_vas
lookup_date(order_speed.order_item_approve_sla_date_time) AS order_item_approve_sla_date_key,
lookup_time(order_speed.order_item_approve_sla_date_time) AS order_item_approve_sla_time_key,
lookup_date(order_speed.order_item_dispatch_by_sla_date_time) AS order_item_dispatch_by_sla_date_key,
lookup_time(order_speed.order_item_dispatch_by_sla_date_time) AS order_item_dispatch_by_sla_time_key,
lookup_date(order_speed.order_item_ship_by_sla_date_time) AS order_item_ship_by_sla_date_key,
lookup_time(order_speed.order_item_ship_by_sla_date_time) AS order_item_ship_by_sla_time_key,
order_speed.order_item_promise_sla_date_key as order_item_promise_sla_date_key,
cast(2359 as INT) AS order_item_promise_sla_time_key,
lookup_date(order_speed.fulfill_item_rtd_after_sla_date_time) AS order_item_rtd_after_sla_date_key,
lookup_time(order_speed.fulfill_item_rtd_after_sla_date_time) AS order_item_rtd_after_sla_time_key,
lookup_date(order_speed.fulfill_item_rtd_actual_o2d_date_time) AS order_item_rtd_actual_o2d_date_key,
lookup_time(order_speed.fulfill_item_rtd_actual_o2d_date_time) AS order_item_rtd_actual_o2d_time_key,
lookup_date(order_speed.fulfill_item_dispatch_o2d_date_time) AS order_item_dispatch_o2d_date_key,
lookup_time(order_speed.fulfill_item_dispatch_o2d_date_time) AS order_item_dispatch_o2d_time_key,
lookup_date(order_speed.fulfill_item_ship_o2d_date_time) AS order_item_ship_o2d_date_key,
lookup_time(order_speed.fulfill_item_ship_o2d_date_time) AS order_item_ship_o2d_time_key,
lookup_date(order_speed.fulfill_item_deliver_o2d_date_time) AS order_item_deliver_o2d_date_key,
lookup_time(order_speed.fulfill_item_deliver_o2d_date_time) AS order_item_deliver_o2d_time_key,
--shipment_speed_vas_end
lookup_date(bags.shipment_first_bag_create_datetime) AS shipment_first_bag_create_date_key,
lookup_time(bags.shipment_first_bag_create_datetime) AS shipment_first_bag_create_time_key,
lookup_date(bags.shipment_first_bag_closed_datetime) AS shipment_first_bag_closed_date_key,
lookup_time(bags.shipment_first_bag_closed_datetime) AS shipment_first_bag_closed_time_key,
lookup_date(bags.shipment_first_bag_reach_datetime) AS shipment_first_bag_reach_date_key,
lookup_time(bags.shipment_first_bag_reach_datetime) AS shipment_first_bag_reach_time_key,
lookup_date(bags.shipment_first_bag_receive_datetime) AS shipment_first_bag_final_received_date_key,
lookup_time(bags.shipment_first_bag_receive_datetime) AS shipment_first_bag_final_received_time_key,
lookup_date(bags.shipment_last_bag_create_datetime) AS shipment_last_bag_create_date_key,
lookup_time(bags.shipment_last_bag_create_datetime) AS shipment_last_bag_create_time_key,
lookup_date(bags.shipment_last_bag_close_datetime) AS shipment_last_bag_close_date_key,
lookup_time(bags.shipment_last_bag_close_datetime) AS shipment_last_bag_close_time_key,
lookup_date(bags.shipment_last_bag_reach_datetime) AS shipment_last_bag_reach_date_key,
lookup_time(bags.shipment_last_bag_reach_datetime) AS shipment_last_bag_reach_time_key,
lookup_date(bags.shipment_last_bag_receive_datetime) AS shipment_last_bag_receive_date_key,
lookup_time(bags.shipment_last_bag_receive_datetime) AS shipment_last_bag_receive_time_key,
cons.shipment_first_consignment_create_date_key AS shipment_first_consignment_create_date_key,
cons.shipment_first_consignment_create_time_key AS shipment_first_consignment_create_time_key,
cons.shipment_first_consignment_receive_date_key AS shipment_first_consignment_receive_date_key,
cons.shipment_first_consignment_receive_time_key AS shipment_first_consignment_receive_time_key,
cons.shipment_last_consignment_create_date_key AS shipment_last_consignment_create_date_key,
cons.shipment_last_consignment_create_time_key AS shipment_last_consignment_create_time_key,
cons.shipment_last_consignment_eta_date_key AS shipment_last_consignment_eta_date_key,
cons.shipment_last_consignment_eta_time_key AS shipment_last_consignment_eta_time_key,
cons.shipment_last_consignment_receive_date_key AS shipment_last_consignment_receive_date_key,
cons.shipment_last_consignment_receive_time_key AS shipment_last_consignment_receive_time_key,
lookup_date(Shipment.rto_first_received_datetime) AS rto_first_received_date_key,
lookup_time(Shipment.rto_first_received_datetime) AS rto_first_received_time_key,
lookup_date(Shipment.rto_create_datetime) AS rto_create_date_key,
lookup_time(Shipment.rto_create_datetime) AS rto_create_time_key,
lookup_date(Shipment.rto_complete_datetime) AS rto_complete_date_key,
lookup_time(Shipment.rto_complete_datetime) AS rto_complete_time_key,
lookup_date(Shipment.tpl_first_ofd_datetime) AS tpl_first_ofd_date_key,
lookup_time(Shipment.tpl_first_ofd_datetime) AS tpl_first_ofd_time_key,
lookup_date(Shipment.tpl_last_ofd_datetime) AS tpl_last_ofd_date_key,
lookup_time(Shipment.tpl_last_ofd_datetime) AS tpl_last_ofd_time_key,
lookup_date(Shipment.first_mh_tc_receive_datetime) AS first_mh_tc_receive_date_key,
lookup_time(Shipment.first_mh_tc_receive_datetime) AS first_mh_tc_receive_time_key,
lookup_date(Shipment.last_mh_tc_receive_datetime) AS last_mh_tc_receive_date_key,
lookup_time(Shipment.last_mh_tc_receive_datetime) AS last_mh_tc_receive_time_key,
lookup_date(Shipment.first_mh_tc_outscan_datetime) AS first_mh_tc_outscan_date_key,
lookup_time(Shipment.first_mh_tc_outscan_datetime) AS first_mh_tc_outscan_time_key,
lookup_date(Tracking.last_mh_tc_outscan_datetime) AS last_mh_tc_outscan_date_key,
lookup_time(Tracking.last_mh_tc_outscan_datetime) AS last_mh_tc_outscan_time_key,
lookup_date(Tracking.first_dh_outscan_datetime) AS first_dh_outscan_date_key,
lookup_time(Tracking.first_dh_outscan_datetime) AS first_dh_outscan_time_key,
lookup_date(Tracking.last_dh_outscan_datetime) AS last_dh_outscan_date_key,
lookup_time(Tracking.last_dh_outscan_datetime) AS last_dh_outscan_time_key,
lookup_date(If(Shipment.ekl_shipment_type LIKE '%rto%',Shipment.rto_create_datetime, If(Shipment.ekl_shipment_type = 'merchant_return'
AND MR.merchant_reference_id IS NOT NULL, If(MR.ekl_shipment_type LIKE '%rto%',MR.rto_create_datetime,MR.shipment_created_at_datetime), Shipment.shipment_created_at_datetime))) AS shipment_recon_start_date_key,
lookup_time(If(Shipment.ekl_shipment_type LIKE '%rto%',Shipment.rto_create_datetime, If(Shipment.ekl_shipment_type = 'merchant_return'
AND MR.merchant_reference_id IS NOT NULL, If(MR.ekl_shipment_type LIKE '%rto%',MR.rto_create_datetime,MR.shipment_created_at_datetime), Shipment.shipment_created_at_datetime))) AS shipment_recon_start_time_key,
lookupkey('facility_id',breach.ideal_first_hop_id) AS ideal_first_destination_id_key,
lookup_time(timestamp(from_unixtime(breach.ideal_connection_cutoff-19800))) AS ideal_connection_cutoff_time_key,
lookup_time(timestamp(from_unixtime(breach.actual_connection_cutoff-19800))) AS actual_connection_cutoff_time_key,
runtask.agent_id_key AS fe_name_key,
lookup_date(runtask.tasklist_completion_datetime) AS actual_rvp_pickup_date_key,
lookup_time(runtask.tasklist_completion_datetime) AS actual_rvp_pickup_time_key,
NULL AS scheduled_rvp_pickup_date_key,
NULL AS scheduled_rvp_pickup_time_key,
--New Columns from FM Promise Facts
if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.mp_num_of_pk_attempts,MP.mp_num_of_pk_attempts) 
AS shipment_mp_num_of_pk_attempts,
if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.no_seller_reattempt,MP.no_seller_reattempt) 
AS shipment_num_seller_reattempt,
if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.no_of_ekl_reattempt,MP.no_of_ekl_reattempt) 
AS shipment_num_of_ekl_reattempt,

if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.bag_receive_at_mh_date_key,MP.bag_receive_at_mh_date_key) 
AS shipment_bag_receive_at_mh_date_key,

if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.bag_receive_at_mh_time_key,MP.bag_receive_at_mh_time_key) 
AS shipment_bag_receive_at_mh_time_key,

if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.consignment_receive_at_mh_date_key,MP.consignment_receive_at_mh_date_key) 
AS shipment_consignment_receive_at_mh_date_key,

if(Shipment.ekl_shipment_type IN ('forward','approved_rto','unapproved_rto')
AND Shipment.shipment_carrier='3PL'
AND Shipment.seller_type='Non-FA',MPN.consignment_receive_at_mh_time_key,MP.consignment_receive_at_mh_time_key) 
AS shipment_consignment_receive_at_mh_time_key,
--New Columns from speed vas fact
order_speed.fulfill_item_unit_reserve_actual_date_key as order_item_reserve_actual_date_key,
order_speed.fulfill_item_unit_reserve_actual_time_key as order_item_reserve_actual_time_key,
Shipment.shipment_item_unit_dispatch_service_tier as shipment_item_unit_dispatch_service_tier,
lookup_date(Shipment.shipment_dispatch_by_datetime) AS shipment_dispatch_by_date_key,
lookup_time(Shipment.shipment_dispatch_by_datetime) AS shipment_dispatch_by_time_key,
NULL AS core_fsn_flag,
Shipment.shipment_lzn_classification as shipment_lzn_classification,
Shipment.shipment_transit_distance as shipment_transit_distance,
Shipment.shipment_transit_time as shipment_transit_time,
lookup_date(bags.shipment_first_dh_bag_reach_datetime) AS shipment_first_dh_bag_reach_date_key,
lookup_time(bags.shipment_first_dh_bag_reach_datetime) AS shipment_first_dh_bag_reach_time_key,
lookup_date(bags.shipment_last_dh_bag_reach_datetime) AS shipment_last_dh_bag_reach_date_key,
lookup_time(bags.shipment_last_dh_bag_reach_datetime) AS shipment_last_dh_bag_reach_time_key,
lookup_date(Shipment.reverse_complete_datetime) as reverse_complete_date_key,
lookup_time(Shipment.reverse_complete_datetime) as reverse_complete_time_key,
lookup_date(Shipment.pickup_slot_start_datetime) as pickup_slot_start_date_key,
lookup_time(Shipment.pickup_slot_start_datetime) as pickup_slot_start_time_key,
lookup_date(Shipment.pickup_slot_end_datetime) as pickup_slot_end_date_key,
lookup_time(Shipment.pickup_slot_end_datetime) as pickup_slot_end_time_key,
rto_first_undelivery_status,
lookup_date(Shipment.first_undelivery_status_datetime) as first_undelivery_status_date_key,
lookup_time(Shipment.first_undelivery_status_datetime) as first_undelivery_status_time_key,
lookup_date(Shipment.last_undelivery_status_datetime) as last_undelivery_status_date_key,
lookup_time(Shipment.last_undelivery_status_datetime) as last_undelivery_status_time_key,
lookup_date(Shipment.rto_first_undelivery_status_datetime) as rto_first_undelivery_status_date_key,
lookup_time(Shipment.rto_first_undelivery_status_datetime) as rto_first_undelivery_status_time_key,
lookup_date(IF(Shipment.ekl_shipment_type='rvp' and reverse_shipment_type in ('PREXO', 'Replacement'), Shipment.fsd_first_dh_received_datetime, 
IF(Shipment.ekl_shipment_type in ('unapproved_rto', 'approved_rto'), Shipment.rto_create_datetime, Shipment.shipment_created_at_datetime)
)
)as shipment_start_date_key,
lookup_time(IF(Shipment.ekl_shipment_type='rvp' and reverse_shipment_type in ('PREXO', 'Replacement'), Shipment.fsd_first_dh_received_datetime, 
IF(Shipment.ekl_shipment_type in ('unapproved_rto', 'approved_rto'), Shipment.rto_create_datetime, Shipment.shipment_created_at_datetime)
)
) as shipment_start_time_key,
Shipment.rto_num_ofd_attempts as rto_num_ofd_attempts,
Shipment.shipment_contour_volume as shipment_contour_volume,
lookup_date(myn_logstcs_outscn_datetime) as myn_logstcs_outscn_date_key,
lookup_time(myn_logstcs_outscn_datetime) as myn_logstcs_outscn_time_key,
Tracking.sort_resort_flag as sort_resort_flag,
Tracking.misroute_score as misroute_score,
Tracking.missort_score as missort_score,
null resort_count,
OBD.delivery_type as obd_delivery_type,
OBD.flyer_id as obd_flyer_id,
OBD.flyer_status as obd_flyer_status
FROM
(SELECT shipment_id,
first_associated_shipment_id,
vendor_tracking_id,
merchant_reference_id,
merchant_id,
seller_id,
shipment_current_status,
payment_type,
payment_mode,
pos_id,
transaction_id,
agent_id,
amount_collected,
seller_type,
packing_box_type,
shipment_dg_flag,
shipment_flash_flag,
shipment_fragile_flag,
shipment_priority_flag,
service_tier,
surface_mandatory_flag,
shipment_size_flag,
item_quantity,
shipping_category,
ekl_shipment_type,
reverse_shipment_type,
first_undelivery_status,
last_undelivery_status,
profiler_flag,
ekl_fin_zone,
ekart_lzn_flag,
shipment_fa_flag,
vendor_id,
shipment_carrier,
shipment_pending_flag,
shipment_num_of_pk_attempt,
fsd_number_of_ofd_attempts,
shipment_rvp_pk_number_of_attempts,
shipment_weight,
sender_weight,
system_weight,
volumetric_weight_source,
volumetric_weight,
billable_weight,
billable_weight_type,
cost_of_breach,
shipment_value,
cod_amount_to_collect,
shipment_charge,
source_address_pincode,
destination_address_pincode,
customer_address_id,
cs_notes,
hub_notes,
fsd_assigned_hub_id,
reverse_pickup_hub_id,
shipment_current_hub_id,
shipment_first_received_hub_id,
shipment_last_received_hub_id,
shipment_first_received_mh_id,
shipment_last_received_mh_id,
shipment_origin_mh_facility_id,
shipment_destination_mh_facility_id,
shipment_origin_facility_id,
shipment_destination_facility_id,
shipment_first_received_dh_id,
shipment_last_received_dh_id,
shipment_first_received_pc_id,
shipment_last_received_pc_id,
rto_received_hub_id,
profiled_hub_id,
shipment_current_hub_type,
CAST (shipment_created_at_datetime AS TIMESTAMP) AS shipment_created_at_datetime, 
CAST (shipment_dispatch_datetime AS TIMESTAMP) AS shipment_dispatch_datetime,
CAST (vendor_dispatch_datetime AS TIMESTAMP) AS vendor_dispatch_datetime,
CAST (shipment_current_status_datetime AS TIMESTAMP) AS shipment_current_status_datetime,
CAST (shipment_first_received_at_datetime AS TIMESTAMP) AS shipment_first_received_at_datetime,
CAST (shipment_delivered_at_datetime AS TIMESTAMP) AS shipment_delivered_at_datetime,
CAST (shipment_last_delivered_at_datetime AS TIMESTAMP) AS shipment_last_delivered_at_datetime,--new column from l1
CAST (shipment_first_delivery_update_datetime AS TIMESTAMP) AS shipment_first_delivery_update_datetime,
CAST (shipment_last_delivery_update_datetime AS TIMESTAMP) AS shipment_last_delivery_update_datetime,
CAST(shipment_first_dispatched_to_merchant_datetime AS TIMESTAMP) AS shipment_first_dispatched_to_merchant_datetime,
CAST (logistics_promise_datetime AS TIMESTAMP) AS logistics_promise_datetime,
CAST (shipment_actual_sla_datetime AS TIMESTAMP) AS shipment_actual_sla_datetime,
CAST (customer_promise_datetime AS TIMESTAMP) AS customer_promise_datetime,
CAST (new_logistics_promise_datetime AS TIMESTAMP) AS new_logistics_promise_datetime,
CAST (new_customer_promise_datetime AS TIMESTAMP) AS new_customer_promise_datetime,
CAST (shipment_last_receive_datetime AS TIMESTAMP) AS shipment_last_receive_datetime,
CAST (fsd_first_dh_received_datetime AS TIMESTAMP) AS fsd_first_dh_received_datetime,
CAST (fsd_last_dh_received_datetime AS TIMESTAMP) AS fsd_last_dh_received_datetime,
CAST(shipment_first_received_pc_datetime AS TIMESTAMP) AS shipment_first_received_pc_datetime,
CAST(shipment_last_received_pc_datetime AS TIMESTAMP) AS shipment_last_received_pc_datetime,
CAST (fsd_first_ofd_datetime AS TIMESTAMP) AS fsd_first_ofd_datetime,
CAST (fsd_last_ofd_datetime AS TIMESTAMP) AS fsd_last_ofd_datetime,
CAST(shipment_first_rfp_datetime AS TIMESTAMP) AS shipment_first_rfp_datetime,
CAST(shipment_last_rfp_datetime AS TIMESTAMP) AS shipment_last_rfp_datetime,
CAST(shipment_first_picksheet_creation_time AS TIMESTAMP) AS shipment_first_picksheet_creation_time,--new column from l1
CAST(shipment_last_picksheet_creation_time AS TIMESTAMP) AS shipment_last_picksheet_creation_time,--new column from l1
CAST (shipment_first_rvp_pickup_time AS TIMESTAMP) AS shipment_first_rvp_pickup_time,
CAST (shipment_last_rvp_pickup_time AS TIMESTAMP) AS shipment_last_rvp_pickup_time,
CAST (received_at_origin_facility_datetime AS TIMESTAMP) AS received_at_origin_facility_datetime,
CAST (rto_first_received_datetime AS TIMESTAMP) AS rto_first_received_datetime,
CAST (rto_create_datetime AS TIMESTAMP) AS rto_create_datetime,
CAST (rto_complete_datetime AS TIMESTAMP) AS rto_complete_datetime,
CAST (tpl_first_ofd_datetime AS TIMESTAMP) AS tpl_first_ofd_datetime,
CAST (tpl_last_ofd_datetime AS TIMESTAMP) AS tpl_last_ofd_datetime,
CAST (first_mh_tc_receive_datetime AS TIMESTAMP) AS first_mh_tc_receive_datetime,
CAST (last_mh_tc_receive_datetime AS TIMESTAMP) AS last_mh_tc_receive_datetime,
CAST (first_mh_tc_outscan_datetime AS TIMESTAMP) AS first_mh_tc_outscan_datetime,
CAST (last_mh_tc_outscan_datetime AS TIMESTAMP) AS last_mh_tc_outscan_datetime,
CAST (first_dh_outscan_datetime AS TIMESTAMP) AS first_dh_outscan_datetime,
CAST (last_dh_outscan_datetime AS TIMESTAMP) AS last_dh_outscan_datetime,
shipment_dispatch_service_tier as shipment_item_unit_dispatch_service_tier,
CAST (shipment_dispatch_by_datetime AS TIMESTAMP) AS shipment_dispatch_by_datetime,
shipment_lzn_classification,
shipment_transit_distance,
shipment_transit_time,
reverse_complete_datetime,
CAST(pickup_slot_start_datetime AS TIMESTAMP) AS pickup_slot_start_datetime,
CAST(pickup_slot_end_datetime AS TIMESTAMP) AS pickup_slot_end_datetime,
rto_first_undelivery_status,
CAST(first_undelivery_status_datetime AS TIMESTAMP) AS first_undelivery_status_datetime,
CAST(last_undelivery_status_datetime AS TIMESTAMP) AS last_undelivery_status_datetime,
CAST(rto_first_undelivery_status_datetime as TIMESTAMP) AS rto_first_undelivery_status_datetime,
rto_num_ofd_attempts,
shipment_contour_volume,
cast(myn_logstcs_outscn_datetime as timestamp) as myn_logstcs_outscn_datetime
FROM bigfoot_external_neo.scp_ekl__shipment_l1_90_fact) Shipment

LEFT OUTER JOIN
bigfoot_external_neo.scp_ekl__first_mile_retruns_hive_fact OBD 
ON shipment.vendor_tracking_id = OBD.returntrackingid

LEFT OUTER JOIN
(SELECT vendor_tracking_id,
national_hop_breach_score,
number_of_hops,
number_of_air_hops,
line_haul_breach_score,
number_of_ftl_hops,
tc_connection_breach_score,
number_of_offloads,
air_hop_breach_score,
actual_route_map,
CAST (first_dh_outscan_datetime AS TIMESTAMP) AS first_dh_outscan_datetime,
CAST(last_mh_tc_outscan_datetime AS TIMESTAMP) AS last_mh_tc_outscan_datetime,
CAST(last_dh_outscan_datetime AS TIMESTAMP) AS last_dh_outscan_datetime,
sort_resort_flag,
misroute_score,
missort_score
FROM bigfoot_external_neo.scp_ekl__tracking_shipment_intermediate_hive_fact) Tracking 
ON Shipment.vendor_tracking_id=Tracking.vendor_tracking_id


LEFT OUTER JOIN
(SELECT merchant_reference_id,
vendor_tracking_id,
ekl_shipment_type,
CAST(shipment_created_at_datetime AS TIMESTAMP) AS shipment_created_at_datetime,
CAST(rto_create_datetime AS TIMESTAMP) AS rto_create_datetime
FROM bigfoot_external_neo.scp_ekl__shipment_l1_90_fact
WHERE ekl_shipment_type NOT IN ('forward','merchant_return')) MR 
ON (MR.merchant_reference_id = Shipment.merchant_reference_id)


-- LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__mp_ekl_hive_fact MP 
-- ON (MP.tracking_id = Shipment.vendor_tracking_id
-- AND MP.tracking_id NOT IN ('not_assigned'))

LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__first_mile_new_promise_hive_fact MP
ON (MP.tracking_id = Shipment.vendor_tracking_id
AND MP.tracking_id NOT IN ('not_assigned'))


LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__shipment_first_last_consignment_map_l1_90_fact cons 
ON (Shipment.vendor_tracking_id = cons.vendor_tracking_id
AND cons.vendor_tracking_id NOT IN ('not_assigned'))


LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__shipment_first_last_bag_map_l1_90_fact bags 
ON (Shipment.vendor_tracking_id = bags.vendor_tracking_id
AND bags.vendor_tracking_id NOT IN ('not_assigned'))


LEFT OUTER JOIN
(SELECT pincode,
max(facility_id) AS facility_id
FROM bigfoot_external_neo.scp_ekl__shipment_facility_id_pincode_map_l1_fact
WHERE hub_type<>'BULK_HUB'
GROUP BY pincode) service ON (Shipment.destination_address_pincode = service.pincode)-- AND service.hub_type <> 'BULK_HUB'
--AND If(Shipment.shipment_size_flag = 'bulk','BULK_HUB','DELIVERY_HUB') = service.hub_type)


LEFT OUTER JOIN
(SELECT order_item_unit_shipment_id,
order_id,
order_external_id,
CAST(order_item_date AS TIMESTAMP) AS order_item_date
FROM bigfoot_external_neo.scp_ekl__first_order_shipment_map_l1_90_fact
WHERE order_item_unit_shipment_id IS NOT NULL) OrderItem 
ON (OrderItem.order_item_unit_shipment_id = shipment.merchant_reference_id)

LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim LRH 
ON Shipment.shipment_last_received_hub_id = LRH.facility_id


LEFT OUTER JOIN 
(SELECT return_item_shipment_id,
refund_status,return_reason,return_sub_reason,refund_reason,return_action,
return_status,refund_mode,return_type,reject_reason,reject_sub_reason,return_item_id,
order_item_id, CAST(order_date_time as timestamp) as order_date_time
from bigfoot_external_neo.scp_ekl__return_shipments_reason_l1_hive_fact) RETURN
ON (RETURN.return_item_shipment_id=Shipment.vendor_tracking_id
AND Shipment.ekl_shipment_type='rvp'
AND RETURN.return_item_shipment_id IS NOT NULL
AND RETURN.return_item_shipment_id<>'not_assigned')


-- LEFT OUTER JOIN bigfoot_common.shipment_pending_location_rule_book_v_2 PendingLocationMapping 
-- ON (Upper(CONCAT(If(Shipment.shipment_fa_flag IS NULL,'NULL',If(Shipment.shipment_fa_flag=1,'TRUE','FALSE')),'-', iF(Shipment.shipment_carrier IS NULL,'NULL',Shipment.shipment_carrier),'-', If(Shipment.fsd_number_of_ofd_attempts > 0,'Attempted',If(Shipment.shipment_current_status = 'undelivered_attempted'
-- AND Shipment.shipment_carrier = '3PL','Attempted','Unattempted')),'-', If(Shipment.ekl_shipment_type IS NULL,'NULL',Shipment.ekl_shipment_type),'-',If(Shipment.reverse_shipment_type IS NULL,'NULL',Shipment.reverse_shipment_type),'-',Shipment.shipment_current_status,'-',If(bags.shipment_last_bag_status IS NULL,'NULL',bags.shipment_last_bag_status),'-', If(cons.shipment_last_consignment_status IS NULL,'NULL',cons.shipment_last_consignment_status),'-',If(Shipment.shipment_current_hub_id <> If(Shipment.ekl_shipment_type = 'rvp',Shipment.shipment_destination_mh_facility_id,Shipment.shipment_origin_mh_facility_id)
-- AND Shipment.shipment_current_hub_type = 'MOTHER_HUB','TRANSPORT_CENTER',Shipment.shipment_current_hub_type),'-', If(bags.shipment_last_bag_created_hub_id <> If(Shipment.ekl_shipment_type = 'rvp',Shipment.shipment_destination_mh_facility_id,Shipment.shipment_origin_mh_facility_id)
-- AND bags.shipment_last_bag_created_hub_type = 'MOTHER_HUB','TRANSPORT_CENTER',If(bags.shipment_last_bag_created_hub_type IS NULL,'NULL',bags.shipment_last_bag_created_hub_type)),'-', If(Shipment.shipment_last_received_hub_id <> If(Shipment.ekl_shipment_type = 'rvp',Shipment.shipment_destination_mh_facility_id,Shipment.shipment_origin_mh_facility_id)
-- AND LRH.type = 'MOTHER_HUB','TRANSPORT_CENTER',If(LRH.type IS NULL,'NULL',LRH.type)),'-',If(RETURN.refund_status IS NULL,'NULL',RETURN.refund_status),'-',If(RETURN.return_status IS NULL,'NULL',RETURN.return_status))) = PendingLocationMapping.key)

LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__logistics_breach_funnel_90_fact breach 
ON Shipment.vendor_tracking_id=breach.vendor_tracking_id
AND breach.vendor_tracking_id NOT IN ('not_assigned')

LEFT OUTER JOIN
bigfoot_external_neo.scp_ekl__runsheet_task_mapping_fact runtask 
ON (runtask.shipment_id=Shipment.vendor_tracking_id and runtask.tasklistid = 1)

-- LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__mp_ekl_hive_fact MPN 
-- ON (MPN.shipment_id = Shipment.first_associated_shipment_id
-- AND Shipment.first_associated_shipment_id IS NOT NULL)

LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__first_mile_new_promise_hive_fact MPN
ON (MPN.shipment_id = Shipment.first_associated_shipment_id
AND Shipment.first_associated_shipment_id IS NOT NULL)

LEFT OUTER JOIN
(SELECT a.shipment_id,
a.maxim AS maxim_v,
min(b.product_id) AS fsn_id
FROM
(SELECT shipment_id,
max(item_value) AS maxim,
min(item_quantity) AS minqty
FROM bigfoot_external_neo.scp_ekl__shipment_item_l1_90_fact
GROUP BY shipment_id) a
INNER JOIN bigfoot_external_neo.scp_ekl__shipment_item_l1_90_fact b ON a.shipment_id=b.shipment_id
AND a.maxim=b.item_value
AND a.minqty = b.item_quantity
GROUP BY a.shipment_id,
a.maxim) fsn_table ON (Shipment.shipment_id=fsn_table.shipment_id
AND Shipment.shipment_id IS NOT NULL)

LEFT OUTER JOIN 
bigfoot_external_neo.scp_ekl__shipment_speed_vas_hive_v2_fact order_speed 
on (order_speed.order_merchant_reference_id <=> Shipment.merchant_reference_id and order_speed.order_vendor_tracking_id <=> Shipment.vendor_tracking_id);
