INSERT overwrite TABLE la_shipment_l0_3PL_fact
SELECT DISTINCT l1_fact.shipment_reference_ids AS shipment_id,
l1_fact.vendor_tracking_id,
l1_fact.shipment_current_status,
l1_fact.shipment_current_status_date_key,
l1_fact.shipment_current_status_time_key,
l1_fact.destination_pincode_key AS destination_geo_id_key,
(CASE
WHEN l1_fact.rto_flag = 1
AND l1_fact.shipment_movement_type = 'Outgoing' THEN 'approved_rto'
WHEN l1_fact.rto_flag = 0
AND l1_fact.shipment_movement_type = 'Outgoing' THEN 'forward'
WHEN l1_fact.shipment_movement_type = 'Incoming' THEN 'rvp'
ELSE NULL
END) AS ekl_shipment_type,
l1_fact.customer_sla_date_key AS customer_promise_date_key,
l1_fact.customer_sla_time_key AS customer_promise_time_key,
l1_fact.design_sla_date_key AS logistics_promise_date_key,
l1_fact.design_sla_time_key AS logistics_promise_time_key,
lookup_date(created_at) AS shipment_created_at_date_key,
lookup_time(created_at) AS shipment_created_at_time_key,
l1_fact.created_at AS shipment_created_at_datetime,
NULL AS vendor_id_key,
l1_fact.vendor_name,
'3PL' AS shipment_carrier,
NULL AS assigned_hub_id,
NULL AS last_current_hub_id,
NULL AS last_current_hub_type,
l1_fact.volumetric_weight AS billable_weight,
NULL AS rvp_origin_geo_id_key,
l1_fact.seller_id_key,
NULL AS pos_id_key,
NULL AS shipment_agent_id_key,
NULL AS transaction_id,
NULL AS amount_collected,
l1_fact.item_seller_type AS seller_type,
l1_fact.item_is_dangerous AS shipment_dg_flag,
NULL AS shipment_fragile_flag,
NULL AS lzn_classification,
NULL AS merchant_id,
l1_fact.shipment_reference_ids AS merchant_reference_id,
NULL AS shipment_first_consignment_id,
NULL AS shipment_last_consignment_id,
NULL AS shipment_first_consignment_create_datetime,
NULL AS shipment_first_consignment_create_date_key,
NULL AS shipment_first_consignment_create_time_key,
NULL AS shipment_last_consignment_create_date_key,
l1_fact.dispatched_to_vendor_first_datetime AS shipment_last_consignment_create_datetime,
NULL AS shipment_last_consignment_eta_in_sec,
NULL AS shipment_last_consignment_eta_datetime,
NULL AS shipment_last_consignment_conn_id,
l1_fact.received_first_datetime AS shipment_inscan_time,
l1_fact.rto_request_first_datetime AS shipment_rto_create_time,
NULL AS last_received_time,
NULL AS number_of_ofd_attempts,
NULL AS assignedhub_expected_time,
NULL AS assignedhub_received_time,
NULL AS returnedtoekl_time,
NULL AS receivedbyekl_time,
NULL AS runsheet_close_datetime,
NULL AS runsheet_close_date_key,
l1_fact.delivered_first_datetime AS ekl_delivery_datetime,
l1_fact.delivered_first_date_key AS ekl_delivery_date_key,
l1_fact.delivered_first_time_key AS ekl_delivery_time_key,
l1_fact.out_for_delivery_first_datetime AS shipment_first_ofd_datetime,
l0_fact.out_for_delivery_last_datetime AS shipment_last_ofd_datetime,
l1_fact.dispatched_to_facility_first_date_key AS vendor_dispatch_datetime,
NULL AS fsd_assigned_hub_sent_datetime,
NULL AS openbox_reject_datetime,
NULL AS first_dh_received_datetime,
l1_fact.rvp_pickup_completed_first_datetime AS rvp_pickup_complete_datetime,
NULL AS shipment_rvp_pk_number_of_attempts,
l1_fact.rto_completed_first_datetime AS end_state_datetime,
NULL AS last_dhhub_sent_datetime,
l1_fact.shipment_value,
l1_fact.amount_to_collect_value AS COD_amount_to_collect,
l1_fact.payment_type AS payment_mode,
dim.ekl_hive_facility_dim_key AS shipment_origin_facility_id_key,
NULL AS rvp_destination_facility_key,
l1_fact.undelivered_attempted_first_secondary_status AS firstundeliverystatus,
NULL AS rvp_hub_id,
NULL AS rvp_hub_id_key,
NULL AS tasklist_tracking_id,
NULL AS ekl_facility_id,
NULL AS last_tasklist_agent_id,
NULL AS last_tasklist_updated_at,
NULL AS tasklist_type,
NULL AS last_tasklist_id,
NULL AS tasklist_status,
NULL AS runsheet_id,
NULL AS last_tasklist_agent_id_key,
NULL AS ekl_facility_id_key,
NULL AS is_cpu_vendor,
NULL AS pincode_location_type,
l1_fact.item_seller_id AS seller_id,
NULL AS pos_id,
NULL AS agent_id,
NULL AS lzn_tat_target,
IF (lookup_date(to_date(cast(rvp_complete_status.rvp_completed_first_datetime as string))) = 0 OR lookup_date(to_date(cast(rvp_complete_status.rvp_completed_first_datetime as string))) IS NULL,rvp_completed_first_date_key,lookup_date(to_date(cast(rvp_complete_status.rvp_completed_first_datetime as string))) ) as rvp_complete_date_key	
FROM bigfoot_external_neo.scp_fulfillment__fulfillment_tpl_shipment_intermediate_fact l1_fact
LEFT JOIN bigfoot_external_neo.scp_fulfillment__fulfillment_liteshipmentstatusevent_base_fact l0_fact ON l0_fact.sr_id = l1_fact.sr_id 
LEFT JOIN (Select `data`.associated_sr_id as sr_id
 from bigfoot_journal.dart_fkint_scp_fulfillment_liteshipmentstatusevent_3
 where `data`.remarks like '%RTO%' and `data`.status = 'created' group by `data`.associated_sr_id)rto_hack
 ON rto_hack.sr_id = l1_fact.sr_id
 LEFT JOIN (Select `data`.associated_sr_id as sr_id, min (`data`.status_date_time) as rvp_completed_first_datetime
 from bigfoot_journal.dart_fkint_scp_fulfillment_liteshipmentstatusevent_3
 where `data`.status IN ('dispatched_to_facility',
						'dispatched_to_merchant',
						'received',
						'rvp_completed',
						'rvp_handover_completed',
						'rvp_handover_initiated',
						'received_by_merchant') 
 group by `data`.associated_sr_id, `data`.shipment_reference_ids)rvp_complete_status
 ON rvp_complete_status.sr_id = l1_fact.sr_id
Left JOIN bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim dim
ON l1_fact.facility_name = dim.name
WHERE UPPER(l1_fact.facility_name) like '%LAR%';
