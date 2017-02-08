INSERT overwrite TABLE la_shipment_l0_snapshot_fact
SELECT sv1.entityid AS entityid,
sv1.`data`.vendor_tracking_id AS vendor_tracking_id,
sv1.`data`.STATUS AS shipment_current_status,
sv1.updatedat AS updatedat,
sv1.`data`.payment_type AS payment_type,
sv1.`data`.shipment_type AS shipment_type,
sv1.`data`.customer_sla AS customer_sla,
sv1.`data`.design_sla AS design_sla,
sv1.`data`.created_at AS created_at,
sv1.`data`.vendor_id AS vendor_id,
sv1.`data`.assigned_address.id AS fsd_assigned_hub_id,
sv1.`data`.current_address.id AS fsd_last_current_hub_id,
sv1.`data`.current_address.type AS fsd_last_current_hub_type,
sv1.`data`.billable_weight AS ekl_billable_weight,
sv1.`data`.source_address.id AS source_address_id,
sv1.`data`.source_address.pincode AS rvp_origin_geo_id,
sv1.`data`.destination_address.id AS destination_address_id,
sv1.`data`.shipment_items [0].seller_id AS seller_id, 
sv1.`data`.payment.payment_details [0].device_id AS pos_id, 
sv1.`data`.payment.payment_details [0].transaction_id AS transaction_id, 
sv1.`data`.payment.payment_details [0].agent_id AS agent_id,
sv1.`data`.payment.amount_collected.value AS amount_collected,
sv1.`data`.source_address.type AS source_address_type,
sv1.`data`.destination_address.type AS destination_address_type,
sv1.`data`.source_address.type AS seller_type,
concat_ws("-",sv1.`data`.attributes) AS sv_attribute,
IF (sv1.`data`.shipment_type = 'rvp',sv1.`data`.source_address.pincode,sv1.`data`.destination_address.pincode) AS location_pincode,
IF (sv1.`data`.shipment_type = 'rvp',sv1.`data`.destination_address.pincode,sv1.`data`.source_address.pincode) AS source_address_pincode,
IF (sv1.`data`.shipment_type = 'rvp',sv1.`data`.source_address.pincode,sv1.`data`.destination_address.pincode) AS destination_address_pincode,
ekl_dim.postal_code as ekl_source_pincode_key,
lzn.lzn_classification as lzn_classification,
lzn.target as lzn_tat_target
FROM bigfoot_snapshot.dart_wsr_scp_ekl_shipment_4_view_total sv1
LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim ekl_dim ON sv1.`data`.source_address.id= ekl_dim.facility_id
LEFT OUTER JOIN bigfoot_common.la_lzn_classification lzn ON (IF (sv1.`data`.shipment_type = 'rvp',sv1.`data`.source_address.pincode,sv1.`data`.destination_address.pincode) = lzn.destination_pincode AND ekl_dim.postal_code = lzn.source_pincode AND lzn.shipment_carrier = 'FSD' AND lzn.tat = 'forward')
WHERE (sv1.`data`.source_address.id IN (563,564 ,565 ,566 ,567 ,1280 ,1282 ,1288 ,1511 ,1757 ,1950 ,2043 ,3594 ,3612 ,3620 ,3621 ,3622 ,3623)
OR sv1.`data`.destination_address.id IN (563 ,564 ,565 ,566 ,567 ,1280 ,1282 ,1288 ,1511, 1757 ,1950 ,2043 ,3594 ,3612 ,3620 ,3621 ,3622 ,3623))
AND (sv1.`data`.vendor_id IN (200 ,207 ,242) OR sv1.`data`.vendor_id = '');
