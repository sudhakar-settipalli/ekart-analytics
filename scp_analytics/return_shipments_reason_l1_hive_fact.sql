INSERT OVERWRITE TABLE return_shipments_reason_l1_hive_fact
Select 
	RR.return_item_shipment_id as return_item_shipment_id,
	--min(RE.`data`.status) 
	-- refund_status is made as null because source team stopped ingesting data into the column dart_fkint_scp_rrr_refund_1_view.'return_refund_id'
	NULL as refund_status, 
	max(RR.return_reason) as return_reason,
	max(RR.return_sub_reason) as return_sub_reason,
	max(RR.refund_reason) AS refund_reason,
	max(RR.return_action) as return_action,
	min(RR.return_item_status) as return_status,
	max(RR.refund_mode) as refund_mode,
	max(RR.return_type) as return_type,
	max(RR.reject_reason) as reject_reason,
	max(RR.reject_sub_reason) as reject_sub_reason,
	max(RR.return_item_id) AS return_item_id,
	max(RR.order_item_id) AS order_item_id,
	max(RR.order_date_time) AS order_date_time
FROM bigfoot_external_neo.scp_rrr__return_l2_id_level_hive_fact RR 
-- LEFT OUTER JOIN bigfoot_snapshot.dart_fkint_scp_rrr_refund_1_view RE 
-- ON RR.return_refund_id=RE.`data`.refund_id
WHERE trim(RR.return_item_shipment_id) NOT IN ('') 
	and RR.return_item_shipment_id is not null
GROUP BY RR.return_item_shipment_id
