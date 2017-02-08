INSERT overwrite TABLE la_shipment_l0_journal_fact
SELECT
A.entityid as entityid, 	
cast(A.shipment_inscan_time as TIMESTAMP) as shipment_inscan_time,
cast(A.shipment_rto_create_time as TIMESTAMP) AS shipment_rto_create_time,
cast(A.fsd_last_received_time as TIMESTAMP) AS fsd_last_received_time,
A.fsd_number_of_ofd_attempts AS fsd_number_of_ofd_attempts,
cast(A.fsd_assignedhub_expected_time as TIMESTAMP) as fsd_assignedhub_expected_time,
cast(A.fsd_assignedhub_received_time as TIMESTAMP) as fsd_assignedhub_received_time,
cast(A.fsd_returnedtoekl_time as TIMESTAMP) AS fsd_returnedtoekl_time,
cast(A.fsd_receivedbyekl_time as TIMESTAMP) AS fsd_receivedbyekl_time,
cast(A.runsheet_close_datetime as TIMESTAMP) AS runsheet_close_datetime,
cast(A.ekl_delivery_datetime as TIMESTAMP) AS ekl_delivery_datetime,
cast(A.shipment_first_ofd_datetime as TIMESTAMP) AS shipment_first_ofd_datetime,
cast(A.shipment_last_ofd_datetime as TIMESTAMP) AS shipment_last_ofd_datetime,
cast(A.vendor_dispatch_datetime as TIMESTAMP) AS vendor_dispatch_datetime,
cast(A.fsd_assigned_hub_sent_datetime as TIMESTAMP) AS fsd_assigned_hub_sent_datetime,
cast(A.openbox_reject_datetime as TIMESTAMP) AS openbox_reject_datetime,
cast(A.received_at_source_facility as TIMESTAMP) AS received_at_source_facility,
cast(A.fsd_first_dh_received_datetime as TIMESTAMP) AS fsd_first_dh_received_datetime,
cast(A.rvp_pickup_complete_datetime as TIMESTAMP) AS rvp_pickup_complete_datetime,
A.shipment_rvp_pk_number_of_attempts AS shipment_rvp_pk_number_of_attempts,
cast(A.end_state_datetime as TIMESTAMP) AS end_state_datetime,
cast(A.fsd_last_dhhub_sent_datetime as TIMESTAMP) AS fsd_last_dhhub_sent_datetime,
A.shipment_value AS shipment_value,
A.cod_amount_to_collect AS cod_amount_to_collect,
A.payment_mode AS payment_mode,
A.fsd_assigned_hub_id.col2 AS fsd_assigned_hub_id,
A.source_facility_id AS source_facility_id,
A.destination_address_pincode AS destination_address_pincode,
A.fsd_firstundeliverystatus.col2 AS fsd_firstundeliverystatus,
A.rvp_hub_id AS rvp_hub_id,
lzn.lzn_classification as lzn_classification,
lzn.target as lzn_tat_target,
cast(A.rvp_complete_datetime as TIMESTAMP) as rvp_complete_datetime,
b2clog.vas_type as vas_type
FROM
(
SELECT
entityid as entityid,
min(if(lower(`data`.STATUS) = 'received' AND upper(`data`.current_address.type) IN ('FKL_FACILITY','MOTHER_HUB'),struct(`shipment`.updatedat,`shipment`.`data`.updated_at),null)).col2 as shipment_inscan_time,
min(IF(`data`.shipment_type LIKE '%rto%',from_utc_timestamp(updatedat, 'GMT'),NULL)) AS shipment_rto_create_time,	
max(IF(`data`.STATUS = 'Received',from_unixtime(unix_timestamp(`data`.updated_at)),NULL)) AS fsd_last_received_time,		
count(DISTINCT IF (`data`.STATUS IN ('Out_For_Delivery'),CONCAT (`data`.STATUS,to_date(from_unixtime(unix_timestamp(`data`.updated_at)))),NULL )) AS fsd_number_of_ofd_attempts,
min(IF(`data`.current_address.id = `data`.assigned_address.id AND `data`.STATUS = 'Expected',struct(`shipment`.updatedat,`shipment`.`data`.updated_at),null)).col2 as fsd_assignedhub_expected_time,
min(IF(`data`.current_address.id = `data`.assigned_address.id AND `data`.STATUS = 'Received',struct(`shipment`.updatedat,`shipment`.`data`.updated_at),null)).col2 as fsd_assignedhub_received_time,
min(IF(`data`.STATUS = 'Returned_To_Ekl',struct(`shipment`.updatedat,`shipment`.`data`.updated_at),null)).col2 as fsd_returnedtoekl_time,
min(IF(`data`.STATUS = 'Received_By_Ekl',struct(`shipment`.updatedat,`shipment`.`data`.updated_at),null)).col2 as fsd_receivedbyekl_time,
min(IF(lower(`data`.STATUS) IN ('delivered','delivery_update'),from_utc_timestamp(updatedat, 'GMT'),NULL )) AS runsheet_close_datetime,
min(IF(lower(`data`.STATUS) IN ('delivered','delivery_update'),struct(`shipment`.updatedat,`shipment`.`data`.updated_at),null)).col2 as ekl_delivery_datetime,
min(IF(`data`.STATUS = 'Out_For_Delivery',struct(`shipment`.updatedat,`shipment`.`data`.updated_at),null)).col2 as shipment_first_ofd_datetime,
max(IF(`data`.STATUS = 'Out_For_Delivery',struct(`shipment`.updatedat,`shipment`.`data`.updated_at),null)).col2 as shipment_last_ofd_datetime,
min(IF(`data`.STATUS = 'dispatched_to_vendor',struct(`shipment`.updatedat,`shipment`.`data`.updated_at),null)).col2 as vendor_dispatch_datetime,
min(IF(`data`.current_address.id = `data`.assigned_address.id AND `data`.STATUS = 'Sent',struct(`shipment`.updatedat,`shipment`.`data`.updated_at),null)).col2 as fsd_assigned_hub_sent_datetime,
min(IF(lower(`data`.STATUS) = 'undelivered_order_rejected_opendelivery',struct(`shipment`.updatedat,`shipment`.`data`.updated_at),null)).col2 as openbox_reject_datetime,
min(IF(`data`.STATUS = 'Expected',struct(`shipment`.updatedat,`shipment`.`data`.updated_at),null)).col2 as received_at_source_facility,
min(IF(`data`.STATUS IN ( 'Received','Undelivered_Not_Attended','Error') AND `data`.current_address.type IN ( 'DELIVERY_HUB','BULK_HUB'),struct(`shipment`.updatedat,`shipment`.`data`.updated_at),null)).col2 as fsd_first_dh_received_datetime,
min(IF(`data`.STATUS = 'PICKUP_Picked_Complete',struct(`shipment`.updatedat,`shipment`.`data`.updated_at),null)).col2 as rvp_pickup_complete_datetime,
count(DISTINCT IF ( `data`.STATUS = 'PICKUP_Out_For_Pickup',CONCAT(`data`.STATUS,to_date(from_unixtime(unix_timestamp(`data`.updated_at)))),NULL )) AS shipment_rvp_pk_number_of_attempts,
max(IF(lower(`data`.STATUS) IN ( 'lost','not_received','reshipped','received_by_merchant','returned_to_seller','delivered' ),struct(`shipment`.updatedat,`shipment`.`data`.updated_at),null)).col2 as end_state_datetime,
max(IF(`data`.STATUS = 'Sent'
AND `data`.current_address.type IN ( 'DELIVERY_HUB','BULK_HUB' ),struct(`shipment`.updatedat,`shipment`.`data`.updated_at),IF(`data`.STATUS = 'Expected'
AND `data`.current_address.type IN ( 'FKL_FACILITY','MOTHER_HUB' ),struct(`shipment`.updatedat,`shipment`.`data`.updated_at),null))).col2 AS fsd_last_dhhub_sent_datetime,
min(`data`.value.value) AS shipment_value,
min(`data`.amount_to_collect.value) AS cod_amount_to_collect,
max(`data`.payment.payment_details.mode [0]) AS payment_mode,
min(if(lower(`data`.status) IN ('inscan_success','received','error','undelivered_not_attended','pickup_addedtopickupsheet') and `data`.shipment_type = 'forward' and `data`.assigned_address.id is not null,struct(`shipment`.updatedat,struct(`data`.current_address.id ,`data`.assigned_address.id)),null)).col2 as fsd_assigned_hub_id, 	
max(if(lower(`data`.status) IN ('expected') and `data`.current_address.type = 'FKL_FACILITY',struct(`shipment`.updatedat,`shipment`.`data`.current_address.id),null )).col2 as source_facility_id,
max(if(lower(`data`.status) IN ('expected') and `data`.current_address.type = 'FKL_FACILITY',struct(`shipment`.updatedat,`shipment`.`data`.destination_address.pincode),null )).col2 as destination_address_pincode,
min(if(lower(`data`.status) IN 
('undelivered_customer_not_available',
'undelivered_door_lock',
'undelivered_holiday',
'undelivered_cod_not_ready',
'undelivered_misroute',
'undelivered_shipment_damage',
'undelivered_order_rejected_by_customer',
'undelivered_no_response',
'undelivered_incomplete_address',
'undelivered_invalid_time_frame',
'undelivered_heavy_traffic',
'undelivered_vehicle_breakdown',
'undelivered_security_instability',
'undelivered_shipment_on_hold',
'undelivered_address_not_found',
'undelivered_heavy_rain',
'undelivered_order_rejected_opendelivery',
'undelivered_for_consolidation',
'undelivered_heavyload',
'undelivered_request_for_reschedule',
'undelivered_outofdeliveryarea',
'undelivered_nonserviceablepincode',
'undelivered_samecitymisroute',
'undelivered_othercitymisroute',
'undelivered_untraceablefromhub',
'undelivered_pickup_cancelled',
'undelivered_pickup_time_elapsed',
'undelivered_pickup_others'),struct(`shipment`.updatedat,struct(`shipment`.updatedat,`shipment`.`data`.status )),null)).col2 as fsd_firstundeliverystatus,
min(if(lower(`data`.STATUS) IN ('expected'),`data`.current_address.id, NULL)) AS rvp_hub_id,
min(IF(lower(`data`.status) IN ('returned_to_seller', 'received_by_merchant','received_by_ekl','delivered', 'delivery_update'),updatedat,NULL)) AS rvp_complete_datetime
FROM bigfoot_journal.dart_wsr_scp_ekl_shipment_4 shipment where DAY > #240#DAY#
GROUP BY entityid) A
LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim ekl_dim1 ON A.source_facility_id= ekl_dim1.facility_id
LEFT OUTER JOIN bigfoot_common.la_lzn_classification lzn ON ( A.destination_address_pincode = lzn.destination_pincode
                                                              AND ekl_dim1.postal_code = lzn.source_pincode AND lzn.shipment_carrier = 'FSD' AND lzn.tat = 'forward')
LEFT OUTER JOIN 
                ( SELECT refid  AS shipment_id,
						 max(`data`.vas_ids[0])  AS vas_type
                         FROM bigfoot_journal.dart_wsr_scp_ekl_b2clogisticsrequest_1 
						 lateral VIEW explode(`data`.ekl_reference_ids) reference_id AS refid
                         WHERE  day > date_format(date_sub(CURRENT_DATE,240),'yyyyMMdd') 
                         GROUP BY refid
				) b2clog
  ON  (A.entityid = b2clog.shipment_id) ;
