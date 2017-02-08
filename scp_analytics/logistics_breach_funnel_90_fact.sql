INSERT OVERWRITE TABLE logistics_breach_funnel_90_fact
SELECT DISTINCT
S.vendor_tracking_id,
S.merchant_reference_id,
S.shipment_origin_facility_id,
S.shipment_origin_mh_facility_id,
S.fsd_assigned_hub_id,
S.shipment_carrier,
cast(S.shipment_dg_flag as int),
lookup_date(S.fsd_first_ofd_datetime),
lookup_time(S.fsd_first_ofd_datetime),
lookup_date(S.fsd_last_ofd_datetime),
lookup_time(S.fsd_last_ofd_datetime),
lookup_date(S.tpl_first_ofd_datetime),
lookup_time(S.tpl_first_ofd_datetime),
lookup_date(S.shipment_delivered_at_datetime),
lookup_time(S.shipment_delivered_at_datetime),
S.shipment_num_of_pk_attempt,
lookup_date(S.logistics_promise_datetime),
lookup_time(S.logistics_promise_datetime),
mh_tc_breach.mh_breach_flag,
null as ideal_connection_cutoff,
null as actual_connection_cutoff,
null as ideal_first_hop_id,
null as actual_first_hop_id,
mh_tc_breach.tc_connection_breach_flag AS mh_connection_breach_flag,
FM.breach_flag as first_mile_breach_flag,
TPT.breach_flag as tpt_breach_flag,
TPT.breach_score as tpt_breach_score,
LM.breach_flag as last_mile_breach_flag,
FFM.Breach_Flag as flash_last_mile_breach_flag,
M.misroute_flag,
CM.customer_misroute_flag,
CD.customer_dependency_flag,

IF(lookup_date(S.shipment_delivered_at_datetime) <= lookup_date(S.logistics_promise_datetime) OR (S.shipment_delivered_at_datetime IS NULL AND from_unixtime(UNIX_TIMESTAMP()) < S.logistics_promise_datetime)
	,'03 Fulfilled'
	,IF(CM.customer_misroute_flag = 1
		,'03 Fulfilled'
		,IF(CD.customer_dependency_flag = 1 and lookup_date(if(S.shipment_carrier = '3PL',tpl_first_ofd_datetime,S.fsd_first_ofd_datetime)) <= lookup_date(S.logistics_promise_datetime) OR CD.statuses like '%Undelivered_Incomplete_Address%' OR LM.breach_bucket = 'ADM_CD'
			,'03 Fulfilled'
			,IF(S.shipment_carrier = '3PL' AND lookup_date(S.vendor_dispatch_datetime)>lookup_date(if(lookup_time(S.vendor_dispatch_datetime)<1830,S.first_mh_tc_receive_datetime,cast(date_add(S.first_mh_tc_receive_datetime,1) as timestamp)))
				,'02 Post-Dispatch_Breach'
				,IF(S.shipment_carrier = '3PL'
					,'02 Post-Dispatch_Breach'
					,IF(FFM.breach_flag=1 AND S.shipment_flash_flag=1
						,'02 Post-Dispatch_Breach'
						,IF(FM.breach_flag = 1 AND FM.LP_CP_Breach_Bucket='First_Mile_PH_breach_dispatch_same_day'
							,'02 Post-Dispatch_Breach'
							,IF(NCD.network_design_breach_flag = 1
								,'02 Post-Dispatch_Breach'
								,IF(NC.network_compliance_breach_flag = 1
									,'02 Post-Dispatch_Breach'
									,IF(NCD.connection_design_breach_flag = 1
										,'02 Post-Dispatch_Breach'
										,IF(mh_tc_breach.mh_breach_flag = 1
											,'02 Post-Dispatch_Breach'
											,IF(mh_tc_breach.tc_connection_breach_flag = 1
												,'02 Post-Dispatch_Breach'
												,IF(M.misroute_flag = 1 AND CM.customer_misroute_flag IS NULL
													,'02 Post-Dispatch_Breach'
														,IF(TPT.breach_flag = 1
															, '02 Post-Dispatch_Breach'
															,IF(LM.breach_flag = 1 OR LM.breach_bucket = 'ADM_FB'
																, '02 Post-Dispatch_Breach'
																,'02 Post-Dispatch_Breach'))))))))))))))
) AS logistics_fulfillment_bucket,

IF(lookup_date(S.shipment_delivered_at_datetime) <= lookup_date(S.logistics_promise_datetime)
	,'Delivered by promise'
	,IF((S.shipment_delivered_at_datetime IS NULL AND from_unixtime(UNIX_TIMESTAMP()) <= S.logistics_promise_datetime)
		,'NOT_DELIVERED'
		,IF(CM.customer_misroute_flag = 1
			,'Customer Misroute'
			,IF((CD.customer_dependency_flag = 1 and lookup_date(if(S.shipment_carrier = '3PL',tpl_first_ofd_datetime,S.fsd_first_ofd_datetime)) <= lookup_date(S.logistics_promise_datetime)) OR CD.statuses like '%Undelivered_Incomplete_Address%' OR LM.breach_bucket = 'ADM_CD'
				,'Customer Dependency'
				,IF(ff.fulfill_item_unit_deliver_date_change_reason='CustomerTriggered' and unix_timestamp(if(S.shipment_carrier = '3PL',cast(tpl_first_ofd_datetime as string),coalesce(TPT.dh_first_bag_received_datetime,cast(S.fsd_first_dh_received_datetime as string),from_unixtime(unix_timestamp())))) <= unix_timestamp(concat_ws(' ',cast(to_date(logistics_promise_datetime) as string),'00:00:00')) + 9*3600
					,'Customer Dependency'
					--DH Breach Flags
					,If(lookup_date(S.fsd_first_ofd_datetime) <= lookup_date(S.logistics_promise_datetime) 
							and unix_timestamp(
								if(S.shipment_carrier = 'FSD',
									coalesce(TPT.dh_first_bag_received_datetime,
										cast(S.fsd_first_dh_received_datetime as string),
											from_unixtime(unix_timestamp())),
									NULL)) <= unix_timestamp(concat_ws(' ',cast(to_date(S.logistics_promise_datetime) as string),'00:00:00')) + 9*3600
						,'01 EKL-DH_Breach'
						,IF(FFM.breach_flag=1 and s.Shipment_flash_flag=1
							,'01 EKL-DH_Breach(PICKUP)'
							,IF(LM.breach_flag = 1
								,'01 EKL-DH_Breach'
								,IF(LM.breach_bucket = 'ADM_FB'
									,'01 EKL-PC_Breach'
									--MH Breach Flags
									,If(S.shipment_carrier = '3PL' AND lookup_date(S.vendor_dispatch_datetime)>lookup_date(if(lookup_time(S.vendor_dispatch_datetime)<1830,S.first_mh_tc_receive_datetime,cast(date_add(S.first_mh_tc_receive_datetime,1) as timestamp)))
										,'02 3PL-MH_Breach'
										,IF(mh_tc_breach.mh_breach_flag = 1
											,if(mh_tc_breach.sort_resort_mp_flag ='SORT','02 EKL-MH_Breach_Sort','02 EKL-MH_Breach_Resort')
											,IF(M.misroute_flag = 1 AND CM.customer_misroute_flag IS NULL
												,'03 EKL-Misroute'
												,IF(TPT.breach_flag = 1
													,IF(Tracking.line_haul_breach_score>0, '04 EKL-TPT_Breach', '10 EKL-Unattributed_Design_Breach')
													--FM Breach Flags
													,IF(FM.breach_flag = 1 AND FM.LP_CP_Breach_Bucket ='First_Mile_PH_breach_not_dispatch_same_day'
														,'05 First_Mile_PH_breach_not_dispatch_same_day'
														,IF(FM.mp_dispatched_to_tc_date_key is not null and FM.consignment_receive_at_mh_date_key is not null and FM.consignment_receive_at_mh_date_key > FM.mp_dispatched_to_tc_date_key
															,'05 First_Mile_PH_Consignment_recieve_next_day'
															--3PL Breach Flag
															,IF(S.shipment_carrier = '3PL'
																,'06 3PL_Breach'
																--Design Breach Flags
																,If(NCD.network_design_breach_flag = 1
																	,'07 Network_Design_Breach'
																	,IF(NCD.connection_design_breach_flag = 1
																		,'08 Connection_Design_Breach'
																		,IF(NC.network_compliance_breach_flag = 1
																			,'09 Network_Compliance_Breach'
																			--Catch All Buckets
																			,IF(S.shipment_current_status ='Undelivered_Not_Attended' and S.shipment_current_hub_type ='MOTHER_HUB' and cast(unix_timestamp(shipment_current_status_datetime) as  int) + 93600 < cast(unix_timestamp() as int)
																				,'02 EKL-MH_Breach_Sort'
																				,IF(S.shipment_current_status = 'dispatched_to_vendor'
																					,'02 EKL-MH_Breach_Sort'
																					,IF(S.shipment_current_status = 'Expected' and S.shipment_current_hub_type in ('DELIVERY_HUB','MOTHER_HUB','PICKUP_CENTER')
																						,'10 EKL-Unattributed_Design_Breach'
																						,IF(S.shipment_current_hub_type = 'PICKUP_CENTER'
																							,'11 EKL-Pickup_Center_RCA','11 EKL-RCA_Bucket')
																					)))))))))))))))))))))
) AS logistics_breach_bucket,


IF(lookup_date(S.shipment_delivered_at_datetime) <= lookup_date(S.customer_promise_datetime) OR (S.shipment_delivered_at_datetime IS NULL AND from_unixtime(UNIX_TIMESTAMP()) < S.customer_promise_datetime),'03 Fulfilled',
IF(CM.customer_misroute_flag = 1,'03 Fulfilled',
IF(CD.customer_dependency_flag = 1 and lookup_date(if(S.shipment_carrier = '3PL',tpl_first_ofd_datetime,S.fsd_first_ofd_datetime)) <= lookup_date(S.customer_promise_datetime) OR CD.statuses like '%Undelivered_Incomplete_Address%'
OR LM.breach_bucket = 'ADM_CD','03 Fulfilled',
If(S.shipment_carrier = '3PL' AND lookup_date(S.vendor_dispatch_datetime)>lookup_date(if(lookup_time(S.vendor_dispatch_datetime)<1830,S.first_mh_tc_receive_datetime,cast(date_add(S.first_mh_tc_receive_datetime,1) as timestamp))),'02 Post-Dispatch_Breach',
If(S.fsd_first_ofd_datetime<S.customer_promise_datetime,'02 Post-Dispatch_Breach',
IF(FFM.breach_flag=1 and s.Shipment_flash_flag=1,'02 Post-Dispatch_Breach',
IF(FM.breach_flag = 1, If(lower(FM.LP_CP_Breach_Bucket) ='First_Mile_PH_breach_not_dispatch_same_day','02 Post-Dispatch_Breach',IF(lower(FM.LP_CP_Breach_Bucket) <> 'met','01 Pre-Dispatch_Breach','01 Pre-Dispatch_Breach')),
IF(S.shipment_carrier = '3PL','02 Post-Dispatch_Breach',
If(NCD.network_design_breach_flag = 1,'02 Post-Dispatch_Breach',
IF(NC.network_compliance_breach_flag = 1,'02 Post-Dispatch_Breach',
IF(NCD.connection_design_breach_flag = 1,'02 Post-Dispatch_Breach',
IF(mh_tc_breach.mh_breach_flag = 1,'02 Post-Dispatch_Breach',
IF(mh_tc_breach.tc_connection_breach_flag = 1,'02 Post-Dispatch_Breach',
IF(TPT.breach_flag = 1, '02 Post-Dispatch_Breach',
IF(M.misroute_flag = 1 AND CM.customer_misroute_flag IS NULL,'02 Post-Dispatch_Breach',
IF(LM.breach_flag = 1 OR LM.breach_bucket = 'ADM_FB', '02 Post-Dispatch_Breach','02 Post-Dispatch_Breach')))))))))))))))) AS customer_fulfillment_bucket,

IF(lookup_date(S.shipment_delivered_at_datetime) <= lookup_date(S.customer_promise_datetime)
	,'Delivered by promise'
	,IF((S.shipment_delivered_at_datetime IS NULL AND from_unixtime(UNIX_TIMESTAMP()) <= S.customer_promise_datetime)
		,'NOT_DELIVERED'
		,IF(CM.customer_misroute_flag = 1
			,'Customer Misroute'
			,IF((CD.customer_dependency_flag = 1 and lookup_date(if(S.shipment_carrier = '3PL',tpl_first_ofd_datetime,S.fsd_first_ofd_datetime)) <= lookup_date(S.customer_promise_datetime)) OR CD.statuses like '%Undelivered_Incomplete_Address%' OR LM.breach_bucket = 'ADM_CD'
				,'Customer Dependency'
				,IF(ff.fulfill_item_unit_deliver_date_change_reason='CustomerTriggered' and unix_timestamp(if(S.shipment_carrier = '3PL',cast(tpl_first_ofd_datetime as string),coalesce(TPT.dh_first_bag_received_datetime,cast(S.fsd_first_dh_received_datetime as string),from_unixtime(unix_timestamp())))) <= unix_timestamp(concat_ws(' ',cast(to_date(customer_promise_datetime) as string),'00:00:00')) + 9*3600
					,'Customer Dependency'
					--DH Breach Flags
					,If(lookup_date(S.fsd_first_ofd_datetime) <= lookup_date(S.customer_promise_datetime) 
							and unix_timestamp(
								if(S.shipment_carrier = 'FSD',
									coalesce(TPT.dh_first_bag_received_datetime,
										cast(S.fsd_first_dh_received_datetime as string),
											from_unixtime(unix_timestamp())),
									NULL)) <= unix_timestamp(concat_ws(' ',cast(to_date(S.customer_promise_datetime) as string),'00:00:00')) + 9*3600
						,'01 EKL-DH_Breach'
						,IF(FFM.breach_flag=1 AND s.Shipment_flash_flag=1
							,'01 EKL-DH_Breach(PICKUP)'
							,IF(LM.breach_flag = 1
								,'01 EKL-DH_Breach'
								,IF(LM.breach_bucket = 'ADM_FB'
									,'09 EKL-PC_Breach'
									--MH Breach Flags
									,If(S.shipment_carrier = '3PL' AND lookup_date(S.vendor_dispatch_datetime)>lookup_date(if(lookup_time(S.vendor_dispatch_datetime)<1830,S.first_mh_tc_receive_datetime,cast(date_add(S.first_mh_tc_receive_datetime,1) as timestamp)))
										,'02 3PL-MH_Breach'
										,IF(mh_tc_breach.mh_breach_flag = 1
											,if(mh_tc_breach.sort_resort_mp_flag ='SORT','02 EKL-MH_Breach_Sort','02 EKL-MH_Breach_Resort')
											,IF(M.misroute_flag = 1 AND CM.customer_misroute_flag IS NULL
												,'03 EKL-Misroute'
												,IF(TPT.breach_flag = 1
													,IF(Tracking.line_haul_breach_score>0, '04 EKL-TPT_Breach', '10 EKL-Unattributed_Design_Breach')
													--FM Breach Flags
													,IF(FM.breach_flag = 1
														,IF(FM.LP_CP_Breach_Bucket ='First_Mile_PH_breach_not_dispatch_same_day', '05 First_Mile_PH_breach_not_dispatch_same_day', FM.LP_CP_Breach_Bucket)
														,IF(FM.mp_dispatched_to_tc_date_key is not null and FM.consignment_receive_at_mh_date_key is not null and FM.consignment_receive_at_mh_date_key > FM.mp_dispatched_to_tc_date_key
															,'05 First_Mile_PH_Consignment_recieve_next_day'
															--3PL Breach Flags
															,IF(S.shipment_carrier = '3PL'
																,'06 3PL_Breach'
																-- Design Breach Flags
																,If(NCD.network_design_breach_flag = 1
																	,'07 Network_Design_Breach'
																	,IF(NCD.connection_design_breach_flag = 1
																		,'08 Connection_Design_Breach'
																		,IF(NC.network_compliance_breach_flag = 1
																			,'09 Network_Compliance_Breach'
																			--Catch All Buckets
																			,IF(S.shipment_current_status ='Undelivered_Not_Attended' and S.shipment_current_hub_type ='MOTHER_HUB' and cast(unix_timestamp(shipment_current_status_datetime) as  int) + 93600 < cast(unix_timestamp()  as int)
																				,'02 EKL-MH_Breach_Sort'
																				,IF(S.shipment_current_status = 'dispatched_to_vendor'
																					,'02 EKL-MH_Breach_Sort'
																					,IF(S.shipment_current_status = 'Expected' and S.shipment_current_hub_type in ('DELIVERY_HUB','MOTHER_HUB','PICKUP_CENTER')
																						,'10 EKL-Unattributed_Design_Breach'
																						,IF(S.shipment_current_hub_type = 'PICKUP_CENTER'
																							,'11 EKL-Pickup_Center_RCA','11 EKL-RCA_Bucket')
																					)))))))))))))))))))))
) AS customer_breach_bucket,
							   
TPT.dh_first_bag_received_datetime,
ff.fulfill_item_unit_deliver_date_change_reason,
Tracking.line_haul_breach_score as eta_breach_score
FROM
(select  shipment_id,vendor_tracking_id,merchant_reference_id,merchant_id,seller_id,shipment_current_status,payment_type,payment_mode,pos_id,transaction_id,agent_id,amount_collected,seller_type,shipment_dg_flag,shipment_fragile_flag,shipment_priority_flag,shipment_size_flag,item_quantity,ekl_shipment_type,reverse_shipment_type,first_undelivery_status,last_undelivery_status,ekl_fin_zone,shipment_fa_flag,vendor_id,shipment_carrier,shipment_pending_flag,shipment_num_of_pk_attempt,fsd_number_of_ofd_attempts,shipment_weight,sender_weight,system_weight,volumetric_weight,billable_weight,billable_weight_type,cost_of_breach,shipment_value,cod_amount_to_collect,shipment_charge,source_address_pincode,destination_address_pincode,customer_address_id,cs_notes,hub_notes,fsd_assigned_hub_id,shipment_current_hub_id,shipment_first_received_hub_id,shipment_last_received_hub_id,shipment_first_received_mh_id,shipment_last_received_mh_id,shipment_origin_mh_facility_id,shipment_destination_mh_facility_id,shipment_origin_facility_id,shipment_destination_facility_id,shipment_current_hub_type, cast (shipment_created_at_datetime as timestamp) as shipment_created_at_datetime, cast (shipment_dispatch_datetime as timestamp) as shipment_dispatch_datetime, cast (vendor_dispatch_datetime as timestamp) as vendor_dispatch_datetime, cast (shipment_current_status_datetime as timestamp) as shipment_current_status_datetime, cast (shipment_first_received_at_datetime as timestamp) as shipment_first_received_at_datetime, cast (shipment_delivered_at_datetime as timestamp) as shipment_delivered_at_datetime, cast (logistics_promise_datetime as timestamp) as logistics_promise_datetime, cast (shipment_actual_sla_datetime as timestamp) as shipment_actual_sla_datetime, cast (customer_promise_datetime as timestamp) as customer_promise_datetime, cast (new_logistics_promise_datetime as timestamp) as new_logistics_promise_datetime, cast (new_customer_promise_datetime as timestamp) as new_customer_promise_datetime, cast (shipment_last_receive_datetime as timestamp) as shipment_last_receive_datetime, cast (fsd_first_dh_received_datetime as timestamp) as fsd_first_dh_received_datetime, cast (fsd_first_ofd_datetime as timestamp) as fsd_first_ofd_datetime, cast (fsd_last_ofd_datetime as timestamp) as fsd_last_ofd_datetime, cast (received_at_origin_facility_datetime as timestamp) as received_at_origin_facility_datetime, cast (rto_create_datetime as timestamp) as rto_create_datetime, cast (rto_complete_datetime as timestamp) as rto_complete_datetime, cast (tpl_first_ofd_datetime as timestamp) as tpl_first_ofd_datetime, cast (tpl_last_ofd_datetime as timestamp) as tpl_last_ofd_datetime, cast (first_mh_tc_receive_datetime as timestamp) as first_mh_tc_receive_datetime, cast (last_mh_tc_receive_datetime as timestamp) as last_mh_tc_receive_datetime, cast (first_mh_tc_outscan_datetime as timestamp) as first_mh_tc_outscan_datetime, cast (last_mh_tc_outscan_datetime as timestamp) as last_mh_tc_outscan_datetime, cast (first_dh_outscan_datetime as timestamp) as first_dh_outscan_datetime, cast (last_dh_outscan_datetime as timestamp) as last_dh_outscan_datetime,shipment_flash_flag  from bigfoot_external_neo.scp_ekl__shipment_l1_90_fact
where ekl_shipment_type in ('forward','unapproved_rto','approved_rto')) S
LEFT OUTER JOIN 
(select 
vendor_tracking_id,
mh_breach_60_nom as mh_breach_flag,
tc_connection_breach_nom as tc_connection_breach_flag,
sort_resort_mp_flag
from
(select vendor_tracking_id,
incoming_consignment_create_datetime,
mh_breach_60_nom,
tc_connection_breach_nom,
sort_resort_mp_flag,
ROW_NUMBER() OVER (PARTITION BY vendor_tracking_id order by incoming_consignment_create_datetime ASC
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as rank
    from 
    (select vendor_tracking_id,
    incoming_consignment_create_datetime,
    mh_breach_60_nom,
    tc_connection_breach_nom,
    sort_resort_mp_flag
    from bigfoot_external_neo.scp_ekl__shipment_facility_metrics_hive_fact
    where mh_breach_60_nom =1 or tc_connection_breach_nom =1
    ) filter_table
)ranker
where rank =1 
)mh_tc_breach
ON S.vendor_tracking_id = mh_tc_breach.vendor_tracking_id
left outer join 
(select 
tracking_id,
max(breach_flag) as breach_flag,
max(mp_dispatched_to_tc_date_key) as mp_dispatched_to_tc_date_key ,
max(consignment_receive_at_mh_date_key) as consignment_receive_at_mh_date_key,
max(LP_CP_Breach_Bucket) as LP_CP_Breach_Bucket
FROM bigfoot_external_neo.scp_ekl__first_mile_breach_90_testv2_fact
group by tracking_id) FM ON S.vendor_tracking_id = FM.tracking_id

LEFT OUTER JOIN 
(select tracking_id,
max(breach_flag) as breach_flag,
max(breach_score) as breach_score,
max(dh_first_bag_received_datetime) as dh_first_bag_received_datetime
FROM bigfoot_external_neo.scp_ekl__tpt_breach_90_fact
group by tracking_id) TPT ON S.vendor_tracking_id = TPT.tracking_id

LEFT OUTER JOIN 
(select
tracking_id,
max(breach_flag) as breach_flag,
max(breach_bucket) as breach_bucket
FROM 
bigfoot_external_neo.scp_ekl__last_mile_breach_90_fact
group by tracking_id) LM ON S.vendor_tracking_id = LM.tracking_id
LEFT OUTER JOIN 
(select 
  vendor_tracking_id,
  max(customer_dependency_flag) as customer_dependency_flag,
  max(statuses) as statuses
  FROM
  bigfoot_external_neo.scp_ekl__customer_dependency_90_fact
  group by vendor_tracking_id)CD ON S.vendor_tracking_id = CD.vendor_tracking_id
LEFT OUTER JOIN 
(
  select 
  vendor_tracking_id,
  max(misroute_flag) as misroute_flag
  FROM 
bigfoot_external_neo.scp_ekl__misroute_90_fact
group by vendor_tracking_id) M ON S.vendor_tracking_id = M.vendor_tracking_id
LEFT OUTER JOIN 
(select vendor_tracking_id,
max(customer_misroute_flag) as customer_misroute_flag
FROM 
bigfoot_external_neo.scp_ekl__customer_misroute_90_fact
group by vendor_tracking_id) CM ON S.vendor_tracking_id = CM.vendor_tracking_id
LEFT OUTER JOIN 
(select vendor_tracking_id,
  max(network_design_breach_flag) as network_design_breach_flag,
  max(connection_design_breach_flag) as connection_design_breach_flag
  FROM bigfoot_external_neo.scp_ekl__network_and_connection_design_breach_hive_fact
  group by vendor_tracking_id) NCD ON S.vendor_tracking_id = NCD.vendor_tracking_id
LEFT OUTER JOIN 
(select 
vendor_tracking_id,
max(network_compliance_breach_flag) as network_compliance_breach_flag
FROM 
bigfoot_external_neo.scp_ekl__network_compliance_breach_90_fact
group by vendor_tracking_id) NC ON S.vendor_tracking_id = NC.vendor_tracking_id 
LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__flash_last_mile_breach_90_fact ffm ON ffm.vendor_tracking_id=S.vendor_tracking_id
LEFT OUTER JOIN 
(
select 
max(fulfill_item_unit_deliver_date_change_reason) as fulfill_item_unit_deliver_date_change_reason,shipment_merchant_reference_id from
bigfoot_external_neo.scp_fulfillment__fulfillment_unit_hive_fact 
where fulfill_item_unit_deliver_date_change_reason like '%CustomerTriggered%' group by shipment_merchant_reference_id
) as ff
on ff.shipment_merchant_reference_id=s.merchant_reference_id
LEFT OUTER JOIN
(SELECT vendor_tracking_id,
max(line_haul_breach_score) as line_haul_breach_score
FROM bigfoot_external_neo.scp_ekl__tracking_shipment_intermediate_hive_fact
group by vendor_tracking_id) Tracking 
on S.vendor_tracking_id = Tracking.vendor_tracking_id;
