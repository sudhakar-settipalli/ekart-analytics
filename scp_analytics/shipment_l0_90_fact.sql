INSERT OVERWRITE TABLE shipment_l0_90_fact
SELECT /*+ MAPJOIN(FKL_MH,Rev_FKL_MH,geo_src,geo_dest) */  
		DISTINCT s_current.entityid   AS shipment_id,
		s_current.first_associated_shipment_id AS first_associated_shipment_id,
		s_current.vendor_tracking_id AS vendor_tracking_id,
		b2clogisticsrequest.merchant_reference_id AS merchant_reference_id,
		b2clogisticsrequest.merchant_id  AS merchant_id,
		s_current.seller_id AS seller_id,
		s_current.status AS shipment_current_status,
		s_current.payment_type AS payment_type,
		s_current.payment_mode AS payment_mode,
		s_current.pos_id AS pos_id,
		s_current.transaction_id AS transaction_id,
		s_current.agent_id  AS agent_id,
		s_current.amount_collected AS amount_collected,
		IF(lower(s_current.source_address_type) = 'warehouse'
			, IF(lower(s_current.seller_id) IN ('wsr','d591418b408940a0')
				,'WSR'
				, IF(lower(s_current.vendor_tracking_id) LIKE'%myn%'
					,'MYN'
					, IF(upper(s_current.vendor_tracking_id) LIKE'%JBN%'
							,'JBN'
							, IF(upper(s_current.vendor_tracking_id) LIKE'%YPM%'
								,'YEPME'
								, IF(upper(s_current.vendor_tracking_id) LIKE'%PTM%'
									,'PAYTM'
									, IF(upper(s_current.vendor_tracking_id) LIKE'%VNK%'
										,'VOONIK'
										, IF(upper(s_current.vendor_tracking_id) LIKE'%HOP%'
											,'HOPSCOTCH'
											, IF(upper(s_current.vendor_tracking_id) LIKE'%RLG%'
												,'RELIANCE_ADA'
												, IF(upper(s_current.vendor_tracking_id) LIKE'%PGN%'
													,'GOPIGEON'
													, IF(upper(s_current.vendor_tracking_id) LIKE'%HLK%'
														,'HEALTHKART'
														, IF(upper(s_current.vendor_tracking_id) LIKE'%MDR%'
															,'MADHURA'
															, IF(upper(s_current.vendor_tracking_id) LIKE'%MVK%'
																,'MR VOONIK'
																, IF(upper(s_current.vendor_tracking_id) LIKE'%CLQ%'
																	,'TATA CLIQ'
																	, IF(upper(s_current.vendor_tracking_id) LIKE'%ABO%'
																		,'ADITYA BIRLA'
																		, IF(upper(s_current.vendor_tracking_id) LIKE'%SHP%'
																			,'SHOPPERSSTOP','FA'))))))))))))))
				)
			, IF(lower(s_current.source_address_type) = 'mp_non_fbf_seller'
				,'Non-FA'
				, IF(lower(s_current.source_address_type) = 'customer'
					, IF(lower(s_current.vendor_tracking_id) LIKE'%myn%'
						,'MYN'
						, IF(lower(s_current.seller_id) IN ('wsr','d591418b408940a0')
							,'WSR'
							, IF(upper(s_current.vendor_tracking_id) LIKE'%JBN%'
								,'JBN'
								, IF(upper(s_current.vendor_tracking_id) LIKE'%YPM%'
									,'YEPME'
									, IF(upper(s_current.vendor_tracking_id) LIKE'%PTM%'
										,'PAYTM'
										, IF(upper(s_current.vendor_tracking_id) LIKE'%VNK%'
											,'VOONIK'
											, IF(upper(s_current.vendor_tracking_id) LIKE'%HOP%'
													,'HOPSCOTCH'
													, IF(upper(s_current.vendor_tracking_id) LIKE'%RLG%'
														,'RELIANCE_ADA'
														, IF(upper(s_current.vendor_tracking_id) LIKE'%PGN%'
															,'GOPIGEON'
															, IF(upper(s_current.vendor_tracking_id) LIKE'%HLK%'
																,'HEALTHKART'
																, IF(upper(s_current.vendor_tracking_id) LIKE'%MDR%'
																	,'MADHURA'
																	, IF(upper(s_current.vendor_tracking_id) LIKE'%MVK%'
																		,'MR VOONIK'
																		, IF(upper(s_current.vendor_tracking_id) LIKE'%CLQ%'
																			,'TATA CLIQ'
																			, IF(upper(s_current.vendor_tracking_id) LIKE'%ABO%'
																				,'ADITYA BIRLA'
																				, IF(upper(s_current.vendor_tracking_id) LIKE'%SHP%'
																					,'SHOPPERSSTOP'
																					, IF(lower(s_current.destination_address_type) = 'warehouse'
																						,'FA','Non-FA')))))))))))))))
						)
					,NULL)
				)
		  ) AS seller_type, 
		s_current.shipment_dg_flag AS shipment_dg_flag,
		s_current.shipment_flash_flag AS shipment_flash_flag,
		s_current.shipment_fragile_flag AS shipment_fragile_flag,
		IF(b2clogisticsrequest.shipment_priority_flag IS NULL,'Normal',b2clogisticsrequest.shipment_priority_flag)  AS shipment_priority_flag,
		b2clogisticsrequest.service_tier  AS service_tier,
		b2clogisticsrequest.surface_mandatory_flag  AS surface_mandatory_flag,
		s_current.shipment_size_flag  AS shipment_size_flag,
		s_current.item_quantity AS item_quantity,
		s_current.shipping_category AS shipping_category,
		IF(s_current.first_associated_shipment_id is not null
				AND lower(t1.shipment_type) IN ('approved_rto', 'unapproved_rto', 'rvp') 
				AND lower(t1.status) NOT IN ('reshipped', 'pickup_leg_completed') 
				AND lower(s_current.status) NOT IN ('pickup_leg_completed')
			,'merchant_return'
			,s_current.shipment_type)   AS ekl_shipment_type,
		IF(s_current.shipment_type = 'rvp',IF(b2clogisticsrequest.vas_type = 'Product Exchange','PREXO',IF(b2clogisticsrequest.vas_type = 'Replacement','Replacement','Pickup_Only')),NULL) AS reverse_shipment_type,
		IF((upper(geo_src.src_lt) = upper(geo_dest.dest_lt) 
				OR  upper(geo_src.src_city) = upper(geo_dest.dest_city)), "INTRACITY", IF(upper(geo_src.src_zone) = upper(geo_dest.dest_zone), "INTRAZONE", IF(upper(geo_src.src_zone) <> upper(geo_dest.dest_zone), "INTERZONE", "Missing"))) AS ekl_fin_zone,
		IF((upper(geo_src.src_lt) = upper(geo_dest.dest_lt) 
				OR  upper(geo_src.src_city) = upper(geo_dest.dest_city)), "LOCAL", IF(upper(geo_src.src_zone) = upper(geo_dest.dest_zone), "ZONAL", IF(upper(geo_src.src_city) IN ('CHENNAI','MUMBAI','NEW DELHI','KOLKATA','BANGALORE','HYDERABAD','AHMEDABAD','PUNE')
				AND upper(geo_dest.dest_city) IN ('CHENNAI', 'MUMBAI', 'NEW DELHI', 'KOLKATA', 'BANGALORE', 'HYDERABAD', 'AHMEDABAD', 'PUNE') 
				AND geo_src.src_city <> geo_dest.dest_city, "METRO", IF(upper(geo_dest.dest_state) IN ('SIKKIM','ASSAM','MANIPUR','MEGHALAYA','MIZORAM','ARUNACHAL PRADESH','NAGALAND','TRIPURA','JAMMU AND KASHMIR'), "JK_NE", "ROI" ) ) ) ) AS ekart_lzn_flag,
		IF(s_current.source_address_type LIKE'%NON_FBF%'
			,0
			,IF(upper(s_current.source_address_type) = 'CUSTOMER'
						AND lower(s_current.destination_address_type) = 'warehouse'
				,1
				,0)) AS shipment_fa_flag,
		s_current.vendor_id AS vendor_id,
		IF(s_current.vendor_tracking_id = 'not_assigned' 
				OR  s_current.vendor_tracking_id IS NULL
			,'VNF'
			,IF(s_current.vendor_id IN (200, 207, 242) 
				,'FSD'
				,'3PL')) AS shipment_carrier,
		IF(lower(s_current.status) IN ('lost', 'reshipped', 'not_received', 'delivered', 'delivery_update', 'received_by_merchant', 'returned_to_seller', 'damaged', 'pickup_leg_completed', 'pickup_leg_complete'),0,1)  AS shipment_pending_flag,
		s_group.out_for_pickup_attempts  AS shipment_num_of_pk_attempt,
		s_group.fsd_number_of_ofd_attempts  AS fsd_number_of_ofd_attempts,
		s_group.shipment_rvp_pk_number_of_attempts  AS shipment_rvp_pk_number_of_attempts,
		s_current.shipment_weight_physical AS shipment_weight,
		s_current.sender_weight_physical AS sender_weight,
		s_current.system_weight_physical AS system_weight,
		NULL  AS volumetric_weight_source,
		IF(s_current.vendor_id IN (198,204,202,196),(vol.volumetric_weight *6/5),vol.volumetric_weight )  AS volumetric_weight,
		IF(cast(IF(s_current.vendor_id IN (198,204,202,196),(vol.volumetric_weight *6/5),vol.volumetric_weight ) AS int) > cast(IF(s_current.shipment_weight_physical IS NULL,0,s_current.shipment_weight_physical) AS int),IF(s_current.vendor_id IN (198,204,202,196),(vol.volumetric_weight *6/5),vol.volumetric_weight ),s_current.shipment_weight_physical) AS billable_weight,
		IF(cast(IF(s_current.vendor_id IN (198,204,202,196),(vol.volumetric_weight *6/5),vol.volumetric_weight ) AS int) > cast(IF(s_current.shipment_weight_physical IS NULL,0,s_current.shipment_weight_physical) AS int),'Volumetric','Physical')   AS billable_weight_type,
		b2clogisticsrequest.cost_of_breach  AS cost_of_breach,
		s_current.shipment_value  AS shipment_value,
		s_current.cod_amount_to_collect  AS cod_amount_to_collect,
		s_current.shipment_charge  AS shipment_charge,
		s_current.source_address_pincode AS source_address_pincode,
		s_current.destination_address_pincode  AS destination_address_pincode,
		IF(s_current.shipment_type NOT IN ('rvp'),s_current.destination_address_id,s_current.source_address_id)  AS customer_address_id,
		notes.cs_notes  AS cs_notes,
		notes.hub_notes  AS hub_notes,
		s_current.assigned_address_id  AS fsd_assigned_hub_id,
		IF(s_current.shipment_type = 'rvp',-1,NULL)  AS reverse_pickup_hub_id,
		s_current.current_address_id  AS shipment_current_hub_id,
		fkl_mh.mh_facility_id  AS shipment_origin_mh_facility_id,
		IF(lower(s_current.status) IN ('received_by_ekl', 'returned_to_ekl', 'received_by_merchant', 'returned_to_seller'),-1, rev_fkl_mh.mh_facility_id) AS shipment_destination_mh_facility_id,
		s_current.source_address_id AS shipment_origin_facility_id,
		IF(s_current.shipment_type IN ('rvp'),s_current.destination_address_id,NULL) AS shipment_destination_facility_id,
		s_current.current_address_type   AS shipment_current_hub_type,
		cast(s_current.created_at AS timestamp) AS shipment_created_at_datetime,
		cast(s_group.received_at_source_facility AS timestamp) AS shipment_dispatch_datetime,
		cast(s_group.dispatched_to_vendor_time AS timestamp)   AS vendor_dispatch_datetime,
		cast(s_current.updatedat AS timestamp)  AS shipment_current_status_datetime,
		cast(s_group.received_time AS timestamp)   AS shipment_first_received_at_datetime,
		cast(s_group.delivered_time AS timestamp)  AS shipment_delivered_at_datetime,
		cast(s_group.last_delivered_time AS timestamp)   AS shipment_last_delivered_at_datetime,
		cast(s_group.first_delivery_update_time AS timestamp)  AS shipment_first_delivery_update_datetime,
		cast(s_group.last_delivery_update_time AS timestamp)   AS shipment_last_delivery_update_datetime,
		cast(s_group.first_dispatched_to_merchant_time AS timestamp) AS shipment_first_dispatched_to_merchant_datetime,
		cast(s_current.design_sla AS timestamp) AS logistics_promise_datetime,
		cast(s_current.actual_sla AS timestamp) AS shipment_actual_sla_datetime,
		cast(s_current.customer_sla AS timestamp) AS customer_promise_datetime,
		cast(s_current.design_sla AS timestamp) AS new_logistics_promise_datetime,
		cast(s_current.customer_sla AS timestamp) AS new_customer_promise_datetime,
		cast(s_group.received_last_time AS timestamp) AS shipment_last_receive_datetime,
		cast(s_group.dh_receive_time AS timestamp) AS fsd_first_dh_received_datetime,
		cast(s_group.last_dh_receive_time AS timestamp)  AS fsd_last_dh_received_datetime,
		cast(s_group.first_received_pc_time AS timestamp)   AS shipment_first_received_pc_datetime,
		cast(s_group.last_received_pc_time AS timestamp) AS shipment_last_received_pc_datetime,
		cast(s_group.first_ofd_time AS timestamp)  AS fsd_first_ofd_datetime,
		cast(s_group.last_ofd_time AS timestamp)   AS fsd_last_ofd_datetime,
		cast(s_group.first_rfp_time AS timestamp)  AS shipment_first_rfp_datetime,
		cast(s_group.last_rfp_time AS timestamp)   AS shipment_last_rfp_datetime,
		cast(s_group.first_picksheet_creation_time AS timestamp)  AS shipment_first_picksheet_creation_time,
		cast(s_group.last_picksheet_creation_time AS timestamp)   AS shipment_last_picksheet_creation_time,
		cast(s_group.first_rvp_pickup_time AS timestamp) AS shipment_first_rvp_pickup_time,
		cast(s_group.last_rvp_pickup_time AS timestamp)  AS shipment_last_rvp_pickup_time,
		cast(s_group.rto_first_received_time AS timestamp)  AS rto_first_received_time,
		cast(s_group.received_at_origin_facility AS timestamp) AS received_at_origin_facility_datetime,
		cast(rto_create_time AS timestamp)   AS rto_create_datetime,
		cast(rto_complete_time AS timestamp) AS rto_complete_datetime,
		cast(3pl_first_ofd_time AS timestamp)   AS tpl_first_ofd_datetime,
		cast(3pl_last_ofd_time AS timestamp) AS tpl_last_ofd_datetime,
		cast(first_mh_tc_receive_time AS timestamp)   AS first_mh_tc_receive_datetime,
		cast(last_mh_tc_receive_time AS timestamp) AS last_mh_tc_receive_datetime,
		NULL AS first_mh_tc_outscan_datetime,
		NULL AS last_mh_tc_outscan_datetime,
		NULL AS first_dh_outscan_datetime,
		NULL AS last_dh_outscan_datetime,
		s_current.shipment_weight_updated_by   AS profiler_flag,
		NULL AS profiled_hub_id,
		s_current.dispatch_service_tier  AS shipment_dispatch_service_tier,
		cast(s_current.dispatch_by_date AS timestamp)   AS shipment_dispatch_by_datetime,
		lzn_cons.shipment_lzn_classification AS shipment_lzn_classification,
		lzn_cons.shipment_transit_distance   AS shipment_transit_distance,
		cast(lzn_cons.shipment_transit_time AS timestamp) AS shipment_transit_time,
		cast(reverse_complete_time AS timestamp)   AS reverse_complete_datetime,
		cast(s_current.pickup_slot_start_date AS timestamp)   AS pickup_slot_start_datetime,
		cast(s_current.pickup_slot_end_date AS timestamp)  AS pickup_slot_end_datetime,
		S_group.rto_number_of_ofd_attempts as rto_num_ofd_attempts,
		s_current.shipment_contour_volume as shipment_contour_volume,
		s_current.mlo_datetime as mlo_datetime
FROM  (
			SELECT
				entityid as entityid,
				IF(size(`data`.associated_shipment_ids) = 1,`data`.associated_shipment_ids[0],NULL) AS first_associated_shipment_id,
				`data`.vendor_tracking_id as vendor_tracking_id,
				`data`.shipment_items[0].seller_id as seller_id,
				`data`.status AS status,
				`data`.payment_type AS payment_type,
				`data`.payment.payment_details.mode[0] AS payment_mode,
				`data`.payment.payment_details[0].device_id AS pos_id,
				`data`.payment.payment_details[0].transaction_id  AS transaction_id,
				`data`.payment.payment_details[0].agent_id  AS agent_id,
				`data`.payment.amount_collected.value AS amount_collected,
				`data`.source_address.type as source_address_type,
				`data`.destination_address.type as destination_address_type,
				IF(concat_ws("-",`data`.attributes) LIKE'%dangerous%',1,0)  AS shipment_dg_flag,
				IF(upper(concat_ws("-",`data`.attributes)) LIKE'%FLASH%',1,0)  AS shipment_flash_flag,
				IF(concat_ws("-",`data`.attributes) LIKE'%ragile%',1,0)  AS shipment_fragile_flag,
				`data`.size  AS shipment_size_flag,
				size(`data`.shipment_items)  AS item_quantity,
				`data`.shipping_category  AS shipping_category,
				`data`.shipment_type as shipment_type,
				`data`.vendor_id  AS vendor_id,
				`data`.shipment_weight.physical as shipment_weight_physical,
				`data`.sender_weight.physical  AS sender_weight_physical,
				`data`.system_weight.physical  AS system_weight_physical,
				`data`.value.value  AS shipment_value,
				`data`.amount_to_collect.value as cod_amount_to_collect,
				`data`.shipment_charge.total_charge.value as shipment_charge,
				`data`.source_address.id as source_address_id,
				`data`.source_address.pincode as source_address_pincode,
				`data`.destination_address.id as destination_address_id,
				`data`.destination_address.pincode  AS destination_address_pincode,
				`data`.assigned_address.id as assigned_address_id,
				`data`.current_address.id as current_address_id,
				`data`.current_address.type as current_address_type,
				`data`.created_at as created_at,
				updatedat as updatedat,
				`data`.design_sla as design_sla,
				`data`.actual_sla as actual_sla,
				`data`.customer_sla as customer_sla,
				`data`.shipment_weight.updated_by as shipment_weight_updated_by,
				`data`.dispatch_service_tier as dispatch_service_tier,
				`data`.dispatch_by_date as dispatch_by_date,
				`data`.pickup_slot_start_date as pickup_slot_start_date,
				`data`.pickup_slot_end_date as pickup_slot_end_date,
				`data`.contour_volume as shipment_contour_volume,
				cast(if(upper(`data`.vendor_tracking_id) like '%MYN%',
						regexp_extract(concat_ws('+',data.attributes), '(\\d+-\\d+-\\d+ \\d+:\\d+:\\d+)',1),
						NULL) 
					as timestamp) as mlo_datetime
			FROM bigfoot_snapshot.dart_wsr_scp_ekl_shipment_4_view 
			WHERE `data`.current_address.type <> 'BULK_HUB'
			    AND `data`.source_address.id NOT IN (563,564,565,566,567,1280,1282,1288,1511,1757,1950,2043,3594,3612,3620,3621,3622,3623)
				AND `data`.destination_address.id NOT IN (563,564,565,566,567,1280,1282,1288,1511,1757,1950,2043,3594,3612,3620,3621,3622,3623)
		) s_current
LEFT OUTER JOIN 
                (  SELECT `data`.lzn_classification   AS shipment_lzn_classification,
						  cast(`data`.transit_distance AS int) AS shipment_transit_distance,
						  `data`.transit_time   AS shipment_transit_time, 
						  `data`.source_pincode AS source_pincode, 
						  `data`.destination_pincode  AS destination_pincode 
                       FROM   bigfoot_journal.dart_fkint_scp_ekl_geo_classification_2
				) lzn_cons
  ON    IF(s_current.shipment_type = 'rvp'
            ,s_current.destination_address_pincode
			,s_current.source_address_pincode) = lzn_cons.source_pincode
        AND IF(s_current.shipment_type = 'rvp'
		        ,s_current.source_address_pincode
				,s_current.destination_address_pincode) = lzn_cons.destination_pincode
LEFT OUTER JOIN bigfoot_common.ekl_fkl_facility_mother_hub_mapping fkl_mh 
  ON  s_current.source_address_id = fkl_mh.fkl_facility_id 
LEFT OUTER JOIN bigfoot_common.ekl_fkl_facility_mother_hub_mapping rev_fkl_mh 
  ON  s_current.destination_address_id = rev_fkl_mh.fkl_facility_id 
LEFT OUTER JOIN 
                (  SELECT `data`.pincode   AS src_pincode, 
						  `data`.zone   AS src_zone, 
						  `data`.local_territory AS src_lt, 
						  `data`.city   AS src_city, 
						  `data`.state  AS src_state 
                       FROM   bigfoot_snapshot.dart_fki_scp_ekl_geo_1_view_total
				) geo_src 
  ON  IF(s_current.shipment_type = 'rvp',s_current.destination_address_pincode,s_current.source_address_pincode) = geo_src.src_pincode
LEFT OUTER JOIN 
                (  SELECT `data`.pincode   AS dest_pincode, 
						  `data`.zone   AS dest_zone, 
						  `data`.local_territory AS dest_lt, 
						  `data`.city   AS dest_city, 
						  `data`.state  AS dest_state 
                       FROM   bigfoot_snapshot.dart_fki_scp_ekl_geo_1_view_total
				) geo_dest 
  ON  IF(s_current.shipment_type = 'rvp',s_current.source_address_pincode,s_current.destination_address_pincode) = geo_dest.dest_pincode
LEFT OUTER JOIN 
                (  SELECT entityid, 
						  `data`.shipment_type AS shipment_type, 
						  lower(`data`.status) AS status 
                       FROM   bigfoot_snapshot.dart_wsr_scp_ekl_shipment_4_view 
                       WHERE  `data`.current_address.type <> 'BULK_HUB'
						   AND `data`.shipment_type IN ('approved_rto', 'unapproved_rto', 'rvp') 
						   AND `data`.source_address.id NOT IN (563,564,565,566,567,1280,1282,1288,1511,1757,1950,2043,3594,3612,3620,3621,3622,3623)
						   AND `data`.destination_address.id NOT IN (563,564,565,566,567,1280,1282,1288,1511,1757,1950,2043,3594,3612,3620,3621,3622,3623)
						   AND `data`.current_address.type <> 'BULK_HUB'
						   AND lower(`data`.status) NOT IN ('reshipped', 'pickup_leg_completed')
				) t1 
  ON  (  t1.entityid = s_current.first_associated_shipment_id)
INNER JOIN 
			( SELECT   entityid  AS shipment_id,
						  min(IF(lower(`data`.status) = 'dispatched_to_vendor',updatedat,NULL)) AS dispatched_to_vendor_time,
						  min(IF(lower(`data`.status) IN ('delivered', 'delivery_update'),updatedat,NULL)) AS delivered_time,
						  max(IF(lower(`data`.status) IN ('delivered', 'delivery_update'),updatedat,NULL))  AS last_delivered_time,
						  min(IF(lower(`data`.status) IN ('delivery_update'),updatedat,NULL))  AS first_delivery_update_time,
						  max(IF(lower(`data`.status) IN ('delivery_update'),updatedat,NULL))  AS last_delivery_update_time,
						  min(IF(lower(`data`.status) IN ('dispatched_to_merchant'),updatedat,NULL)) AS first_dispatched_to_merchant_time,
						  min(IF(lower(`data`.status) = 'undelivered_attempted'  AND   `data`.vendor_id NOT IN (200,207,242),updatedat,NULL)) AS 3pl_first_ofd_time,
						  max(IF(lower(`data`.status) = 'undelivered_attempted'  AND   `data`.vendor_id NOT IN (200,207,242),updatedat,NULL)) AS 3pl_last_ofd_time,
						  min(If(lower(`data`.status) = 'out_for_delivery' and `data`.shipment_type = 'forward',updatedat,NULL)) as first_ofd_time,
						  max(If(lower(`data`.status) = 'out_for_delivery' and `data`.shipment_type = 'forward',updatedat,NULL)) as last_ofd_time,
						  min(IF(lower(`data`.status) = 'ready_for_pickup',updatedat,NULL)) AS first_rfp_time,
						  max(IF(lower(`data`.status) = 'ready_for_pickup',updatedat,NULL)) AS last_rfp_time,
						  min(IF(lower(`data`.status) = 'pickup_addedtopickupsheet',updatedat,NULL)) AS first_picksheet_creation_time,
						  max(IF(lower(`data`.status) = 'pickup_addedtopickupsheet',updatedat,NULL)) AS last_picksheet_creation_time,
						  min(IF(lower(`data`.status) = 'pickup_out_for_pickup',updatedat,NULL))  AS first_rvp_pickup_time,
						  max(IF(lower(`data`.status) = 'pickup_out_for_pickup',updatedat,NULL))  AS last_rvp_pickup_time,
						  min(IF(lower(`data`.status) IN ('received', 'undelivered_not_attended','error'),updatedat,NULL)) AS received_time,
						  min(IF(lower(`data`.status) IN ('received', 'undelivered_not_attended','error')  AND   `data`.current_address.type IN ('DELIVERY_HUB', 'BULK_HUB'),updatedat,NULL)) AS dh_receive_time,
						  max(IF(lower(`data`.status) IN ('received', 'undelivered_not_attended','error')  AND   `data`.current_address.type IN ('DELIVERY_HUB', 'BULK_HUB'),updatedat,NULL)) AS last_dh_receive_time,
						  min(IF(lower(`data`.status) IN ('received', 'undelivered_not_attended','error')  AND   `data`.current_address.type IN ('PICKUP_CENTER'),updatedat,NULL)) AS first_received_pc_time,
						  max(IF(lower(`data`.status) IN ('received', 'undelivered_not_attended','error')  AND   `data`.current_address.type IN ('PICKUP_CENTER'),updatedat,NULL)) AS last_received_pc_time,
						  min(IF(lower(`data`.status) IN ('received')  AND   `data`.shipment_type        IN ('approved_rto', 'unapproved_rto'),updatedat,NULL)) AS rto_first_received_time,
						  min(IF(`data`.status IN ('Received', 'Undelivered_Not_Attended', 'Error')  AND   `data`.current_address.type = 'MOTHER_HUB',updatedat,NULL)) AS first_mh_tc_receive_time,
						  max(IF(lower(`data`.status) IN ('received', 'undelivered_not_attended','error')  AND   `data`.current_address.type = 'MOTHER_HUB',updatedat,NULL)) AS last_mh_tc_receive_time,
						  max(IF(lower(`data`.status) IN ('received', 'error', 'undelivered_not_attended'),updatedat,NULL)) AS received_last_time,
						  sum( CASE 
								   WHEN `data`.shipment_type = 'forward' 
								   AND   `data`.status IN ('Out_For_Delivery', 'out_for_delivery') THEN 1
								   ELSE 0 
								END) AS fsd_number_of_ofd_attempts,
						  sum( CASE 
								   WHEN `data`.shipment_type in ('approved_rto','unapproved_rto')
								   AND   `data`.status IN ('Out_For_Delivery', 'out_for_delivery') THEN 1
								   ELSE 0 
								END) AS rto_number_of_ofd_attempts,
						  sum(IF(`data`.status = 'pickup_out_for_pickup'  AND   `data`.source_address.type LIKE'%NON_FBF%',1,0))  AS out_for_pickup_attempts,
						  min(IF(`data`.shipment_type LIKE'%rto',updatedat,NULL)) AS rto_create_time,
						  min(IF(lower(`data`.status) IN ('returned_to_seller', 'received_by_merchant', 'delivered', 'delivery_update'),updatedat,NULL)) AS rto_complete_time,
						  max(IF(lower(`data`.status) IN ('returned_to_seller', 'received_by_merchant', 'delivered', 'delivery_update'),updatedat,NULL)) AS reverse_complete_time,
						  sum(IF(`data`.status = 'PICKUP_Out_For_Pickup',1,0)) AS shipment_rvp_pk_number_of_attempts,
						  min(IF(`data`.status = 'expected',updatedat,NULL))   AS received_at_source_facility,
						  min(IF(`data`.status = 'Expected'  AND   `data`.current_address.type = 'DELIVERY_HUB',updatedat,NULL)) AS reverse_pickup_hub_time,
						  min(IF(`data`.status = 'pickup_complete',updatedat,NULL))  AS received_at_origin_facility
					 FROM   bigfoot_journal.dart_wsr_scp_ekl_shipment_4 
					 WHERE  day > date_format(date_sub(CURRENT_DATE,140),'yyyyMMdd') 
					 	AND `data`.source_address.id NOT IN (563,564,565,566,567,1280,1282,1288,1511,1757,1950,2043,3594,3612,3620,3621,3622,3623)
					    AND `data`.destination_address.id NOT IN (563,564,565,566,567,1280,1282,1288,1511,1757,1950,2043,3594,3612,3620,3621,3622,3623)
						AND `data`.current_address.type <> 'BULK_HUB'
					 GROUP BY entityid 
			) s_group 
  ON  s_group.shipment_id = s_current.entityid 
LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__fc_profiler_volumetric_estimate_final_hive_fact vol
  ON  s_current.entityid = vol.shipment_id
LEFT OUTER JOIN 
                ( SELECT  refid  AS shipment_id,
						  max(`data`.merchant_id) AS merchant_id,
						  max(`data`.vas_ids[0])  AS vas_type,
						  max(`data`.merchant_reference_id)   AS merchant_reference_id,
						  max(`data`.cost_of_breach.value) AS cost_of_breach,
						  max(IF(size(`data`.logistics_service_offering)>= 2,`data`.logistics_service_offering[1],NULL))   AS service_tier,
						  max(IF(size(`data`.logistics_service_offering)>= 3,`data`.logistics_service_offering[2],NULL))   AS surface_mandatory_flag,
						  max(IF(`data`.cost_of_breach.value >= 20000
						            , "SDD"
									, IF(`data`.cost_of_breach.value >= 15000
									        ,'SDD_Pilot'
											, IF(`data`.cost_of_breach.value >= 1000
											        ,'NDD'
													, IF(`data`.cost_of_breach.value >= 100
													        ,'NDD_Pilot'
															,'Normal'))))) AS shipment_priority_flag
                         FROM     bigfoot_journal.dart_wsr_scp_ekl_b2clogisticsrequest_1 
						 lateral VIEW explode(`data`.ekl_reference_ids) reference_id   AS refid
                         WHERE    day > date_format(date_sub(CURRENT_DATE,120),'yyyyMMdd') 
                         GROUP BY refid
				) b2clogisticsrequest 
  ON  (  s_current.entityid = b2clogisticsrequest.shipment_id) 
LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__ekl_notes_fact notes 
  ON  (	s_current.vendor_tracking_id = notes.vendor_tracking_id 
		and notes.vendor_tracking_id <> 'not_assigned')
WHERE COALESCE(fkl_mh.serviceability,'non-large') <> 'Large'
	AND COALESCE(rev_fkl_mh.serviceability,'non-large') <> 'Large';
