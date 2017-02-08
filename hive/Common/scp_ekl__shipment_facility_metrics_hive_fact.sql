INSERT OVERWRITE TABLE shipment_facility_metrics_hive_fact
SELECT          /*+ STREAMTABLE(b,mhb,outbag)*/
                a.vendor_tracking_id, 
                a.seller_type, 
                a.shipment_priority_flag, 
                a.shipment_dg_flag, 
                a.ekl_shipment_type, 
                a.service_tier, 
                a.ekart_lzn_flag,
				IF(a.shipment_dg_flag = true
				        ,'DG'
						, If(a.shipment_direction_flag = 'forward'
						        , If(a.service_tier = 'Economy Delivery'
								        ,'ECONOMY'
										, If(a.service_tier = 'Next Day Delivery' OR  a.service_tier = 'Same Day Delivery'
										        ,'PRIORITY'
												, If(a.service_tier = 'Standard Delivery' OR a.service_tier IS NULL 
												        ,'REGULAR'
														,NULL ) 
											) 
									)
								, 'DG' 
							) 
					) as shipment_type, 
                a.shipment_current_status, 
                a.shipment_direction_flag, 
                a.shipment_direction_flag_out, 
                a.reverse_shipment_type, 
                a.incoming_consignment_id, 
                a.incoming_bag_id, 
                a.incoming_bag_tracking_id, 
                a.incoming_hop_source_type, 
                a.incoming_bag_category, 
                a.incoming_bag_service_type, 
                date_format(a.incoming_consignment_create_datetime,'yyyy-MM-dd HH:mm:ss')   AS incoming_consignment_create_datetime, 
                lookup_date(a.incoming_consignment_create_datetime) AS incoming_consignment_create_date_key,
                date_format(a.incoming_consignment_received_datetime,'yyyy-MM-dd HH:mm:ss') AS incoming_consignment_received_datetime, 
                a.outgoing_consignment_id, 
                a.outgoing_bag_tracking_id, 
                a.outgoing_bag_category, 
                a.outgoing_bag_service_type, 
                date_format(a.outgoing_consignment_create_datetime,'yyyy-MM-dd HH:mm:ss')   AS outgoing_consignment_create_datetime, 
                lookup_date(a.outgoing_consignment_create_datetime) AS outgoing_consignment_create_date_key,
                date_format(a.outgoing_consignment_received_datetime,'yyyy-MM-dd HH:mm:ss') AS outgoing_consignment_received_datetime, 
                date_format(a.shipment_facility_inscan_datetime,'yyyy-MM-dd HH:mm:ss')   AS shipment_facility_inscan_datetime, 
                lookup_date(a.shipment_facility_inscan_datetime) AS shipment_facility_inscan_date_key,
                date_format(a.shipment_facility_outscan_datetime,'yyyy-MM-dd HH:mm:ss')  AS shipment_facility_outscan_datetime, 
                lookup_date(a.shipment_facility_outscan_datetime)   AS shipment_facility_outscan_date_key,
                --pendency timestamps 
                date_format(b.bag_shipment_first_inscan_datetime,'yyyy-MM-dd HH:mm:ss')  AS bag_shipment_first_inscan_datetime,
                date_format(IF (a.incoming_bag_destination_hub_inscan_datetime > a.incoming_consignment_received_datetime, a.incoming_bag_destination_hub_inscan_datetime, a.incoming_consignment_received_datetime),'yyyy-MM-dd HH:mm:ss') AS incoming_bag_destination_hub_inscan_datetime, 
                date_format(a.shipment_created_at_datetime,'yyyy-MM-dd HH:mm:ss')  AS shipment_created_at_datetime,
                lookup_date(a.shipment_created_at_datetime)   AS shipment_created_at_date_key,
                date_format(a.shipment_delivered_at_datetime,'yyyy-MM-dd HH:mm:ss')   AS shipment_delivered_at_datetime,
                lookup_date(a.shipment_delivered_at_datetime) AS shipment_delivered_at_date_key,
                date_format(a.shipment_first_received_at_datetime,'yyyy-MM-dd HH:mm:ss') AS shipment_first_received_at_datetime,
                lookup_date(a.shipment_first_received_at_datetime)  AS shipment_first_received_at_date_key,
                date_format(a.shipment_dispatch_datetime,'yyyy-MM-dd HH:mm:ss') AS shipment_dispatch_datetime,
                date_format(a.customer_promise_datetime,'yyyy-MM-dd HH:mm:ss')  AS customer_promise_datetime,
                date_format(a.logistics_promise_datetime,'yyyy-MM-dd HH:mm:ss') AS logistics_promise_datetime,
                lookup_date(a.customer_promise_datetime)   AS customer_promise_date_key,
                lookup_date(a.logistics_promise_datetime)  AS logistics_promise_date_key,
                --breach columns 
                date_format(COALESCE(mhb.first_bag_closed_datetime,tcb.first_bag_closed_datetime),'yyyy-MM-dd HH:mm:ss') AS first_bag_closed_datetime,
                COALESCE(lookup_time(timestamp(from_unixtime(mhb.ideal_cutoff-19800))),lookup_time(timestamp(from_unixtime(tcb.ideal_cutoff-19800))))  AS ideal_connection_cutoff_time_key,
                COALESCE(lookup_time(timestamp(from_unixtime(mhb.actual_cutoff-19800))),lookup_time(timestamp(from_unixtime(tcb.actual_cutoff-19800))))   AS actual_connection_cutoff_time_key,
                lookupkey('facility_id',COALESCE(mhb.actual__first_hop_id,tcb.actual__first_hop_id))   AS actual_first_hop_facility_key,
                lookupkey('facility_id',COALESCE(mhb.ideal_first_hop_id,tcb.ideal_first_hop_id)) AS ideal_first_hop_facility_key,
                COALESCE(mhb.ideal_connection_id,tcb.ideal_connection_id)   AS ideal_connection_id,
                IF(a.shipment_direction_flag = 'forward',a.fsd_assigned_hub_id,a.shipment_origin_mh_facility_id)   AS breach_destination_facility_id,
                lookupkey('facility_id',IF(a.shipment_direction_flag = 'forward',a.fsd_assigned_hub_id,a.shipment_origin_mh_facility_id))  AS breach_destination_facility_id_key,
                IF(a.incoming_hop_source_type = 'MP_BAG', 'FM_MH_HOP', IF(a.incoming_consignment_source_hub_id = a.incoming_bag_source_hub_id, IF(a.incoming_consignment_source_hub_id = a.shipment_origin_mh_facility_id, 'SORT', 'RESORT' ), 'TRANSIT' ) ) AS sort_resort_mp_flag,
                IF(a.incoming_hop_source_type = 'MP_BAG',1,IF(a.incoming_consignment_destination_hub_id = a.outgoing_consignment_source_hub_id,1,0))   AS continuity_flag,
                a.outgoing_consignment_source_hub_id  AS pendency_metrics_facility_id,
                lookupkey('facility_id',a.outgoing_consignment_source_hub_id)  AS pendency_metrics_facility_id_key,
                a.incoming_consignment_source_hub_id  AS breach_first_mh_metrics_facility_id,
                lookupkey('facility_id',a.incoming_consignment_source_hub_id)  AS breach_first_mh_metrics_facility_id_key,
                a.shipment_origin_mh_facility_id, 
                lookupkey('facility_id',a.shipment_origin_mh_facility_id) AS shipment_origin_mh_facility_id_key,
                a.fsd_assigned_hub_id, 
                lookupkey('facility_id',a.fsd_assigned_hub_id) AS fsd_assigned_hub_id_key,
                lookupkey('facility_id',a.shipment_origin_facility_id)  AS shipment_origin_facility_id_key,
                IF(unix_timestamp(a.shipment_facility_outscan_datetime) 
							- IF(a.incoming_hop_source_type = 'MP_BAG'
									,unix_timestamp(b.bag_shipment_first_inscan_datetime)
									,unix_timestamp(a.outgoing_bag_receive_datetime))> 45*60
					, 1
					, 0) AS bag_shipments_inscan_pendency_gte_45min_nom,
                IF (a.incoming_hop_source_type = 'MP_BAG' AND a.outgoing_consignment_source_hub_id = a.shipment_origin_mh_facility_id
				        , 'MP'
						,IF(a.incoming_consignment_source_hub_id = a.incoming_bag_source_hub_id AND a.incoming_consignment_source_hub_id IS NOT NULL
						        , 'RESORT'
								,'NOT_APPLICABLE') 
					) AS bag_shipments_inscan_pendency_flag,
                IF (a.incoming_hop_source_type = 'MP_BAG' AND a.outgoing_consignment_source_hub_id = a.shipment_origin_mh_facility_id
				        , 1
						,IF(a.outgoing_consignment_source_hub_id = a.outgoing_bag_source_hub_id AND a.outgoing_consignment_source_hub_id IS NOT NULL
						        , 1
								,0)
					)  AS inscan_pendency_denom,
                unix_timestamp(a.shipment_facility_outscan_datetime) - unix_timestamp(b.bag_shipment_first_inscan_datetime) AS bag_shipments_inscan_pendency_actual_value,
                IF(unix_timestamp(a.shipment_facility_outscan_datetime) 
				                        - unix_timestamp(IF (a.incoming_bag_destination_hub_inscan_datetime > a.incoming_consignment_received_datetime
										                            , a.incoming_bag_destination_hub_inscan_datetime
																	, a.incoming_consignment_received_datetime)
														) > 12*60*60 
									AND unix_timestamp(a.shipment_facility_outscan_datetime) 
									         - unix_timestamp(IF (a.incoming_bag_destination_hub_inscan_datetime > a.incoming_consignment_received_datetime
											                        , a.incoming_bag_destination_hub_inscan_datetime
																	, a.incoming_consignment_received_datetime)
																) <24*60*60
						, 1
						, 0
					)   AS shipment_inscan_pendency_gte_12hrs_lte_24hrs_nom,
                IF( unix_timestamp(a.shipment_facility_outscan_datetime) 
				                        - unix_timestamp(IF (a.incoming_bag_destination_hub_inscan_datetime > a.incoming_consignment_received_datetime
										                        , a.incoming_bag_destination_hub_inscan_datetime
																, a.incoming_consignment_received_datetime)
														) > 24*60*60 
						, 1
						, 0
					) AS shipment_inscan_pendency_gte_24hrs_nom,
                IF(a.outgoing_consignment_id IS NOT NULL
				    ,1
					,0)  AS shipment_inscan_pendency_denom,
                unix_timestamp(a.shipment_facility_outscan_datetime) 
				                    - unix_timestamp(IF (a.incoming_bag_destination_hub_inscan_datetime > a.incoming_consignment_received_datetime
									                        , a.incoming_bag_destination_hub_inscan_datetime
															, a.incoming_consignment_received_datetime)
													)   AS shipment_inscan_pendency_actual_value,
                IF(a.incoming_consignment_source_hub_id = a.shipment_origin_mh_facility_id 
				                AND unix_timestamp(a.shipment_first_received_at_datetime) 
								        - unix_timestamp(a.shipment_dispatch_datetime) > 24*60*60
						, 1
						, 0
					) AS dispatch_to_inscan_24hrs_pendency_nom,
                IF(a.incoming_consignment_source_hub_id = a.shipment_origin_mh_facility_id
				        , 1
						,0)   AS first_mh_metrics_denom,
                unix_timestamp(a.shipment_first_received_at_datetime) 
				        - unix_timestamp(a.shipment_dispatch_datetime) AS dispatch_to_inscan_pendency_actual_value,
                IF( a.shipment_delivered_at_datetime IS NOT NULL AND a.ekl_shipment_type = 'forward' 
				                AND ( unix_timestamp(a.customer_promise_datetime)
								            -unix_timestamp(a.shipment_delivered_at_datetime) < 0)
						,1
						,0) AS cpd_breach_nom, 
                IF (a.shipment_delivered_at_datetime IS NOT NULL AND a.ekl_shipment_type = 'forward' 
				                    AND ( unix_timestamp(a.logistics_promise_datetime) 
									                -unix_timestamp(a.shipment_delivered_at_datetime) < 0)
						,1
						,0) AS lpd_breach_nom, 
                IF(a.ekl_shipment_type = 'forward'
				        ,1
						,0)   AS promise_breach_denom,
                IF(a.outgoing_consignment_create_datetime IS NOT NULL OR a.incoming_consignment_received_datetime IS NOT NULL
				        , IF(unix_timestamp(a.outgoing_consignment_create_datetime) 
						                - unix_timestamp(a.incoming_consignment_received_datetime) > 26.5* 60* 60
								, '>26.5HRS'
								, IF(unix_timestamp(a.outgoing_consignment_create_datetime) 
								                    - unix_timestamp(a.incoming_consignment_received_datetime) > 25.5 * 60* 60
											, '>25.5HRS'
											, IF(unix_timestamp(a.outgoing_consignment_create_datetime) 
											                - unix_timestamp(a.incoming_consignment_received_datetime) > 24* 60* 60
													, '>24HRS'
													, IF(unix_timestamp(a.outgoing_consignment_create_datetime) 
													            - unix_timestamp(a.incoming_consignment_received_datetime) > 12* 60* 60
															, '>12HRS'
															, IF(unix_timestamp(a.outgoing_consignment_create_datetime) 
															            - unix_timestamp(a.incoming_consignment_received_datetime) > 6* 60* 60
																	, '>6HRS'
																	, IF(unix_timestamp(a.outgoing_consignment_create_datetime) 
																	            - unix_timestamp(a.incoming_consignment_received_datetime) > 4* 60* 60
																		, '>4HRS'
																		, IF( unix_timestamp(a.outgoing_consignment_create_datetime) 
																		        - unix_timestamp(a.incoming_consignment_received_datetime) > 2* 60* 60
																			, '>2HRS'
																			, '<2HRS' 
																			) 
																		) 
																) 
														) 
												) 
									) 
							)
						, 'TIMESTAMPS_NULL' 
					) AS inscan_outscan_pendency_time_buckets,
                IF(a.shipment_first_received_at_datetime IS NOT NULL OR a.shipment_dispatch_datetime IS NOT NULL 
				                AND (a.incoming_consignment_source_hub_id = a.shipment_origin_mh_facility_id)
						, IF(unix_timestamp(a.shipment_first_received_at_datetime) 
						                - unix_timestamp(a.shipment_dispatch_datetime) > 24* 60* 60
								, '>24HRS'
								, IF(unix_timestamp(a.shipment_first_received_at_datetime) 
								                - unix_timestamp(a.shipment_dispatch_datetime) > 6 * 60* 60
										, '>6HRS'
										, IF(unix_timestamp(a.shipment_first_received_at_datetime) 
										            - unix_timestamp(a.shipment_dispatch_datetime) > 4* 60* 60
												, '>4HRS'
												, IF(unix_timestamp(a.shipment_first_received_at_datetime) 
												            - unix_timestamp(a.shipment_dispatch_datetime) > 2* 60* 60
														, '>2HRS'
														, IF(unix_timestamp(a.shipment_first_received_at_datetime) 
														                - unix_timestamp(a.shipment_dispatch_datetime) > 30* 60
																, '>30MIN'
																, IF(unix_timestamp(a.shipment_first_received_at_datetime) 
																            - unix_timestamp(a.shipment_dispatch_datetime) > 20* 60
																		, '>20MIN'
																		, IF(unix_timestamp(a.shipment_first_received_at_datetime) 
																		            - unix_timestamp(a.shipment_dispatch_datetime) > 10* 60
																				, '>10MIN'
																				, '<10MIN' 
																			) 
																	) 
															) 
													) 
											) 
									) 
							)
						, 'TIMESTAMPS_NULL' 
					) AS dispatch_inscan_pendency_time_buckets,
                IF(a.shipment_facility_inscan_datetime IS NOT NULL 
				                AND a.incoming_consignment_source_hub_id IS NOT NULL 
				                AND a.incoming_consignment_source_hub_id = a.incoming_bag_source_hub_id
						,1
						,0
					) AS mh_breach_60_denom,
                IF(a.shipment_facility_inscan_datetime IS NOT NULL 
				                AND a.incoming_consignment_source_hub_id IS NOT NULL
						,1
						,0
					) AS tc_connection_breach_denom,
                mhb.mh_breach_flag  AS mh_breach_60_nom,
                IF(a.incoming_consignment_source_hub_id = a.incoming_bag_source_hub_id
				        , mhb.connection_cutoff_breach_flag
						, tcb.connection_cutoff_breach_flag )  AS tc_connection_breach_nom,
                date_format(COALESCE(mhb.mh_received_at_datetime,tcb.mh_received_at_datetime),'yyyy-MM-dd HH:mm:ss')  AS mh_received_at_datetime,
                lookup_date(COALESCE(mhb.mh_received_at_datetime,tcb.mh_received_at_datetime))   AS mh_received_at_date_key,
                IF(a.outgoing_hop_source_type = 'MP_BAG'
				        , 'FM_MH_HOP'
						, IF(a.outgoing_consignment_source_hub_id = a.outgoing_bag_source_hub_id
						        , IF(a.outgoing_consignment_source_hub_id = a.shipment_origin_mh_facility_id
								        , 'SORT'
										, 'RESORT' 
									)
								, 'TRANSIT' 
							) 
					) AS sort_resort_mp_flag_out,
                a.incoming_bag_source_hub_receive_datetime, 
                IF  ( outbag.bag_first_closed_datetime IS NOT NULL 
				                OR IF(a.incoming_bag_destination_hub_inscan_datetime > a.incoming_consignment_received_datetime
								        , a.incoming_bag_destination_hub_inscan_datetime
										, a.incoming_consignment_received_datetime
									) IS NOT NULL
						, IF(unix_timestamp(outbag.bag_first_closed_datetime) 
						                    - unix_timestamp(IF (a.incoming_bag_destination_hub_inscan_datetime > a.incoming_consignment_received_datetime
											                        , a.incoming_bag_destination_hub_inscan_datetime
																	, a.incoming_consignment_received_datetime
																)) > 26 * 60* 60
								, '>26HRS'
								, IF(unix_timestamp(outbag.bag_first_closed_datetime) 
								                - unix_timestamp(IF (a.incoming_bag_destination_hub_inscan_datetime > a.incoming_consignment_received_datetime
												                        , a.incoming_bag_destination_hub_inscan_datetime
																		, a.incoming_consignment_received_datetime
																	)) > 25.5 * 60* 60
										, '>25.5HRS'
										, IF(unix_timestamp(outbag.bag_first_closed_datetime) 
										                - unix_timestamp(IF (a.incoming_bag_destination_hub_inscan_datetime > a.incoming_consignment_received_datetime
														                        , a.incoming_bag_destination_hub_inscan_datetime
																				, a.incoming_consignment_received_datetime
																			)) > 24* 60* 60
												, '>24HRS'
												, IF(unix_timestamp(outbag.bag_first_closed_datetime) 
												                - unix_timestamp(IF (a.incoming_bag_destination_hub_inscan_datetime > a.incoming_consignment_received_datetime
																                        , a.incoming_bag_destination_hub_inscan_datetime
																						, a.incoming_consignment_received_datetime
																				    )) > 12* 60* 60
														, '>12HRS'
														, IF(unix_timestamp(outbag.bag_first_closed_datetime) 
														                - unix_timestamp(IF (a.incoming_bag_destination_hub_inscan_datetime > a.incoming_consignment_received_datetime
																		                        , a.incoming_bag_destination_hub_inscan_datetime
																								, a.incoming_consignment_received_datetime
																						    )) > 6* 60* 60
																, '>6HRS'
																, IF(unix_timestamp(outbag.bag_first_closed_datetime) 
																                - unix_timestamp(IF (a.incoming_bag_destination_hub_inscan_datetime > a.incoming_consignment_received_datetime
																				                        , a.incoming_bag_destination_hub_inscan_datetime
																										, a.incoming_consignment_received_datetime
																									)) > 4* 60* 60
																		, '>4HRS'
																		, IF( unix_timestamp(outbag.bag_first_closed_datetime) 
																		                - unix_timestamp(IF (a.incoming_bag_destination_hub_inscan_datetime > a.incoming_consignment_received_datetime
																												, a.incoming_bag_destination_hub_inscan_datetime
																												, a.incoming_consignment_received_datetime
																											)) > 2* 60* 60
																				, '>2HRS'
																				, '<2HRS' 
																			) 
																	) 
															) 
													) 
											) 
									) 
							)
						, 'TIMESTAMPS_NULL' 
	                ) AS mh_bag_inscan_outscan_pendency_time_buckets,
                IF(outbag.bag_first_closed_datetime IS NOT NULL 
				                OR IF (a.incoming_bag_destination_hub_inscan_datetime > a.incoming_consignment_received_datetime
								        , a.incoming_bag_destination_hub_inscan_datetime
										, a.incoming_consignment_received_datetime
									    ) IS NOT NULL
						, (unix_timestamp(outbag.bag_first_closed_datetime) 
						                - unix_timestamp(IF (a.incoming_bag_destination_hub_inscan_datetime > a.incoming_consignment_received_datetime
										                        , a.incoming_bag_destination_hub_inscan_datetime
																, a.incoming_consignment_received_datetime
															)
														)
							)
						,NULL
				) AS mh_bag_inscan_outscan_pendency_in_seconds,
                NULL  AS incoming_bag_closed_datetime,
                outbag.bag_first_closed_datetime  AS outgoing_bag_closed_datetime,
                a.incoming_consignment_source_hub_id_0, 
                a.incoming_consignment_source_hub_id_0__key, 
                a.incoming_consignment_create_datetime_0, 
                a.incoming_consignment_received_datetime_0, 
                NULL AS ideal_p1, 
                NULL AS ideal_p2, 
                NULL AS ideal_p3, 
                NULL AS ideal_p4, 
                NULL AS ideal_primary_start_time, 
                NULL AS ideal_primary_end_time, 
                NULL AS ideal_slot_type, 
                NULL AS actual_bag_close_time, 
                NULL AS shipment_lzn_classification, 
                NULL AS ideal_slot_name, 
                NULL AS ideal_connection_time_key, 
                NULL AS ideal_rule_date, 
                NULL AS current_facility_id_key, 
                NULL AS actual_p1, 
                NULL AS actual_p2, 
                NULL AS actual_p3, 
                NULL AS actual_p4, 
                NULL AS actual_slot_name, 
                NULL AS actual_primary_start_time, 
                NULL AS actual_primary_end_time, 
                NULL AS actual_slot_type, 
                NULL AS actual_rule_date, 
                a.incoming_consignment_connection_id, 
                a.outgoing_consignment_connection_id, 
                a.incoming_connection_cut_off_datetime, 
                a.outgoing_connection_cut_off_datetime, 
                IF  ((upper(geo_src.lt) = upper(geo_dest.lt) OR upper(geo_src.city)=upper(geo_dest.city))
				        , "INTRACITY"
						, IF(upper(geo_src.geo_zone) = upper(geo_dest.geo_zone)
						        , "INTRAZONE"
								, IF(upper(geo_src.geo_zone) <> upper(geo_dest.geo_zone)
								        , "INTERZONE"
										, "Missing"
									)
							)
					) AS ekl_fin_zone,
				IF (a.incoming_bag_source_hub_id=a.shipment_origin_mh_facility_id and a.incoming_bag_destination_hub_id=a.fsd_assigned_hub_id,'positive_bag','resort_bag') as positive_bag_flag,
				a.sort_resort_new_flag,
				a.incoming_consignment_mode,
				date_format(a.fsd_first_dh_received_datetime,'yyyy-MM-dd HH:mm:ss') as fsd_first_dh_received_datetime,
				a.ekl_fin_zone as actual_lzn_flag
				FROM bigfoot_external_neo.scp_ekl__shipment_facility_l2_90_fact AS a

LEFT JOIN       bigfoot_external_neo.scp_ekl__shipment_facility_bag_first_shipment_inscan_fact b
ON              a.incoming_consignment_id = b.incoming_consignment_id 
AND             a.incoming_bag_id = b.incoming_bag_id 

LEFT JOIN  (SELECT facility.facility_id as facility_id, 
					`data`.local_territory as lt, 
					`data`.city   AS city,
					`data`.zone   AS geo_zone,
					`data`.state  AS geo_state, 
					facility.postal_code					
			FROM bigfoot_external_neo.scp_ekl__ekl_facility_hive_dim  as facility 
			JOIN bigfoot_snapshot.dart_fki_scp_ekl_geo_1_view_total AS geo 
			ON   geo.`data`.pincode=facility.postal_code
			) as geo_src
ON              geo_src.facility_id=a.incoming_consignment_source_hub_id 

LEFT JOIN  (SELECT facility.facility_id as facility_id, 
					`data`.local_territory as lt, 
					`data`.city   AS city,
					`data`.zone   AS geo_zone,
					`data`.state  AS geo_state,
					facility.postal_code				
			FROM bigfoot_external_neo.scp_ekl__ekl_facility_hive_dim  as facility 
			JOIN bigfoot_snapshot.dart_fki_scp_ekl_geo_1_view_total AS geo 
			ON   geo.`data`.pincode=facility.postal_code
			) as geo_dest
ON              geo_dest.facility_id=a.incoming_consignment_destination_hub_id 

-- LEFT JOIN    (  SELECT data.lzn_classification   AS shipment_lzn_classification,
--                               cast(data.transit_distance AS int) AS shipment_transit_distance,
--                               data.transit_time   AS shipment_transit_time, 
--                               data.source_pincode AS source_pincode, 
--                               data.destination_pincode  AS destination_pincode 
--                        FROM   bigfoot_journal.dart_fkint_scp_ekl_geo_classification_2 
--                        WHERE  data.lzn_classification IS NOT NULL) AS lzn_cons
-- ON              geo_src.postal_code= lzn_cons.source_pincode 
-- AND             geo_dest.postal_code = lzn_cons.destination_pincode 

LEFT JOIN       bigfoot_external_neo.scp_ekl__sbr_tc_breach_60_fact tcb 
ON              a.incoming_consignment_id = tcb.incoming_consignment_id
AND             a.vendor_tracking_id = tcb.vendor_tracking_id

LEFT JOIN       bigfoot_external_neo.scp_ekl__sbr_mh_breach_60_fact mhb 
ON              a.incoming_consignment_id = mhb.incoming_consignment_id
AND             a.vendor_tracking_id = mhb.vendor_tracking_id  

LEFT JOIN       bigfoot_external_neo.scp_ekl__bag_l1_90_fact AS outbag 
ON              a.outgoing_bag_id = outbag.bag_id 
WHERE  a.incoming_consignment_id IS NOT NULL;

