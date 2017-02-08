
INSERT overwrite TABLE ekl_logistics_profiler_revenue_new_hive_fact
-- changes 3pl handover charges 
-- NDD air flag 
-- returns processing charges 
-- replacement 10 rs discount 
-- ROI express and standard 
-- RVP revenue = RVP Validation service (Rs 20 per shipment ) + RVP rate 
SELECT   x.vendor_tracking_id, 
         x.shipment_fa_flag, 
         x.seller_id, 
         x.lzn, 
         lookup_date(x.shipment_dispatch_datetime)                        AS shipment_dispatch_date_key, 
         lookup_time(x.shipment_dispatch_datetime)                        AS shipment_dispatch_time_key, 
         lookup_date(x.shipment_first_received_at_datetime)               AS shipment_received_at_date_key,
         lookup_time(x.shipment_first_received_at_datetime)               AS shipment_received_at_time_key,
         lookup_date(x.received_at_origin_facility_datetime)              AS shipment_received_at_origin_facility_date_key,
         lookup_time(x.received_at_origin_facility_datetime)              AS shipment_received_at_origin_facility_time_key,
         x.shipment_value                                                 AS shipment_value, 
         (x.billable_weight/1000)                                         AS billable_weight,
		 IF(x.sla_tier <> 'non-express', 'Express', 'Non-Express')         as sla_bucket,
         concat(x.weight_bucket,'-',(x.weight_bucket+500))                AS weight_bucket, 
         sum(x.delivery_revenue)                                          AS delivery_revenue,
         sum(x.forward_revenue)                                           AS forward_revenue,
         sum(x.rto_revenue)                                               AS rto_revenue, 
         max(x.cod_revenue)                                               AS cod_revenue, 
         sum(x.priority_shipment_revenue)                                 AS priority_shipment_revenue,
         sum(x.priority_shipment_incremental_revenue)                     AS priority_shipment_incremental_revenue,
         sum(x.priority_shipment_revenue+x.cod_revenue)                   AS vas_revenue, 
         sum(x.rvp_revenue)                                            AS rvp_revenue, 
         sum(x.first_mile_revenue)                                        AS first_mile_revenue,
         sum(x.handover_revenue_3pl)                                      AS handover_revenue_3pl,
         sum(x.pos_revenue)                                               AS pos_revenue, 
         x.asp_bucket                                                     AS asp_bucket, 
         sum(x.risk_surcharge)                                            AS risk_surcharge, 
         sum(x.forward_revenue+x.rto_revenue+x.priority_shipment_revenue) AS rto_revenue_total,
         x.first_mile_revenue_flag                                        AS first_mile_revenue_flag,
         lookupkey('pincode',x.source_pincode)                            AS source_pincode_key,
         lookupkey('pincode',x.destination_pincode)                       AS destination_pincode_key,
         x.air_flag                                                       AS air_flag, 
         x.reverse_shipment_type                                          AS reverse_shipment_type, 
         x.ekart_lzn_flag                                                 AS ekart_lzn_flag, 
         sum(x.return_processing_charges)                                 AS return_processing_charges, 
         sum(x.replacement_discount)                                      AS replacement_discount,
		 rto_creation_place,
		   max(x.shipment_dead_weight/1000) as shipment_dead_weight,
		   concat(max(x.shipment_dead_weight_bucket),'-',(max(x.shipment_dead_weight_bucket)+500)) as shipment_dead_weight_bucket,
		   max(x.profiled_volume_in_gms/1000) as profiled_volumetric_weight,
		   concat(max(x.profiled_volume_weight_bucket),'-',(max(x.profiled_volume_weight_bucket)+500)) as profiled_volumetric_weight_bucket,
		   max(x.seller_dead_weight/1000) as seller_dead_weight,
		   concat(max(x.seller_dead_weight_bucket),'-',(max(x.seller_dead_weight_bucket)+500)) as seller_dead_weight_bucket,
		   max(x.seller_volumetric_weight/1000) as seller_volumetric_weight,
		   concat(max(x.seller_volumetric_weight_bucket),'-',(max(x.seller_volumetric_weight_bucket)+500)) as seller_volumetric_weight_bucket,
		   max(x.non_fa_volumetric_estimate/1000) as non_fa_volumetric_estimate,
	       max(x.non_fa_volumetric_estimate_source) as non_fa_volumetric_estimate_source,		   
		   max(x.profiled_billable_weight/1000) as profiled_billable_weight,
		   concat(max(x.profiled_billable_weight_bucket),'-',(max(x.profiled_billable_weight_bucket)+500)) as profiled_billable_weight_bucket, 
		   max(profiled_dead_weight_at_source) as profiled_dead_weight_at_source,
		   max(profiled_volumetric_weight_at_source) as profiled_volumetric_weight_at_source,
		   max(CASE when (x.shipment_dead_weight is null 
		                or x.shipment_dead_weight = 0)
						and (x.seller_dead_weight is null or x.seller_dead_weight = 0)
					then 2222
		            when (x.shipment_dead_weight is null 
		                or x.shipment_dead_weight = 0)
					then 8888
					when (x.seller_dead_weight is null or x.seller_dead_weight = 0)
					then 9999
					else floor(x.shipment_dead_weight/500) - floor(x.seller_dead_weight/500)
				end ) as Prof2seller_dead_wt_bucket_jump,
		   max(CASE when (x.profiled_volume_in_gms is null or  x.profiled_volume_in_gms = 0)
		            and (x.seller_volumetric_weight is null or x.seller_volumetric_weight = 0)
					then 2222
					when (x.profiled_volume_in_gms is null or  x.profiled_volume_in_gms = 0)
		            then 8888
					when (x.seller_volumetric_weight is null or x.seller_volumetric_weight = 0)
					then 9999
				    else floor(x.profiled_volume_in_gms/500) - floor(x.seller_volumetric_weight/500)
				end ) as Prof2seller_vol_wt_bucket_jump,
		   concat(floor(max( greatest(x.shipment_dead_weight, x.profiled_volume_in_gms)/500))*500,'-',floor(max( greatest(x.shipment_dead_weight, x.profiled_volume_in_gms )/500))*500+500) as max_profiled_weight_bucket,
		   concat(floor(max( greatest(x.seller_dead_weight, x.seller_volumetric_weight )/500))*500,'-',floor(max( greatest(x.seller_dead_weight, x.seller_volumetric_weight)/500))*500+500) as max_seller_weight_bucket,
		   max(x.product_detail_fsn) as product_detail_fsn,
		   max(x.median_fsn_vol_wt) as median_fsn_vol_wt,
		   max(x.order_id) as order_id,
		   max(x.order_external_id) as order_external_id,
           LOOKUPKEY('seller_id',x.seller_id) AS seller_hive_dim_key
FROM     ( 
                          SELECT           s.vendor_tracking_id, 
                                           s.shipment_first_received_at_datetime, 
                                           rc.lzn, 
                                           s.actual_month, 
                                           s.ekart_lzn_flag, 
                                           s.payment_mode, 
                                           s.reverse_shipment_type, 
                                           s.shipment_dispatch_datetime, 
                                           s.received_at_origin_facility_datetime, 
                                           s.ekl_shipment_type, 
                                           s.shipment_priority_flag, 
                                           s.payment_type, 
                                           s.shipment_value, 
                                           s.shipment_fa_flag, 
                                           s.seller_id, 
                                           s.logistics_promise_datetime, 
                                           s.ekl_fin_zone, 
                                           s.billable_weight, 
                                           floor(s.billable_weight/500)*500 as weight_bucket, 
                                           s.asp_bucket, 
                                           s.air_flag, 
                                           s.source_pincode, 
                                           s.destination_pincode,
                                           s.seller_type,
                                           IF(shipment_carrier = 'FSD',  
										         IF(s.shipment_priority_flag = 'Normal' OR s.shipment_priority_flag IS NULL, 
												        IF(s.ekl_shipment_type = 'forward' AND s.shipment_current_status NOT IN ('pickup_leg_complete','pickup_leg_completed'), 
														       rc.forward_rate, 
															   0 ), 
														0 ), 
												 0 ) AS delivery_revenue,
                                           IF(s.shipment_carrier = 'FSD' AND s.seller_type IN ('Non-FA', 'FA', 'WSR'), 
										          s.shipment_value * 0.001, 
												  0 ) AS risk_surcharge,
												  
                                           IF(shipment_carrier = 'FSD', 
										          IF((s.shipment_priority_flag = 'Normal' OR s.shipment_priority_flag IS NULL), 
												        IF(s.ekl_shipment_type NOT IN ('rvp', 'merchant_return') AND s.shipment_current_status NOT IN ('pickup_leg_complete', 'pickup_leg_completed'), 
														       rc.forward_rate, 
															   0 ), 
														0 ), 
													0 ) AS forward_revenue,
                                           IF(shipment_carrier = 'FSD', 
										          IF(lower(s.ekl_shipment_type) LIKE '%rto%', 
												        rc.rto_rate, 
														0 ), 
												  0 ) AS rto_revenue,
										   IF(  shipment_carrier = 'FSD' 
										            AND lower(ekl_shipment_type) in ('rvp', 'approved_rto','unapproved_rto') 
											   , case when s.seller_type <> 'Non-FA'
                                                      then 5											   
											          when s.seller_type = 'Non-FA' 
													  AND no_first_mile_shipments_on_rvp_first_received_date  >= 200
											          then rc.nonfa_returns_process_fee_200
													  when s.seller_type = 'Non-FA' 
													  AND no_first_mile_shipments_on_rvp_first_received_date  >= 50 
													  AND no_first_mile_shipments_on_rvp_first_received_date < 200
													  then rc.nonfa_returns_process_fee_50_200
													  when s.seller_type = 'Non-FA' 
													  AND no_first_mile_shipments_on_rvp_first_received_date  >= 10 
													  AND no_first_mile_shipments_on_rvp_first_received_date < 50
													  then rc.nonfa_returns_process_fee_10_50
													  when s.seller_type = 'Non-FA' 
													  then rc.nonfa_returns_process_fee_0_10
													else 0
												 end 
											   , 0 ) AS return_processing_charges,
										   IF(shipment_carrier = 'FSD', 
										         IF(((s.payment_type = 'COD' 
												             AND ( s.payment_mode = 'COD' OR s.payment_mode IS NULL) 
														     AND s.ekl_shipment_type = 'forward' ) 
												             AND s.shipment_current_status NOT IN ('pickup_leg_complete', 'pickup_leg_completed')), 
												       IF(s.shipment_value<20000, 
													          rc.cod_collection_charge_lt_20k, 
															  rc.cod_collection_charge_gte_20k ) 
													   , 0 )
											     , 0 ) AS cod_revenue,
										   
                                           IF(shipment_carrier = 'FSD', 
										         IF(s.payment_type = 'COD' AND s.ekl_shipment_type = 'forward', 
												         IF(s.payment_mode = 'POS', 
														      0.012 * s.shipment_value, 
															  0 ), 
														 0 ), 
													0 ) AS pos_revenue,
										   IF(shipment_carrier = 'FSD', 
										         (IF(s.shipment_priority_flag LIKE 'NDD%', 
												        IF(s.air_flag = 1 or rc.lzn not in ('LOCAL','ZONAL'),
														       rc.ndd_charge_air,rc.ndd_charge) , 
															   0 ) 
														+ IF(s.shipment_priority_flag LIKE 'SDD%', 
														       rc.sdd_charge, 
															   0 ) 
												  ), 
											0 ) AS priority_shipment_revenue,
                                           --priority  incremental revenue 
                                           IF(shipment_carrier = 'FSD', 
										         IF(( s.shipment_priority_flag LIKE 'NDD%' OR s.shipment_priority_flag LIKE 'SDD%'), 
												         (IF(s.shipment_priority_flag LIKE 'NDD%', 
														         IF(s.air_flag = 1 or rc.lzn not in ('LOCAL','ZONAL'),
																         rc.ndd_charge_air,
																		 rc.ndd_charge) , 
																 0 ) 
														 + IF(s.shipment_priority_flag LIKE 'SDD%', 
														         rc.sdd_charge, 
																 0 ) - rc.forward_rate ), 
														 0 ), 
												 0 ) AS priority_shipment_incremental_revenue, 
                                           -- RVP revenue = RVP Validation service (Rs 20 per shipment ) + RVP rate
                                           IF(shipment_carrier = 'FSD', 
										         IF(s.ekl_shipment_type = 'rvp', 
												        rc.rvp_rate+20, 
														0 ), 
												 0 ) AS rvp_revenue,
                                           IF(shipment_carrier = 'FSD', 
										         IF(lower(s.reverse_shipment_type) = 'replacement' AND s.ekl_shipment_type = 'rvp',
												       10,
													   0), 
												 0) AS replacement_discount,
                                           --Need to modify the logic,need to fix rate based on no of shipments per day 
                                           IF(shipment_carrier ='FSD' 
										       AND s.seller_type = 'Non-FA' 
											   AND s.shipment_current_status <> 'not_received' 
											   AND ekl_shipment_type NOT IN ('rvp', 'merchant_return'),
												   case 
													   when z.no_of_daily_first_mile_shipments >= 300 
													   then rc.first_mile_rate_300
													   when z.no_of_daily_first_mile_shipments < 300 
													        AND z.no_of_daily_first_mile_shipments >=200 
													   then rc.first_mile_rate_200_300
													   when z.no_of_daily_first_mile_shipments < 200 
													        AND z.no_of_daily_first_mile_shipments >=70 
													   then rc.first_mile_rate_70_200
													   when z.no_of_daily_first_mile_shipments < 70 
													        AND z.no_of_daily_first_mile_shipments >=50 
													   then rc.first_mile_rate_50_70
													   when z.no_of_daily_first_mile_shipments < 50 
													        AND z.no_of_daily_first_mile_shipments >=10 
													   then rc.first_mile_rate_10_50
													   else rc.first_mile_rate_10 
												   end
										   , 0 ) AS first_mile_revenue,
                                           IF(shipment_carrier ='FSD' 
										       AND s.seller_type = 'Non-FA' 
											   AND s.shipment_current_status <> 'not_received' 
											   AND ekl_shipment_type NOT IN ('rvp', 'merchant_return'),
												   case 
													   when z.no_of_daily_first_mile_shipments >= 300 
													   then "GTE_300"
													   when z.no_of_daily_first_mile_shipments < 300 
													        AND z.no_of_daily_first_mile_shipments >=200 
													   then "BW_200_300"
													   when z.no_of_daily_first_mile_shipments < 200 
													        AND z.no_of_daily_first_mile_shipments >=70 
													   then "BW_70_200"
													   when z.no_of_daily_first_mile_shipments < 70 
													        AND z.no_of_daily_first_mile_shipments >=50 
													   then "BW_50_70"
													   when z.no_of_daily_first_mile_shipments < 50 
													        AND z.no_of_daily_first_mile_shipments >=10 
													   then "BW_10_50"
													   else "LTE_10" 
												   end
										   , "NOT_APPLICABLE" ) AS first_mile_revenue_flag,
                                           IF(shipment_carrier = '3PL', 10, 0 )  AS handover_revenue_3pl,
										   sla_tier,
										   rto_creation_place,
										   s.shipment_dead_weight,
										   floor( s.profiled_dead_weight_at_source/500)*500 AS shipment_dead_weight_bucket,
										   s.profiled_volume_in_gms,
										   floor( s.profiled_volume_in_gms/500)*500 AS profiled_volume_weight_bucket,
										   s.seller_dead_weight,
										   floor( s.seller_dead_weight/500)*500 AS seller_dead_weight_bucket,
										   s.seller_volumetric_weight,
										   floor( s.seller_volumetric_weight/500)*500 AS seller_volumetric_weight_bucket,
										   s.non_fa_volumetric_estimate,
										   s.non_fa_volumetric_estimate_source,
										   s.profiled_billable_weight,
										   floor( s.profiled_billable_weight/500)*500 AS profiled_billable_weight_bucket,
										   profiled_dead_weight_at_source,
										   profiled_volumetric_weight_at_source,
										   s.product_detail_fsn,
										   s.median_fsn_vol_wt,
										   s.order_id,
										   s.order_external_id
                          FROM             bigfoot_common.ekl_pnl_revenue_rate_card_2_201607 rc 
                          RIGHT OUTER JOIN  (        SELECT    t.vendor_tracking_id, 
                                                               shipment_first_received_at_datetime,
                                                               shipment_dispatch_datetime, 
                                                               received_at_origin_facility_datetime,
                                                               --use coalesce here 
                                                                -- month(
															    --     IF(shipment_dispatch_datetime IS NULL, 
																--	        IF(shipment_first_received_at_datetime IS NULL
																--			, shipment_created_at_datetime
																--			,shipment_first_received_at_datetime)
																--		, shipment_dispatch_datetime)) AS actual_month,
																month(coalesce(case 
																         when shipment_carrier = '3PL' 
																		 then shipment_first_received_at_datetime
																		 when ekl_shipment_type = 'forward' and shipment_carrier = 'FSD' 
																		      and shipment_current_status IN ('pickup_leg_complete', 'pickup_leg_completed')
																		 then received_at_origin_facility_datetime
																		 when ekl_shipment_type = 'forward' and shipment_carrier = 'FSD'
																		 then if(shipment_delivered_at_datetime >= coalesce(rto_complete_datetime, received_at_origin_facility_datetime), shipment_delivered_at_datetime, coalesce(rto_complete_datetime, received_at_origin_facility_datetime))
																		 when lower(ekl_shipment_type) in ('rvp', 'approved_rto','unapproved_rto')
																		 then rto_complete_datetime																		 
																	end, shipment_first_received_at_datetime, shipment_created_at_datetime)) as actual_month,
																(case 
																         when shipment_carrier = '3PL' 
																		 then 'shipment_first_received_at_datetime'
																		 when ekl_shipment_type = 'forward' and shipment_carrier = 'FSD' 
																		      and shipment_current_status IN ('pickup_leg_complete', 'pickup_leg_completed')
																		 then 'received_at_origin_facility_datetime'
																		 when ekl_shipment_type = 'forward' and shipment_carrier = 'FSD'
																		 then 'shipment_delivered_at_datetime'
																		 when lower(ekl_shipment_type) in ('rvp', 'approved_rto','unapproved_rto')
																		 then 'rto_complete_datetime'
																	end) as actual_month_source,	
                                                               ekl_shipment_type, 
                                                               shipment_priority_flag, 
                                                               payment_type, 
                                                               payment_mode, 
                                                               shipment_value, 
                                                               shipment_fa_flag, 
                                                               shipment_current_status, 
                                                               seller_id, 
                                                               logistics_promise_datetime, 
                                                               ekl_fin_zone, 
                                                               seller_type, 
															   (case 
																   when seller_type = 'Non-FA' 
																	and p.nonfa_billable_weight is not null
																	and p.nonfa_billable_weight <> 0
																   then if(p.nonfa_billable_weight < 15000
																             , p.nonfa_billable_weight
																			 , 14900)
																   when seller_type <> 'Non-FA' 
																     and p.profiled_billable_weight is not null 
																	 and p.profiled_billable_weight <> 0
																   then if(p.profiled_billable_weight < 15000
																             , p.profiled_billable_weight
																			 , 14900)
																   else if(substr(t.vendor_tracking_id,1,3)='FMP', 850, 1400)
																    
															   end ) AS billable_weight,
															   floor((case 
																   when seller_type = 'Non-FA' 
																	and p.nonfa_billable_weight is not null
																	and p.nonfa_billable_weight <> 0
																   then if(p.nonfa_billable_weight < 15000
																             , p.nonfa_billable_weight
																			 , 14900)
																   when seller_type <> 'Non-FA' 
																     and p.profiled_billable_weight is not null 
																	 and p.profiled_billable_weight <> 0
																   then if(p.profiled_billable_weight < 15000
																             , p.profiled_billable_weight
																			 , 14900)
																   else if(substr(t.vendor_tracking_id,1,3)='FMP', 850, 1400)
																   
															   end )/500)*500 AS weight_bucket,
															   -- pushing obnormal weights (>=15kg) into 14.5 to 15 kg bucket
                                                               --IF(p.billable_weight IS NULL OR p.billable_weight = 0, IF(substr(t.vendor_tracking_id,1,3)='FMP', 850, 1400 ), IF(p.billable_weight <15000, p.billable_weight, 14900 )) AS billable_weight,

                                                               --floor( IF(p.billable_weight IS NULL OR p.billable_weight = 0, IF(substr(t.vendor_tracking_id,1,3)='FMP', 850, 1400 ), IF(p.billable_weight <15000, p.billable_weight, 14900 )) /500)*500 AS weight_bucket,
															   case 
															        when shipment_value > 25000 
															          then "ASP>25K"
															        when shipment_value > 20000 
																	  then "ASP_20K_to_25K"
																	when shipment_value > 15000 
																	  then "ASP_15K_to_20K"
																	when shipment_value > 10000 
																	  then "ASP_10K_to_15K"
																	when shipment_value > 5000  
																	  then "ASP_5K_to_10K"
																	else "ASP<5K"
															   end as asp_bucket,
																-- IF(shipment_value >25000,"ASP>25K", IF(shipment_value >20000, "ASP_20K_to_25K", IF(shipment_value>15000, "ASP_15K_to_20K", IF(shipment_value>10000, "ASP_10K_to_15K", IF(shipment_value>5000, "ASP_5K_to_10K", "ASP<5K" ) ) ) )) AS asp_bucket,
															   
                                                               shipment_carrier, 
                                                               source_city, 
                                                               source_pincode, 
                                                               source_state, 
                                                               destination_zone, 
                                                               destination_city, 
                                                               destination_pincode, 
                                                               destination_state, 
                                                               ekart_lzn_flag, 
                                                               reverse_shipment_type, 
                                                               h.air_flag,
															   (case when upper(t.service_tier) = upper('Economy Delivery') then 'non-express' else 'express' end) as sla_tier,
															   rto_creation_place,
															   t.shipment_delivered_at_datetime,
															   t.rto_complete_datetime,
															   t.rto_first_received_datetime,
															    p.shipment_dead_weight,
																p.profiled_volume_in_gms,
																p.seller_dead_weight,
																p.seller_volumetric_weight,
																p.non_fa_volumetric_estimate,
																p.non_fa_volumetric_estimate_source,
																p.profiled_billable_weight,
																p.profiled_dead_weight_at_source,
																p.profiled_volumetric_weight_at_source,
																p.product_detail_fsn,
											                    p.median_fsn_vol_wt,
																h.order_id,
																h.order_external_id
                                                    FROM      bigfoot_external_neo.scp_ekl__shipment_l1_90_fact as t
                                                     LEFT JOIN 
                                                               ( 
                                                                      SELECT vendor_tracking_id,
                                                                             max(coalesce(greatest(seller_volumetric_weight, seller_dead_weight, profiled_dead_weight_at_source),non_fa_volumetric_estimate_source)) AS nonfa_billable_weight,
																			 max(IF (profiled_dead_weight_at_source > volumetric_weight, profiled_dead_weight_at_source,volumetric_weight)) AS profiled_billable_weight,
																			 max(shipment_dead_weight) as shipment_dead_weight,
																			 max(profiled_volume_in_gms) as profiled_volume_in_gms,
																			 max(seller_dead_weight) as seller_dead_weight,
																			 max(seller_volumetric_weight) as seller_volumetric_weight,
																			 max(non_fa_volumetric_estimate) as non_fa_volumetric_estimate,
																			 max(non_fa_volumetric_estimate_source) as non_fa_volumetric_estimate_source,
																			 max(profiled_dead_weight_at_source) as profiled_dead_weight_at_source,
																			 max(profiled_volumetric_weight_at_source) as profiled_volumetric_weight_at_source,
																			 max(product_detail_fsn) as product_detail_fsn,
																			 max(median_fsn_vol_wt) as median_fsn_vol_wt
                                                                      FROM   bigfoot_external_neo.scp_ekl__fc_profiler_volumetric_estimate_final_hive_fact 
																	  group by vendor_tracking_id) p
                                                     ON        p.vendor_tracking_id = t.vendor_tracking_id
                                                     LEFT JOIN 
                                                               ( 
                                                                      SELECT sh.vendor_tracking_id             AS tracking_id,
																	         max((CASE 
																				 WHEN seller_type = 'Non-FA' 
																				   AND shipment_received_at_origin_date_key IS NULL 
																				   AND shipment_first_received_date_key IS NULL 
																				 THEN 'RTOs_before_PH_inscan'
																				 WHEN seller_type = 'Non-FA' 
																				   AND shipment_received_at_origin_date_key IS NOT NULL 
																				   AND shipment_dispatch_date_key IS NULL 
																				   AND shipment_first_received_date_key IS NULL 
																				 THEN 'RTOs_between_pickup_and_dispatch'
																				 WHEN IF(seller_type = 'Non-FA', shipment_dispatched_to_tc_date_key, shipment_dispatch_date_key ) IS NOT NULL 
																				   AND shipment_first_received_date_key IS NULL 
																				 THEN 'RTOs_between_dispatch_and_MH_primary_scan'
																				 WHEN shipment_first_received_date_key IS NOT NULL 
																				   AND first_mh_tc_receive_date_key IS NULL 
																				 THEN 'RTOs_between_inscan_and_bagging'
																				 WHEN first_mh_tc_receive_date_key IS NOT NULL 
																				   AND shipment_first_consignment_create_date_key IS NULL 
																				 THEN 'RTOs_between_bagging_and_MH_outscan'
																				 WHEN shipment_first_consignment_create_date_key IS NOT NULL 
																				   AND fsd_first_dh_received_date_key IS NULL 
																				 THEN 'RTOs_between_MH_outscan_and_DH_Inscan'
																				 WHEN fsd_first_dh_received_date_key IS NOT NULL 
																				   AND fsd_first_ofd_date_key IS NULL 
																				 THEN 'RTOs_between_DH_Inscan_&_OFD'
																				 WHEN fsd_first_ofd_date_key IS NOT NULL 
																				 THEN 'RTOs_post_OFD'
																			 ELSE 'Uncategorised' 
																			 END)) as rto_creation_place,
                                                                             max(IF(sh.number_of_air_hops > 0,1,0)) AS air_flag,
																			 max(order_id) as order_id,
																			 max(order_external_id) as order_external_id
                                                                      FROM   bigfoot_external_neo.scp_ekl__shipment_hive_90_fact sh
                                                                      WHERE  sh.shipment_carrier = 'FSD'
																	  group by vendor_tracking_id) h
                                                     ON        h.tracking_id = t.vendor_tracking_id
                                                     LEFT JOIN 
                                                               ( 
                                                                      SELECT pincode AS destination_pincode,
                                                                             city    AS destination_city,
                                                                             state   AS destination_state,
                                                                             zone    AS destination_zone
                                                                      FROM   bigfoot_external_neo.scp_ekl__logistics_geo_hive_dim) u
                                                               --rvp use source pincode 
                                                     ON        IF(ekl_shipment_type = 'rvp',t.source_address_pincode,t.destination_address_pincode) = u.destination_pincode
                                                     LEFT JOIN 
                                                               ( 
                                                                      SELECT pincode AS source_pincode,
                                                                             city    AS source_city,
                                                                             state   AS source_state
                                                                      FROM   bigfoot_external_neo.scp_ekl__logistics_geo_hive_dim )v
                                                     ON        IF(ekl_shipment_type = 'rvp',t.destination_address_pincode,t.source_address_pincode) = v.source_pincode
                                                     WHERE     t.shipment_carrier IN ('3PL', 'FSD') 
													    AND    t.vendor_tracking_id <> 'not_assigned'
                                                        AND    length(t.vendor_tracking_id) > 5
						                    ) s
                                           --zones names as is from rate card 
                          ON    lower( CASE 
									 WHEN s.ekl_fin_zone = 'INTRACITY' THEN 'LOCAL'
									 WHEN s.ekl_fin_zone IN('INTRAZONE') THEN 'ZONAL'
									 WHEN ( s.source_city IN ('CHENNAI','MUMBAI','NEW DELHI','KOLKATA','BANGALORE','HYDERABAD','AHMEDABAD','PUNE')
													  AND s.destination_city IN ('CHENNAI','MUMBAI','NEW DELHI','KOLKATA','BANGALORE','HYDERABAD','AHMEDABAD','PUNE')
													  AND              s.source_city <>s.destination_city)
													  --need to check sla value with pavan 
											 THEN IF(s.sla_tier <> 'non-express','METRO_EXPRESS','METRO_NONEXPRESS')
									 WHEN s.destination_state IN ('SIKKIM','ASSAM','MANIPUR','MEGHALAYA','MIZORAM','ARUNACHAL PRADESH','NAGALAND','TRIPURA','JAMMU AND KASHMIR') 
											 THEN 'JK_NE'
									 WHEN s.ekart_lzn_flag = 'ROI' AND s.destination_zone IN ('SOUTH','WEST') 
											 THEN IF(s.sla_tier <> 'non-express' ,'ROI_EXPRESS','ROI_STANDARD')
									 WHEN s.ekart_lzn_flag = 'ROI' AND s.destination_zone IN ('EAST','NORTH') 
											 THEN 'ROI_BLENDED'
									 ELSE 'ROI_BLENDED'
                                  END
								) = lower(rc.lzn) 
                          AND rc.rate_card_month = 12 
                          AND s.weight_bucket = rc.weight_bucket_min 
                          LEFT OUTER JOIN 
                                           (    SELECT   seller_id, 
                                                             --use recieved at origin date time 
                                                             to_date(received_at_origin_facility_datetime) AS received_at_origin_facility_date,
                                                             --shipment_received_at_date, 
                                                             count(vendor_tracking_id) AS no_of_daily_first_mile_shipments
                                                    FROM     bigfoot_external_neo.scp_ekl__shipment_l1_90_fact
                                                    WHERE    seller_type='Non-FA' 
                                                    AND      shipment_carrier = 'FSD' 
                                                    GROUP BY seller_id, 
                                                             to_date(received_at_origin_facility_datetime)) z
                          ON s.seller_id = z.seller_id 
						       AND to_date(s.received_at_origin_facility_datetime) = z.received_at_origin_facility_date
                          LEFT OUTER JOIN 
                                           (    SELECT   seller_id, 
                                                             --use recieved at origin date time 
                                                             to_date(received_at_origin_facility_datetime) AS received_at_origin_facility_date,
															 --shipment_received_at_date on a rvp competed date, 
                                                             count(vendor_tracking_id) AS no_first_mile_shipments_on_rvp_first_received_date
                                                    FROM     bigfoot_external_neo.scp_ekl__shipment_l1_90_fact
                                                    WHERE    seller_type='Non-FA' 
                                                    AND      shipment_carrier = 'FSD' 
                                                    GROUP BY seller_id, 
                                                             to_date(received_at_origin_facility_datetime)) z1
                          ON s.seller_id = z1.seller_id 
						       AND to_date(s.rto_complete_datetime) = z1.received_at_origin_facility_date

						  ) x
GROUP BY x.vendor_tracking_id, 
         x.shipment_fa_flag, 
         x.seller_id, 
         x.lzn, 
         x.asp_bucket, 
         lookup_date(x.shipment_dispatch_datetime), 
         lookup_time(x.shipment_dispatch_datetime), 
         lookup_date(x.shipment_first_received_at_datetime), 
         lookup_time(x.shipment_first_received_at_datetime), 
         lookup_date(x.received_at_origin_facility_datetime), 
         lookup_time(x.received_at_origin_facility_datetime), 
         x.shipment_value, 
         x.billable_weight, 
         x.sla_tier, 
         concat(x.weight_bucket,'-',(x.weight_bucket+500)), 
         x.first_mile_revenue_flag, 
         lookupkey('pincode',x.source_pincode), 
         lookupkey('pincode',x.destination_pincode), 
         x.air_flag, 
         x.reverse_shipment_type, 
         x.ekart_lzn_flag, 
         x.rto_creation_place ;
