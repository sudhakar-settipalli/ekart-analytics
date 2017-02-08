insert overwrite table profiler_weight_volume_l0_hive_fact
	--change_log
	--feb17
	--reverted volumetric conversion factor to 6000
	--added jbn vendor
	--fixed ekl facility to motherhub mapping
	--profiled_volume in grams remove  shipment_l0_90 conversion
	--check lbh units 
	--feb23
	--added shipment current status col
select 
		s_view.data.vendor_tracking_id as vendor_tracking_id,
		s_view.entityid  as shipment_id,
		round(s_profiler.length,2) as profiled_length,
		round(s_profiler.breadth,2) as profiled_breadth,
		round(s_profiler.height,2) as profiled_height,
		--volumetric conversion rounding off to tens of grams
		round((round(s_profiler.length,2) * round(s_profiler.breadth,2) * round(s_profiler.height,2))/6,-1) as profiled_volume_in_gms,
		s_profiler.physical as shipment_dead_weight,
		s_profiler.updated_by as profiled_flag,
		case 
		   when lower(s_view.data.source_address.type) = 'warehouse' and lower(s_view.data.shipment_items[0].seller_id) = 'wsr'
		   then 'WSR'
		   when lower(s_view.data.source_address.type) = 'warehouse' and lower(s_view.data.shipment_items[0].seller_id) = 'myn'
		   then 'MYN'
		   when lower(s_view.data.source_address.type) = 'warehouse' and lower(s_view.data.shipment_items[0].seller_id)='jbn_209'
		   then 'JBN'
		   when lower(s_view.data.source_address.type) = 'warehouse'
		   then 'FA'
		   when lower(s_view.data.source_address.type) = 'mp_non_fbf_seller'
		   then 'Non-FA'
		   when lower(s_view.data.source_address.type) = 'customer' and lower(s_view.data.shipment_items[0].seller_id) = 'myn'
		   then 'MYN'
		   when lower(s_view.data.source_address.type) = 'customer' and lower(s_view.data.shipment_items[0].seller_id) = 'wsr'
		   then 'WSR'
		   when lower(s_view.data.source_address.type) = 'customer' and lower(s_view.data.shipment_items[0].seller_id) = 'jbn_209'
		   then 'JBN'
		   when lower(s_view.data.source_address.type) = 'customer' and lower(s_view.data.destination_address.type)='warehouse'
		   then 'FA'
		   when lower(s_view.data.source_address.type) = 'customer'
		   then 'Non-FA'
		   else null
		 end as seller_type,
		-- If(lower(s_view.data.source_address.type) = 'warehouse',
		--     If(lower(s_view.data.shipment_items[0].seller_id) = 'wsr',
		--         'WSR',
		--         if(lower(s_view.data.shipment_items[0].seller_id) = 'myn',
		--             'MYN',
		--             if(lower(s_view.data.shipment_items[0].seller_id)='jbn_209',
		--                 'JBN',
		--                 'FA'
		--                 )
		--             )
		--         ),
		--     if(lower(s_view.data.source_address.type) = 'mp_non_fbf_seller',
		--         'Non-FA',
		--         if(lower(s_view.data.source_address.type) = 'customer',
		--             If(lower(S_view.data.shipment_items[0].seller_id) = 'myn',
		--                 'MYN',
		--                 If(lower(S_view.data.shipment_items[0].seller_id) = 'wsr',
		--                     'WSR',
		--                     if(lower(s_view.data.shipment_items[0].seller_id) = 'jbn_209',
		--                         'JBN',
		--                         If(lower(S_view.data.destination_address.type)='warehouse',
		--                             'FA',
		--                             'Non-FA'
		--                             )
		--                         )
		--                     )
		--                 ),
		--             null
		--             )
		--         )
		--     ) as seller_type,
		s_view.data.shipment_items[0].seller_id as seller_id,
		s_view.data.shipment_items[0].product_id as first_product_id,
		lookupkey('product_id',s_view.data.shipment_items[0].product_id) as first_product_id_key,
		size(s_view.data.shipment_items) as no_of_items,
		s_view.data.shipment_items[0].quantity as first_item_quantity,
		s_view.data.shipping_category as shipping_category,
		s_view.data.shipment_type as ekl_shipment_type,
		coalesce(FKL_MH.mh_facility_id,s_profiler.current_address_id) as profiled_hub_id,
		if(FKL_MH.mh_facility_id is not null
		        ,'MOTHER_HUB'
				,s_profiler.current_address_type) as profiled_hub_type,
		s_profiler.updated_at as profiled_datetime,
		s_view.data.created_at as shipment_created_at_datetime,
		lookup_date(s_profiler.updated_at) as profiled_date_key,
		lookup_time(s_profiler.updated_at) as profiled_time_key,
		lookup_date(s_view.data.created_at) as shipment_created_date_key,
		lookup_time(s_view.data.created_at) as shipment_created_time_key,
		s_ph.updated_at as pickup_datetime,
		lookup_date(s_ph.updated_at) as pickup_date_key,
		lookup_time(s_ph.updated_at) as pickup_time_key,
		s_ph.current_address_id  as ph_hub_id,
		s_ph.current_address_type as ph_hub_type,
		lookupkey('facility_id',s_ph.current_address_id) as ph_hub_key,
		lookupkey('facility_id',coalesce(FKL_MH.mh_facility_id,s_profiler.current_address_id)) as profiled_hub_key,
		--volumetric conversion 
		floor((round(s_profiler.length,2) * round(s_profiler.breadth,2) * round(s_profiler.height,2))/(6*500))*500 as profiled_vol_weight_bucket,
		floor(s_profiler.physical/500)*500 as shipment_dead_weight_bucket,
		if(s_profiler.length * s_profiler.breadth * s_profiler.height = 0
		       ,1
			   ,0) as is_zero_lbh,
		if(s_profiler.height is not null
		       ,1
			   ,0) as is_profiled,
		prod.analytic_vertical as analytic_vertical,
		prod.analytic_super_category as analytic_super_category,
		prod.analytic_sub_category as analytic_sub_category,
		prod.analytic_category as analytic_category,
		s_view.data.assigned_address.id as assigned_hub_id,
		lookupkey('facility_id',s_view.data.assigned_address.id) as assigned_hub_id_key,
		s_view.data.status as shipment_current_status,
		b2CLogisticsRequest.merchant_reference_id AS merchant_reference_id,
		b2CLogisticsRequest.merchant_id AS merchant_id,
		If(concat_ws("-",s_view.data.attributes) like '%dangerous%'
		       ,1
			   ,0
			) as shipment_dg_flag,
		If(b2CLogisticsRequest.shipment_priority_flag is NULL
		       ,'Normal'
		       ,b2CLogisticsRequest.shipment_priority_flag
		   ) AS shipment_priority_flag,
		S_view.data.shipment_weight.physical as shipment_physical_weight,
		floor(S_view.data.shipment_weight.physical/500)*500 as shipment_physical_weight_bucket,
		b2CLogisticsRequest.service_tier as service_tier,
		b2CLogisticsRequest.surface_mandatory_flag as surface_mandatory_flag,
		seller_lbhw.seller_dead_weight,
		floor(seller_lbhw.seller_dead_weight/500)*500 AS seller_dead_weight_bucket,
		seller_lbhw.seller_volumetric_weight as seller_volumetric_weight,
		floor((seller_lbhw.seller_volumetric_weight)/500)*500 AS seller_vol_weight_bucket,
		s_view.data.contour_volume as contour_volume
from bigfoot_snapshot.dart_wsr_scp_ekl_shipment_4_view as s_view
	left join 
			(select
					vendor_tracking_id,
					updated_at,
					current_address_id,
					current_address_type,
					length,
					breadth,
					height,
					physical,
					updated_by
					from 
						(select
								vendor_tracking_id,
								updated_at,
								current_address_id,
								current_address_type,
								updatedat,
								length,
								breadth,
								height,
								physical,
								updated_by,
								ROW_NUMBER() over (PARTITION BY vendor_tracking_id order by updatedat ASC
								ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as rank 
							FROM
								(select data.vendor_tracking_id as vendor_tracking_id,
										data.updated_at as updated_at,
										data.current_address.id as current_address_id,
										data.current_address.type as current_address_type,
										data.shipment_weight.volumetric_details.length as length,
										data.shipment_weight.volumetric_details.breadth as breadth,
										data.shipment_weight.volumetric_details.height as height,
										data.shipment_weight.physical as physical,
										data.shipment_weight.updated_by as updated_by,
										updatedat as updatedat
								from bigfoot_journal.dart_wsr_scp_ekl_shipment_4 
								where data.shipment_weight.volumetric_details.length is not null 
									and data.shipment_weight.updated_by='gor' 
									and  day > #120#DAY# 
								) i 
						)ranked where ranked.rank = 1
			) s_profiler
				on  s_profiler.vendor_tracking_id = s_view.data.vendor_tracking_id
	left join 
			(select
					vendor_tracking_id,
					updated_at,
					current_address_id,
					current_address_type
				from 
					(select
							vendor_tracking_id,
							updated_at,
							current_address_id,
							current_address_type,
							updatedat,
							ROW_NUMBER() over (PARTITION BY vendor_tracking_id order by updatedat ASC
							 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as rank 
					FROM
						(select 
								data.vendor_tracking_id as vendor_tracking_id,
								data.updated_at as updated_at,
								data.current_address.id as current_address_id,
								data.current_address.type as current_address_type,
								updatedat as updatedat
						from bigfoot_journal.dart_wsr_scp_ekl_shipment_4 
						where data.status = 'pickup_complete' 
							AND day > #120#DAY# 
							AND data.source_address.type like '%NON_FBF%'
						 ) i2 
					)ranked2 
					where ranked2.rank = 1
			 ) s_ph
				on s_ph.vendor_tracking_id = s_view.data.vendor_tracking_id
	left join bigfoot_external_neo.sp_product__product_categorization_hive_dim prod
		on s_view.data.shipment_items[0].product_id = prod.product_id
	left join bigfoot_common.ekl_fkl_facility_mother_hub_mapping FKL_MH
		on s_profiler.current_address_id = FKL_MH.fkl_facility_id
	LEFT OUTER JOIN
				(SELECT refid AS shipment_id,
				data.merchant_id AS merchant_id,
				data.vas_ids[0] AS vas_type,
				max(data.merchant_reference_id) AS merchant_reference_id,
				data.cost_of_breach.value AS cost_of_breach,
				IF(size(data.logistics_service_offering)>=2,data.logistics_service_offering[1],NULL) as service_tier,
				IF(size(data.logistics_service_offering)>=3,data.logistics_service_offering[2],NULL) as surface_mandatory_flag,
				case 
				   when data.cost_of_breach.value >= 20000
				   then 'SDD'
				   when data.cost_of_breach.value >= 15000
				   then 'SDD_Pilot'
				   when data.cost_of_breach.value >= 1000
				   then 'NDD'
				   when data.cost_of_breach.value >= 100
				   then 'NDD_Pilot'
				   else 'Normal'
				 end AS shipment_priority_flag
				   -- IF(data.cost_of_breach.value >= 20000, "SDD", IF(data.cost_of_breach.value >= 15000, 'SDD_Pilot', IF(data.cost_of_breach.value >= 1000,  'NDD', IF(data.cost_of_breach.value >= 100, 'NDD_Pilot', 'Normal' ) ))) AS shipment_priority_flag
				FROM bigfoot_snapshot.dart_wsr_scp_ekl_b2clogisticsrequest_1_view 
				     LATERAL VIEW explode(data.ekl_reference_ids) reference_id AS refid
				group by refid,
				data.merchant_id,
				data.vas_ids,
				data.cost_of_breach.value,
				data.logistics_service_offering) b2CLogisticsRequest ON S_view.entityid = b2CLogisticsRequest.shipment_id
	left join (select 
	               order_item_unit_tracking_id, 
				   return_item_shipment_id
				from bigfoot_external_neo.scp_oms__order_item_unit_s0_fact as oiu
				join bigfoot_external_neo.scp_rrr__return_l1_fact as rl1
				on rl1.order_item_id = oiu.order_item_id
				group by order_item_unit_tracking_id,return_item_shipment_id
				 ) as oius0
           on oius0.return_item_shipment_id = s_view.data.vendor_tracking_id
	left join (select  
						vendortrackingid, 
						-- for seller volumetric lbh/5 and for profiler it is lbh/6 and rounding off to tens of grams
						round(cast(round(length,2) * round(breadth,2) * round(height,2)/(5.0) as double),-1) as seller_volumetric_weight, 
						round(cast(weight as double),2) as seller_dead_weight
				from (select merchantreferenceid, 
								vendortrackingid, 
								quantity, 
								length, 
								breadth, 
								height, 
								weight, 
								history_created_at, 
								rank() over (partition by vendortrackingid order by history_created_at desc ) as latest_version
								from bigfoot_external_neo.scp_fulfillment__fulfillment_cartman_weight_history_fact 
								where  ship_created_date_key > lookup_date(date_add(current_timestamp(),-120))
									and weight_source_type = 'SELLER_PACKING_DETAILS' 
						) as fcwh
				where fcwh.latest_version = 1
				) as seller_lbhw			
		ON        if(s_view.data.shipment_type = 'rvp',oius0.order_item_unit_tracking_id, s_view.data.vendor_tracking_id) = seller_lbhw.vendortrackingid;
