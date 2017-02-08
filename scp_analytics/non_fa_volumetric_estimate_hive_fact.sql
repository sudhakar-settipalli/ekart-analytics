INSERT overwrite TABLE non_fa_volumetric_estimate_hive_fact 
SELECT    a.vendor_tracking_id, 
          a.shipment_id, 
  		  if(a.shipment_dead_weight is null, 
		        a.shipment_dead_weight,
                V.estimate_profiled_dead_weight 
			) as shipment_dead_weight,
          a.profiled_flag, 
          a.seller_type, 
          a.no_of_items, 
          a.shipping_category, 
          a.ekl_shipment_type, 
          a.profiled_hub_id, 
          a.profiled_hub_type, 
          a.shipment_created_at_datetime, 
          NULL AS     packing_box_used_name, 
          NULL AS     suggested_packing_box_name, 
		  if(a.profiled_volume_in_gms is not null and a.profiled_volume_in_gms < 30000, 
		        a.profiled_volume_in_gms,
                if(V.estimated_volumetric_profiler_weight is not null and V.estimated_volumetric_profiler_weight < 30000, 
				        V.estimated_volumetric_profiler_weight, 
				        null
				  )
            ) as volumetric_weight,
          if(a.profiled_volume_in_gms is not null and a.profiled_volume_in_gms < 30000,
		        'nonfa_profiler',
				if(V.estimated_volumetric_profiler_weight is not null and V.estimated_volumetric_profiler_weight < 30000,
				        V.estimated_volumetric_profiler_weight,
						null
				  ) 
             ) as volumetric_weight_source,
          profiled_volume_in_gms,
		  v.non_fa_volumetric_estimate, 
          v.non_fa_volumetric_estimate_source,
  		  a.seller_dead_weight as seller_dead_weight, 
  		  a.seller_volumetric_weight as seller_volumetric_weight,
  		a.shipment_dead_weight as profiled_dead_weight_at_source,			
		a.profiled_volume_in_gms as profiled_volumetric_weight_at_source,
		a.first_product_id as product_detail_fsn,
		fa_suggested_fsn_wt.fa_suggested_vol_wt as fsn_fa_suggested_box_vol_wt
FROM      bigfoot_external_neo.scp_ekl__profiler_weight_volume_l0_hive_fact a 
LEFT JOIN 
          ( 
                   SELECT   vendor_tracking_id, 
                            sum(estimated_volumetric_weight) AS non_fa_volumetric_estimate,
							sum(estimate_profiled_dead_weight) AS estimate_profiled_dead_weight,
							sum(estimate_seller_dead_weight)   AS estimate_seller_dead_weight,
							sum(estimate_seller_vol_weight)    AS estimate_seller_vol_weight,
							sum(estimate_profiled_vol_weight)  AS estimate_profiled_vol_weight,
							sum(estimated_volumetric_profiler_weight) AS estimated_volumetric_profiler_weight,
                            concat_ws('-',collect_set(estimate_volumetric_source)) AS non_fa_volumetric_estimate_source 
                   FROM     ( 
                                      SELECT    sil190_i1.vendor_tracking_id     AS  vendor_tracking_id,
                                                sil190_i1.product_id     AS  product_id,
                                                COALESCE(fsnve.median_fsn_vol_weight*sil190_i1.item_quantity,
								                         volve.median_vertical_vol_weight*sil190_i1.item_quantity,
								                         catve.median_category_vol_weight*sil190_i1.item_quantity)  as estimated_volumetric_profiler_weight,
												COALESCE(fsnve.median_fsn_dead_weight,volve.median_vertical_dead_weight,catve.median_category_dead_weight)*sil190_i1.item_quantity as estimate_profiled_dead_weight,
												COALESCE(fsnve.fsn_median_seller_dead_weight,volve.vertical_median_seller_dead_weight,catve.category_median_seller_dead_weight)*sil190_i1.item_quantity as estimate_seller_dead_weight,
												COALESCE(fsnve.fsn_median_seller_vol_weight, volve.vertical_median_seller_vol_weight, catve.category_median_seller_vol_weight)*sil190_i1.item_quantity as estimate_seller_vol_weight,
												COALESCE(fsnve.median_fsn_vol_weight,volve.median_vertical_vol_weight,catve.median_category_vol_weight)*sil190_i1.item_quantity as estimate_profiled_vol_weight,
												greatest( 
												          COALESCE(fsnve.median_fsn_dead_weight,volve.median_vertical_dead_weight,catve.median_category_dead_weight), 
														  COALESCE(fsnve.fsn_median_seller_dead_weight,volve.vertical_median_seller_dead_weight,catve.category_median_seller_dead_weight),
														  COALESCE(fsnve.fsn_median_seller_vol_weight, volve.vertical_median_seller_vol_weight, catve.category_median_seller_vol_weight)
														  )*sil190_i1.item_quantity AS  estimated_volumetric_weight,
												case 
													when fsnve.fsn_median_seller_vol_weight IS NOT NULL
														then 'nonfa_fsn_median_estimate'
													when volve.vertical_median_seller_vol_weight IS NOT NULL
														then 'nonfa_vertical_median_estimate'
													when catve.category_median_seller_vol_weight IS NOT NULL
														then 'nonfa_category_median_estimate'
													else 'non_fa_missing_volumetric_estimate'
												end as estimate_volumetric_source
												
												--IF(c.fsn_median_seller_vol_weight IS NOT NULL, 'nonfa_fsn_median_estimate', 
												--       If(d.vertical_median_seller_vol_weight IS NOT NULL, 'nonfa_vertical_median_estimate', 
												--	       If(e.category_median_seller_vol_weight IS NOT NULL, 'nonfa_category_median_estimate', 
												--		   'non_fa_missing_volumetric_estimate') ) ) as estimate_volumetric_source
                                      FROM      bigfoot_external_neo.scp_ekl__shipment_item_l1_90_fact as sil190_i1
                                      LEFT JOIN bigfoot_external_neo.sp_product__product_categorization_hive_dim as pcd
                                      ON        sil190_i1.product_id = pcd.product_id 
                                      LEFT JOIN bigfoot_external_neo.scp_ekl__profiler_weight_volume_fsn_estimate_l1_hive_fact as fsnve
                                      ON        sil190_i1.product_id = fsnve.product_id 
                                      LEFT JOIN bigfoot_external_neo.scp_ekl__profiler_weight_volume_vertical_estimate_l1_hive_fact as volve
                                      ON        pcd.analytic_vertical = volve.analytic_vertical 
                                      LEFT JOIN bigfoot_external_neo.scp_ekl__profiler_weight_volume_category_estimate_l1_hive_fact as catve
                                      ON        pcd.analytic_category = catve.analytic_category 
							) vol_est
                   GROUP BY vendor_tracking_id ) v 
              ON        a.vendor_tracking_id = v.vendor_tracking_id 
		 LEFT JOIN ( SELECT product_detail_fsn, 
							percentile(cast((packing_box_suggested_breadth * packing_box_suggested_height * packing_box_suggested_length)/0.36614 AS int),0.5) AS fa_suggested_vol_wt
						FROM bigfoot_external_neo.scp_warehouse__fc_shipment_item_l0_hive_fact as fc_item
						left join bigfoot_external_neo.scp_warehouse__fc_product_detail_hive_dim as fc_product_dim
						on fc_item.shipment_item_product_key = fc_product_dim.fc_product_detail_hive_dim_key
						join bigfoot_external_neo.scp_warehouse__fc_shipment_l1_hive_fact as fcsl1H 
						on (fc_item.shipment_display_id=fcsl1H.shipment_display_id 
						    and fcsl1H.shipment_quantity=1 )
						where fc_item.shipment_destination_type='customer' 
						and fc_item.shipment_dispatched_date_key >= lookup_date(date_add(current_timestamp(),-90))
						group by product_detail_fsn 
					) as fa_suggested_fsn_wt 
		    ON a.first_product_id = fa_suggested_fsn_wt.product_detail_fsn
WHERE     a.seller_type = 'Non-FA';
