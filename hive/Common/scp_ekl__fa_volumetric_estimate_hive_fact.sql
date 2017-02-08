INSERT overwrite TABLE fa_volumetric_estimate_hive_fact 
SELECT a.vendor_tracking_id, 
       a.shipment_id, 
       a.shipment_dead_weight, 
       a.profiled_flag, 
       a.seller_type, 
       a.no_of_items, 
       a.shipping_category, 
       a.ekl_shipment_type, 
       a.profiled_hub_id, 
       a.profiled_hub_type, 
       a.shipment_created_at_datetime, 
       fc_vol.packing_box_used_name      AS packing_box_used_name, 
       fc_vol.suggested_packing_box_name AS suggested_packing_box_name,
	   IF(fc_vol.warehouse_vol_source IS NULL
	       , fsn_median_weight.median_fsn_vol_wt
		   , If (a.seller_type IN ('MYN', 'JBN')
		         , a.shipment_dead_weight
				 , If(fc_vol.warehouse_vol_source IN ('Warehouse_Fsn_lbh', 'Warehouse_cms_estimate')
				      , sum_warehouse_vol_wt
					  , max_warehouse_vol_wt) 
				) 
		  ) as volumetric_weight,
	   IF(fc_vol.warehouse_vol_source IS NULL
	      , 'Warehouse_Fsn_lbh'
		  , IF(a.seller_type = 'MYN'
		       , 'Myntra_jabong_shipment_dead_weight'
			   , fc_vol.warehouse_vol_source) 
		) AS volumetric_weight_source, 
		fc_vol.warehouse_vol_source,
        fsn_median_weight.median_fsn_vol_wt,
        sum_warehouse_vol_wt,
        max_warehouse_vol_wt,
		a.first_product_id as product_detail_fsn
	FROM bigfoot_external_neo.scp_ekl__profiler_weight_volume_l0_hive_fact a 
	LEFT JOIN ( SELECT   box_tracking_id                AS vendor_tracking_id, 
						  sum(warehouse_vol_wt)           AS sum_warehouse_vol_wt, 
						  max(warehouse_vol_wt)           AS max_warehouse_vol_wt, 
						  max(packing_box_used_name)      AS packing_box_used_name, 
						  max(packing_box_suggested_name) AS suggested_packing_box_name, 
						  max(warehouse_vol_source)       AS warehouse_vol_source, 
						  max(product_cms_vertical)       AS product_cms_vertical 
                   FROM     ( SELECT    warehouse_company, 
                                      shipment_item_id, 
                                      shipment_item_product_id, 
                                      shipment_item_quantity, 
                                      shipment_item_status, 
                                      shipment_id, 
                                      shipment_courier_name, 
                                      shipment_display_id, 
                                      shipment_warehouse_id, 
                                      shipment_status, 
                                      box_used_packing_box_id, 
                                      box_actual_weight, 
                                      box_suggested_packing_box_id, 
                                      box_tracking_id, 
                                      packing_box_used_outer_name as packing_box_used_name, 
                                      packing_box_used_outer_length as packing_box_used_length, 
                                      packing_box_suggested_breadth, 
                                      packing_box_suggested_height, 
                                      packing_box_suggested_bucket, 
                                      fc_item.product_cms_vertical, 
                                      packing_box_suggested_name, 
									case 
										when fc_item.product_cms_vertical = 'book' 
											and (packing_box_used_outer_length * packing_box_used_outer_breadth *packing_box_used_outer_height) < 10000
										then (packing_box_used_outer_length * packing_box_used_outer_breadth * packing_box_used_outer_height * 0.7)/0.36614
										when fc_item.product_cms_vertical in ('mobile','tablet') 
											and box_suggested_packing_box_id is not null 
											and packing_box_suggested_name  like 'B%' 
											and (packing_box_suggested_breadth * packing_box_suggested_height * packing_box_suggested_length * 1.05) < 10000
										then (packing_box_suggested_breadth * packing_box_suggested_height * packing_box_suggested_length * 1.05)/0.36614
										when packing_box_used_outer_name like 'S%' 
											and (product_length * product_breadth * product_height * 1.2) < 10000
										then (product_length * product_breadth * product_height * 1.2 * shipment_item_quantity)/0.36614
										when packing_box_used_outer_name is not null 
											and (packing_box_used_outer_length * packing_box_used_outer_breadth * packing_box_used_outer_height) < 10000
										then (packing_box_used_outer_length * packing_box_used_outer_breadth * packing_box_used_outer_height)/0.36614
									else (cms_est.median_cms_vol_wt * shipment_item_quantity)/0.36614
									end as warehouse_vol_wt,
									
									case 
										when fc_item.product_cms_vertical in ('mobile','tablet') 
											and box_suggested_packing_box_id is not null 
											and packing_box_suggested_name  like 'B%'
											and (packing_box_suggested_breadth * packing_box_suggested_height * packing_box_suggested_length * 1.05) < 10000
										then 'Warehouse_primary_packing_box'
										when packing_box_used_outer_name like 'S%' 
											and (product_length * product_breadth * product_height * 1.2) < 10000
										then 'Warehouse_Fsn_lbh'
										when packing_box_used_outer_name is not null 
											and (packing_box_used_outer_length * packing_box_used_outer_breadth * packing_box_used_outer_height) < 10000
										then 'Warehouse_Packing_box'
										else 'Warehouse_cms_estimate'
									end as warehouse_vol_source
                                FROM      bigfoot_external_neo.scp_warehouse__fc_shipment_item_l0_hive_fact fc_item
                                LEFT JOIN bigfoot_external_neo.scp_warehouse__fc_cms_vertical_estimate_l1_hive_fact cms_est
                                   ON        fc_item.product_cms_vertical = cms_est.product_cms_vertical 
							) fc_est
         GROUP BY box_tracking_id
		 )fc_vol 
		    ON a.vendor_tracking_id = fc_vol.vendor_tracking_id 
		 LEFT JOIN ( SELECT   product_detail_fsn, 
							  percentile(cast((product_detail_length * product_detail_breadth * product_detail_height * 1.2)/0.36614 AS int),0.5) AS median_fsn_vol_wt
					 FROM     bigfoot_external_neo.scp_warehouse__fc_product_detail_hive_dim 
					 WHERE    ( product_detail_length * product_detail_breadth * product_detail_height) < 10000
					 GROUP BY product_detail_fsn 
					) as fsn_median_weight 
		    ON a.first_product_id = fsn_median_weight.product_detail_fsn 
		 WHERE a.seller_type IN ('MYN','FA','WSR','JBN');
