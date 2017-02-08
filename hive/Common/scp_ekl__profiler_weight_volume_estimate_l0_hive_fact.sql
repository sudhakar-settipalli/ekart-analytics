INSERT overwrite TABLE profiler_weight_volume_estimate_l0_hive_fact 
SELECT  distinct   a.vendor_tracking_id, 
           a.shipment_id, 
           a.profiled_volume_in_gms, 
           a.shipment_dead_weight, 
           a.first_product_id, 
           b.analytic_vertical, 
           b.analytic_super_category, 
           b.analytic_sub_category, 
           b.analytic_category, 
           a.seller_dead_weight       AS seller_dead_weight, 
           a.seller_volumetric_weight AS seller_volumetric_weight 
FROM       bigfoot_external_neo.scp_ekl__profiler_weight_volume_l0_hive_fact a 
INNER JOIN bigfoot_external_neo.sp_product__product_categorization_hive_dim b 
ON         a.first_product_id = b.product_id 
WHERE      seller_type = 'Non-FA' 
AND        profiled_volume_in_gms IS NOT NULL 
AND        shipment_dead_weight IS NOT NULL 
AND        no_of_items = 1 
           --quant filter 
AND        first_item_quantity = 1 
AND        shipment_dead_weight < 30000 
AND        profiled_volume_in_gms < 30000 
AND        a.profiled_date_key > 20160116 ;
