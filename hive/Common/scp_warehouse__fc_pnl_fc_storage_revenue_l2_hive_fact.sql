INSERT OVERWRITE TABLE fc_pnl_fc_storage_revenue_l2_hive_fact 
select
a.inventory_item_warehouse_id as fc_pnl_fc_storage_warehouse,
b.fc_product_detail_hive_dim_key as fc_pnl_fc_storage_product_id_key,
a.date_key as fc_pnl_fc_storage_date_key,
sum(b.product_detail_volume*a.quantity*0.000578) as fc_pnl_fc_storage_space_utilized_in_cubic_feet,
sum(b.product_detail_volume*a.quantity*0.000578*c.fc_pnl_rate_inventory_storage/30) as fc_pnl_fc_storage_actual_space_used_revenue,
sum(a.quantity) as fc_pnl_fc_storage_units
from bigfoot_external_neo.scp_warehouse__fc_inventory_history_snapshot_l0_hive_fact as a
 left outer join bigfoot_external_neo.scp_warehouse__fc_product_detail_hive_dim as b
 on a.inventory_item_wid=b.product_detail_wid and a.warehouse_company=b.product_detail_warehouse_company
left outer join bigfoot_common.fc_pnl_rate_card as c on 
substring((a.date_key),1,6) =c.fc_pnl_rate_month
where if(b.product_detail_volume is null,0,b.product_detail_volume) >= c.fc_pnl_rate_fc_inbound_volume_bucket_min and
if(b.product_detail_volume is null,0,b.product_detail_volume) < c.fc_pnl_rate_fc_inbound_volume_bucket_max 
and a.inventory_item_storage_location_type not in 
('disposal_area',
'disposal_bulk',
'external_inspection_area',
'external_inspection_bulk',
'external_liquidation_damaged_area',
'external_liquidation_damaged_bulk',
'external_liquidation_non_damaged_area',
'external_liquidation_non_damaged_bulk',
'external_liquidation_packing_table',
'fraud_area',
'fraud_bulk',
'prexo_liquidation_packing_table',
'product_exchange_area',
'product_exchange_bulk',
'product_missing_bulk',
'qc_bulk',
'refurbishment_area',
'refurbishment_bulk',
'refurbishment_packing_table',
'reprocess_table',
'return_qc_pass_bulk',
'return_reprocessed_bulk',
'returns_center_bulk',
'returns_excess_bulk',
'returns_supplier_return_area',
'returns_supplier_return_bulk',
'returns_supplier_return_packing_table',
'scan_wsn_bulk',
'seller_return_area',
'seller_return_bulk',
'seller_unidentified_area',
'seller_unidentified_bulk',
'verification_pending_area',
'verification_pending_bulk',
'refinishing')
group by a.inventory_item_warehouse_id
,
b.fc_product_detail_hive_dim_key,
a.date_key 
