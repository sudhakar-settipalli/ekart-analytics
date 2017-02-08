INSERT OVERWRITE TABLE fc_pnl_rc_long_storage_revenue_l2_hive_fact 
select
a.inventory_item_warehouse_id as fc_pnl_rc_long_storage_warehouse,
b.fc_product_detail_hive_dim_key as fc_pnl_rc_long_storage_product_id_key,
a.date_key as fc_pnl_rc_long_storage_date_key,
a.inventory_item_age_since_received as fc_pnl_rc_long_storage_age,
sum(a.quantity*c.fc_pnl_rate_rc_long_storage) as fc_pnl_fc_long_storage_daily_revenue,
sum(a.quantity)	as fc_pnl_rc_long_storage_units
from bigfoot_external_neo.scp_warehouse__fc_inventory_history_snapshot_l0_hive_fact as a
 left outer join bigfoot_external_neo.scp_warehouse__fc_product_detail_hive_dim as b
 on a.inventory_item_wid=b.product_detail_wid and a.warehouse_company=b.product_detail_warehouse_company
left outer join (
select 
f.fc_pnl_rate_month
 as fc_pnl_rate_month
,
max(f.fc_pnl_rate_rc_long_storage) as fc_pnl_rate_rc_long_storage
from bigfoot_common.fc_pnl_rate_card as f
group by f.fc_pnl_rate_month
) as c on 
substring((a.date_key),1,6) =c.fc_pnl_rate_month
where a.inventory_item_age_since_received > 75
and a.inventory_item_storage_location_type 
in 
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
group by 
a.inventory_item_warehouse_id
,
b.fc_product_detail_hive_dim_key,
a.date_key
,a.inventory_item_age_since_received

