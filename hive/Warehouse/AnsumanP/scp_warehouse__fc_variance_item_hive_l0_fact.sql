INSERT OVERWRITE TABLE fc_variance_item_hive_l0_fact
select variance_company,variance_created_at,variance_created_by,variance_inventory_item_id,variance_reason_dim_key,variance_quantity,
variance_storage_location_dim_key,variance_updated_at,variance_id,variance_warehouse_id,variance_wid,variance_created_date_key,
variance_created_time_key,variance_updated_date_key,variance_updated_time_key,variance_product_dim_key,variance_warehouse_dim_key
from ( 
SELECT 'wsr' as variance_company,
from_unixtime(unix_timestamp(`data`.created_at)) as variance_created_at,
`data`.created_by as variance_created_by,
`data`.inventory_item_id as variance_inventory_item_id,
lookupkey('variance_reason_id',concat(`data`.inventory_variance_reason_id,'wsr')) as variance_reason_dim_key,
`data`.quantity as variance_quantity,
lookupkey('storage_location_storage_id',concat(`data`.storage_location_id,'wsr')) as variance_storage_location_dim_key,
from_unixtime(unix_timestamp(`data`.updated_at)) as variance_updated_at,
`data`.variance_id,
`data`.warehouse_id as variance_warehouse_id,
`data`.wid as variance_wid,
lookup_date(`data`.created_at) as variance_created_date_key,
lookup_time(`data`.created_at) as variance_created_time_key,
lookup_date(`data`.updated_at) as variance_updated_date_key,
lookup_time(`data`.updated_at) as variance_updated_time_key,
lookupkey('product_detail_product_id',concat(`data`.product_id,'wsr')) as variance_product_dim_key,
lookupkey('warehouse_id',`data`.warehouse_id) as variance_warehouse_dim_key
from bigfoot_snapshot.dart_wsr_scp_warehouse_inventory_variance_item_2_view_total
union all
SELECT 'fki' as variance_company,
from_unixtime(unix_timestamp(`data`.created_at)) as variance_created_at,
`data`.created_by as variance_created_by,
`data`.inventory_item_id as variance_inventory_item_id,
lookupkey('variance_reason_id',concat(`data`.inventory_variance_reason_id,'fki')) as variance_reason_dim_key,
`data`.quantity as variance_quantity,
lookupkey('storage_location_storage_id',concat(`data`.storage_location_id,'fki')) as variance_storage_location_dim_key,
from_unixtime(unix_timestamp(`data`.updated_at)) as variance_updated_at,
`data`.variance_id,
`data`.warehouse_id as variance_warehouse_id,
`data`.wid as variance_wid,
lookup_date(`data`.created_at) as variance_created_date_key,
lookup_time(`data`.created_at) as variance_created_time_key,
lookup_date(`data`.updated_at) as variance_updated_date_key,
lookup_time(`data`.updated_at) as variance_updated_time_key,
lookupkey('product_detail_product_id',concat(`data`.product_id,'fki')) as variance_product_dim_key,
lookupkey('warehouse_id',`data`.warehouse_id) as variance_warehouse_dim_key
from bigfoot_snapshot.dart_fki_scp_warehouse_inventory_variance_item_1_view_total) t1;
