INSERT OVERWRITE TABLE fc_product_serial_number_map_l0_hive_fact
select 
ism2.product_serial_number_id,
ism2.inventory_item_id,
ism2.number1,
ism2.number2,
ism2.number3,
ism2.product_serial_number_created_at,
ism2.product_serial_number_created_date_key,
ism2.product_serial_number_created_time_key,
ism2.product_serial_number_updated_at,
ism2.product_serial_number_updated_date_key,
ism2.product_serial_number_updated_time_key,
ism2.is_valid
from 
(
select
ism.data.id as product_serial_number_id,
ism.data.inventory_item_id as inventory_item_id,
ism.data.number1 as number1,
ism.data.number2 as number2,
ism.data.number3 as number3,
ism.data.created_at as product_serial_number_created_at,
lookup_date(ism.data.created_at) as product_serial_number_created_date_key,
lookup_time(ism.data.created_at) as product_serial_number_created_time_key,
ism.data.updated_at as product_serial_number_updated_at,
lookup_date(ism.data.updated_at) as product_serial_number_updated_date_key,
lookup_time(ism.data.updated_at) as product_serial_number_updated_time_key,
IF(ism.`data`.is_valid=TRUE,1,0) as is_valid
from bigfoot_snapshot.dart_wsr_scp_warehouse_inventory_serial_no_mapping_3_view_total as ism
union all
select
ism1.data.id as product_serial_number_id,
ism1.data.inventory_item_id as inventory_item_id,
ism1.data.number1 as number1,
ism1.data.number2 as number2,
ism1.data.number3 as number3,
ism1.data.created_at as product_serial_number_created_at,
lookup_date(ism1.data.created_at) as product_serial_number_created_date_key,
lookup_time(ism1.data.created_at) as product_serial_number_created_time_key,
ism1.data.updated_at as product_serial_number_updated_at,
lookup_date(ism1.data.updated_at) as product_serial_number_updated_date_key,
lookup_time(ism1.data.updated_at) as product_serial_number_updated_time_key,
IF(ism1.`data`.is_valid=TRUE,1,0) as is_valid
from bigfoot_snapshot.dart_fki_scp_warehouse_inventory_serial_no_mapping_2_view_total as ism1
) ism2;
