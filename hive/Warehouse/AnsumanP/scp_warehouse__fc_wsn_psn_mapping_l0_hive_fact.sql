INSERT OVERWRITE TABLE fc_wsn_psn_mapping_l0_hive_fact
Select
final.warehouse_company,
final.wh_serial_number_id,
final.attribute_name,
final.attribute_value,
final.updated_at,
final.created_at,
final.is_valid,
final.wsn_display_id
from 
(
Select
'fki' as warehouse_company,
`data`.wh_serial_number_product_serial_number_mapping_wh_serial_number_id as wh_serial_number_id,
`data`.wh_serial_number_product_serial_number_mapping_attribute_name as attribute_name,
`data`.wh_serial_number_product_serial_number_mapping_attribute_value as attribute_value,
`data`.wh_serial_number_product_serial_number_mapping_updated_at as updated_at,
`data`.wh_serial_number_product_serial_number_mapping_created_at as created_at,
if(`data`.wh_serial_number_product_serial_number_mapping_is_valid = true,1,0) as is_valid,
`data`.wh_serial_number_display_id as wsn_display_id
from bigfoot_snapshot.dart_fki_scp_warehouse_wh_serial_number_product_serial_number_mapping_1_view_total

UNION ALL

Select
'wsr' as warehouse_company,
`data`.wh_serial_number_product_serial_number_mapping_wh_serial_number_id as wh_serial_number_id,
`data`.wh_serial_number_product_serial_number_mapping_attribute_name as attribute_name,
`data`.wh_serial_number_product_serial_number_mapping_attribute_value as attribute_value,
`data`.wh_serial_number_product_serial_number_mapping_updated_at as updated_at,
`data`.wh_serial_number_product_serial_number_mapping_created_at as created_at,
if(`data`.wh_serial_number_product_serial_number_mapping_is_valid = true,1,0) as is_valid,
`data`.wh_serial_number_display_id as wsn_display_id
from bigfoot_snapshot.dart_wsr_scp_warehouse_wh_serial_number_product_serial_number_mapping_1_view_total	
) final;
