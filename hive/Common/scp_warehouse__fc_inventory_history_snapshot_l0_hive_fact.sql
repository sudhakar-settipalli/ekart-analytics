INSERT OVERWRITE TABLE #INCR_TBL# PARTITION(date_key)
SELECT ii.warehouse_company,
ii.inventory_item_warehouse_id,
ii.inventory_item_wid,
ii.inventory_item_storage_location_type,
sum(ii.inventory_item_quantity) as quantity,
ii.inventory_item_age_since_received,
lookup_date(from_unixtime(unix_timestamp())) as date_key
from bigfoot_external_neo.scp_warehouse__fc_inventory_item_l0_hive_fact ii
where ii.inventory_item_quantity>0 and (ii.inventory_item_storage_location_type not in ('outbound_shipment_table','outbound_consignment_table')
or ii.inventory_item_storage_location_type is null)
group by ii.warehouse_company,
ii.inventory_item_warehouse_id,
ii.inventory_item_wid,
ii.inventory_item_storage_location_type,
ii.inventory_item_age_since_received,
lookup_date(from_unixtime(unix_timestamp()))
UNION ALL
SELECT '' as warehouse_company,
'' as inventory_item_warehouse_id,
'' as inventory_item_wid,
'' as inventory_item_storage_location_type,
'' as quantity,
'' as inventory_item_age_since_received,
'' as date_key
from bigfoot_journal.dart_fki_scp_warehouse_inventory_item_3_2#INCR_READ# 
where day = 90000101
;

