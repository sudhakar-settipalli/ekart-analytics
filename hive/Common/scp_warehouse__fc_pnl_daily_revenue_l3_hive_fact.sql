INSERT OVERWRITE TABLE fc_pnl_daily_revenue_l3_hive_fact 
select
final_table.fc_pnl_warehouse as fc_pnl_warehouse,
final_table.fc_pnl_product_id_key as fc_pnl_product_id_key ,
final_table.fc_pnl_date_key as fc_pnl_date_key ,
final_table.fc_pnl_units as  fc_pnl_units ,
final_table.fc_pnl_revenue_type fc_pnl_revenue_type ,
final_table.fc_pnl_revenue as fc_pnl_revenue ,
final_table.fc_pnl_freebie_units as fc_pnl_freebie_units ,
final_table.fc_pnl_freebie_revenue as fc_pnl_freebie_revenue ,
final_table.fc_pnl_fragile_units as fc_pnl_fragile_units ,
final_table.fc_pnl_fragile_revenue as fc_pnl_fragile_revenue ,
final_table.fc_pnl_returns_mobile_units as fc_pnl_returns_mobile_units ,
final_table.fc_pnl_returns_mobile_revenue as fc_pnl_returns_mobile_revenue ,
final_table.fc_pnl_volume as fc_pnl_volume ,
final_table.fc_pnl_storage_space_utilized_in_cubic_feet as fc_pnl_storage_space_utilized_in_cubic_feet ,
final_table.fc_pnl_long_storage_age as fc_pnl_long_storage_age ,
final_table.fc_pnl_outbound_destination_warehouse as fc_pnl_outbound_destination_warehouse
from
(
select 
fc_pnl_outbound_warehouse as fc_pnl_warehouse ,
fc_pnl_outbound_product_id_key as fc_pnl_product_id_key ,
fc_pnl_outbound_date_key as fc_pnl_date_key ,
fc_pnl_outbound_processing_units as  fc_pnl_units ,
'outbound' as fc_pnl_revenue_type ,
fc_pnl_outbound_processing_revenue as fc_pnl_revenue ,
fc_pnl_outbound_freebie_units as fc_pnl_freebie_units ,
fc_pnl_outbound_freebie_revenue as fc_pnl_freebie_revenue ,
fc_pnl_outbound_fragile_units as fc_pnl_fragile_units ,
fc_pnl_outbound_fragile_revenue as fc_pnl_fragile_revenue ,
null as fc_pnl_returns_mobile_units ,
null as fc_pnl_returns_mobile_revenue ,
fc_pnl_outbound_single_shipment_volume as fc_pnl_volume ,
null as fc_pnl_storage_space_utilized_in_cubic_feet ,
null as fc_pnl_long_storage_age ,
null as fc_pnl_outbound_destination_warehouse
from
bigfoot_external_neo.scp_warehouse__fc_pnl_outbound_revenue_l2_hive_fact
where fc_pnl_outbound_processing_revenue_type='outbound'

union all

select 
fc_pnl_outbound_warehouse as fc_pnl_warehouse ,
fc_pnl_outbound_product_id_key as fc_pnl_product_id_key ,
fc_pnl_outbound_date_key as fc_pnl_date_key ,
fc_pnl_outbound_processing_units as  fc_pnl_units ,
'iwit outbound' as fc_pnl_revenue_type ,
fc_pnl_outbound_processing_revenue as fc_pnl_revenue ,
null as fc_pnl_freebie_units ,
null as fc_pnl_freebie_revenue ,
null as fc_pnl_fragile_units ,
null as fc_pnl_fragile_revenue ,
null as fc_pnl_returns_mobile_units ,
null as fc_pnl_returns_mobile_revenue ,
null as fc_pnl_volume ,
null as fc_pnl_storage_space_utilized_in_cubic_feet ,
null as fc_pnl_long_storage_age ,
fc_pnl_outbound_destination_warehouse as fc_pnl_outbound_destination_warehouse
from
bigfoot_external_neo.scp_warehouse__fc_pnl_outbound_revenue_l2_hive_fact
where fc_pnl_outbound_processing_revenue_type='iwit outbound'

union all

select
fc_pnl_returns_warehouse as fc_pnl_warehouse ,
fc_pnl_returns_product_id_key as fc_pnl_product_id_key ,
fc_pnl_returns_date_key as fc_pnl_date_key ,
fc_pnl_returns_processing_units as  fc_pnl_units ,
'returns' as fc_pnl_revenue_type ,
fc_pnl_returns_processing_revenue as fc_pnl_revenue ,
null as fc_pnl_freebie_units ,
null as fc_pnl_freebie_revenue ,
null as fc_pnl_fragile_units ,
null as fc_pnl_fragile_revenue ,
fc_pnl_returns_mobile_units as fc_pnl_returns_mobile_units ,
fc_pnl_returns_mobile_revenue as fc_pnl_returns_mobile_revenue ,
fc_pnl_returns_single_shipment_volume as fc_pnl_volume ,
null as fc_pnl_storage_space_utilized_in_cubic_feet ,
null as fc_pnl_long_storage_age ,
null as fc_pnl_outbound_destination_warehouse
from
bigfoot_external_neo.scp_warehouse__fc_pnl_returns_revenue_l2_hive_fact

union all

select
fc_pnl_inbound_warehouse as fc_pnl_warehouse ,
fc_pnl_inbound_product_id_key as fc_pnl_product_id_key ,
fc_pnl_inbound_date_key as fc_pnl_date_key ,
fc_pnl_inbound_processing_units as  fc_pnl_units ,
'inbound' as fc_pnl_revenue_type ,
fc_pnl_inbound_processing_revenue as fc_pnl_revenue ,
null as fc_pnl_freebie_units ,
null as fc_pnl_freebie_revenue ,
null as fc_pnl_fragile_units ,
null as fc_pnl_fragile_revenue ,
null as fc_pnl_returns_mobile_units ,
null as fc_pnl_returns_mobile_revenue ,
fc_pnl_inbound_product_volume as fc_pnl_volume ,
null as fc_pnl_storage_space_utilized_in_cubic_feet ,
null as fc_pnl_long_storage_age ,
null as fc_pnl_outbound_destination_warehouse
from
bigfoot_external_neo.scp_warehouse__fc_pnl_inbound_revenue_l2_hive_fact
where fc_pnl_inbound_processing_revenue_type='inbound'

union all

select
fc_pnl_inbound_warehouse as fc_pnl_warehouse ,
fc_pnl_inbound_product_id_key as fc_pnl_product_id_key ,
fc_pnl_inbound_date_key as fc_pnl_date_key ,
fc_pnl_inbound_processing_units as  fc_pnl_units ,
'iwit inbound' as fc_pnl_revenue_type ,
fc_pnl_inbound_processing_revenue as fc_pnl_revenue ,
null as fc_pnl_freebie_units ,
null as fc_pnl_freebie_revenue ,
null as fc_pnl_fragile_units ,
null as fc_pnl_fragile_revenue ,
null as fc_pnl_returns_mobile_units ,
null as fc_pnl_returns_mobile_revenue ,
null as fc_pnl_volume ,
null as fc_pnl_storage_space_utilized_in_cubic_feet ,
null as fc_pnl_long_storage_age ,
null as fc_pnl_outbound_destination_warehouse
from
bigfoot_external_neo.scp_warehouse__fc_pnl_inbound_revenue_l2_hive_fact
where fc_pnl_inbound_processing_revenue_type='iwit inbound'

union all

select
fc_pnl_fc_storage_warehouse as fc_pnl_warehouse ,
fc_pnl_fc_storage_product_id_key as fc_pnl_product_id_key ,
fc_pnl_fc_storage_date_key as fc_pnl_date_key ,
fc_pnl_fc_storage_units as  fc_pnl_units ,
'fc storage' as fc_pnl_revenue_type ,
fc_pnl_fc_storage_actual_space_used_revenue as fc_pnl_revenue ,
null as fc_pnl_freebie_units ,
null as fc_pnl_freebie_revenue ,
null as fc_pnl_fragile_units ,
null as fc_pnl_fragile_revenue ,
null as fc_pnl_returns_mobile_units ,
null as fc_pnl_returns_mobile_revenue ,
null as fc_pnl_volume ,
fc_pnl_fc_storage_space_utilized_in_cubic_feet as fc_pnl_storage_space_utilized_in_cubic_feet ,
null as fc_pnl_long_storage_age ,
null as fc_pnl_outbound_destination_warehouse
from
bigfoot_external_neo.scp_warehouse__fc_pnl_fc_storage_revenue_l2_hive_fact

union all

select
fc_pnl_rc_storage_warehouse as fc_pnl_warehouse ,
fc_pnl_rc_storage_product_id_key as fc_pnl_product_id_key ,
fc_pnl_rc_storage_date_key as fc_pnl_date_key ,
fc_pnl_rc_storage_units as  fc_pnl_units ,
'rc storage' as fc_pnl_revenue_type ,
fc_pnl_rc_storage_actual_space_used_revenue as fc_pnl_revenue ,
null as fc_pnl_freebie_units ,
null as fc_pnl_freebie_revenue ,
null as fc_pnl_fragile_units ,
null as fc_pnl_fragile_revenue ,
null as fc_pnl_returns_mobile_units ,
null as fc_pnl_returns_mobile_revenue ,
null as fc_pnl_volume ,
fc_pnl_rc_storage_space_utilized_in_cubic_feet as fc_pnl_storage_space_utilized_in_cubic_feet ,
null as fc_pnl_long_storage_age ,
null as fc_pnl_outbound_destination_warehouse
from
bigfoot_external_neo.scp_warehouse__fc_pnl_rc_storage_revenue_l2_hive_fact


union all 

select
a.fc_pnl_fc_long_storage_warehouse as fc_pnl_warehouse ,
a.fc_pnl_fc_long_storage_product_id_key as fc_pnl_product_id_key ,
a.fc_pnl_fc_long_storage_date_key as fc_pnl_date_key ,
a.fc_pnl_fc_long_storage_units as  fc_pnl_units ,
'fc long storage' as fc_pnl_revenue_type ,
a.fc_pnl_fc_long_storage_daily_revenue as fc_pnl_revenue ,
null as fc_pnl_freebie_units ,
null as fc_pnl_freebie_revenue ,
null as fc_pnl_fragile_units ,
null as fc_pnl_fragile_revenue ,
null as fc_pnl_returns_mobile_units ,
null as fc_pnl_returns_mobile_revenue ,
null as fc_pnl_volume ,
a.fc_pnl_fc_long_storage_space_utilized_in_cubic_feet as fc_pnl_storage_space_utilized_in_cubic_feet ,
a.fc_pnl_fc_long_storage_age as fc_pnl_long_storage_age ,
null as fc_pnl_outbound_destination_warehouse

from 
(
select
fc_pnl_fc_long_storage_warehouse as fc_pnl_fc_long_storage_warehouse ,
fc_pnl_fc_long_storage_product_id_key as fc_pnl_fc_long_storage_product_id_key ,
fc_pnl_fc_long_storage_date_key as fc_pnl_fc_long_storage_date_key ,
fc_pnl_fc_long_storage_age as fc_pnl_fc_long_storage_age,
sum(fc_pnl_fc_long_storage_daily_revenue) as fc_pnl_fc_long_storage_daily_revenue ,
sum(fc_pnl_fc_long_storage_units) as fc_pnl_fc_long_storage_units,
sum(fc_pnl_fc_long_storage_space_utilized_in_cubic_feet) as fc_pnl_fc_long_storage_space_utilized_in_cubic_feet
from
bigfoot_external_neo.scp_warehouse__fc_pnl_fc_long_storage_revenue_l2_hive_fact
group by
fc_pnl_fc_long_storage_warehouse,
fc_pnl_fc_long_storage_product_id_key,
fc_pnl_fc_long_storage_date_key,
fc_pnl_fc_long_storage_age
) a


union all

select 
a.fc_pnl_rc_long_storage_warehouse as fc_pnl_warehouse ,
a.fc_pnl_rc_long_storage_product_id_key as fc_pnl_product_id_key ,
a.fc_pnl_rc_long_storage_date_key as fc_pnl_date_key ,
a.fc_pnl_rc_long_storage_units as  fc_pnl_units ,
'rc long storage' as fc_pnl_revenue_type ,
a.fc_pnl_rc_long_storage_daily_revenue as fc_pnl_revenue ,
null as fc_pnl_freebie_units ,
null as fc_pnl_freebie_revenue ,
null as fc_pnl_fragile_units ,
null as fc_pnl_fragile_revenue ,
null as fc_pnl_returns_mobile_units ,
null as fc_pnl_returns_mobile_revenue ,
null as fc_pnl_volume  ,
null as fc_pnl_storage_space_utilized_in_cubic_feet ,
a.fc_pnl_rc_long_storage_age as fc_pnl_long_storage_age ,
null as fc_pnl_outbound_destination_warehouse
from
(
select
 fc_pnl_rc_long_storage_warehouse,
 fc_pnl_rc_long_storage_product_id_key,
 fc_pnl_rc_long_storage_date_key,
 fc_pnl_rc_long_storage_age,
 sum(fc_pnl_rc_long_storage_daily_revenue) as fc_pnl_rc_long_storage_daily_revenue,
 sum(fc_pnl_rc_long_storage_units) as fc_pnl_rc_long_storage_units
from
bigfoot_external_neo.scp_warehouse__fc_pnl_rc_long_storage_revenue_l2_hive_fact
group by
fc_pnl_rc_long_storage_warehouse,
fc_pnl_rc_long_storage_product_id_key,
fc_pnl_rc_long_storage_date_key,
fc_pnl_rc_long_storage_age
) a

) final_table
;
