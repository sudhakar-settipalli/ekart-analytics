INSERT OVERWRITE TABLE tracking_shipment_intermediate_hive_fact
select
tr.vendor_tracking_id,
national_hop_breach_score,
number_of_hops,
number_of_air_hops,
line_haul_breach_score,
number_of_ftl_hops,
tc_connection_breach_score,
number_of_offloads,
air_hop_breach_score,
actual_route_map,
first_dh_outscan_datetime,
last_mh_tc_outscan_datetime,
last_dh_outscan_datetime,
sort_resort_flag,
misroute_score,
missort_score,
resort_count
from
(
select 
vendor_tracking_id,
sum(If(consignment_movement_flag = 'INTERZONE',1,0)*consignment_breach_flag) as national_hop_breach_score,
count(consignment_id) as number_of_hops,
sum(If(lower(consignment_mode) = 'air',1,0)) as number_of_air_hops,
sum(consignment_breach_flag) as line_haul_breach_score,
sum(ftl_flag) as number_of_ftl_hops,
null as tc_connection_breach_score,
null as number_of_offloads,
sum(If(lower(consignment_mode) = 'air',1,0)*consignment_breach_flag) as air_hop_breach_score,
null as actual_route_map,
min(If(consignment_source_hub_type = 'DELIVERY_HUB',consignment_create_datetime,null)) as first_dh_outscan_datetime,
max(If(consignment_source_hub_type = 'MOTHER_HUB',consignment_create_datetime,null)) as last_mh_tc_outscan_datetime,
max(If(consignment_source_hub_type = 'DELIVERY_HUB',consignment_create_datetime,null)) as last_dh_outscan_datetime
from 
bigfoot_external_neo.scp_ekl__tracking_shipment_l0_hive_fact
group by vendor_tracking_id
) as tr
LEFT JOIN
(
	select vendor_tracking_id,
  	case when ((max(rank)=1 and max(preprimary_fac)=0) or (max(rank) =2 and max(preprimary_fac)<>0)) then 'sort' 
  	when max(preprimary_fac)<>0 then 'Preprimary'
    when max(rank) >1 then 'Resort' else 'Sort' end as sort_resort_flag,
	max(rank)-(case when  max(preprimary_fac)<> 0 then 2 else 1 end ) as resort_count
    from bigfoot_external_neo.scp_ekl__bag_l1_total_fact group by vendor_tracking_id
) as sorts
on sorts.vendor_tracking_id=tr.vendor_tracking_id
LEFT JOIN
(
	select sum( case when sequence=1 and error_flag='Misroute' then 1 else 0 end) as misroute_score,
	sum( case when sequence=1 and error_flag='Missort' then 1 else 0 end) as missort_score,
	vendor_tracking_id 
	from bigfoot_external_neo.scp_ekl__misroute_missort_hive_fact group by vendor_tracking_id
) as missorts
on
missorts.vendor_tracking_id=tr.vendor_tracking_id;
