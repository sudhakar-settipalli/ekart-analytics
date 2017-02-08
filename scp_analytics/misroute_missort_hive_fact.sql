INSERT OVERWRITE TABLE misroute_missort_hive_fact
select 
vendor_tracking_id,
bag_id,
shipment_type,
cid,
bag_source_hub_id,
bag_assigned_hub_id,
consignment_id,
consignment_source_hub_id,
consignment_destination_hub_id,
shipment_origin_mh_facility_id,
fsd_assigned_hub_id,
error_time,
bag_first_closed_datetime,
bag_receive_datetime,
flag,
ROW_NUMBER() OVER (PARTITION BY X.vendor_tracking_id,X.cid ORDER BY X.flag ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as rank,
error_flag,
ROW_NUMBER() OVER (PARTITION BY X.vendor_tracking_id,X.cid ORDER BY X.flag1 ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as rank1
from
(select 
B.shipment_id as vendor_tracking_id,
cast(split(B.bag,"-")[1] as INT) as bag_id,
A.shipment_type,
A.cid,
D.bag_source_hub_id,
D.bag_assigned_hub_id,
E.consignment_id,
E.consignment_source_hub_id,
E.consignment_destination_hub_id,
F.shipment_origin_mh_facility_id,
F.fsd_assigned_hub_id,
A.error_time,
D.bag_first_closed_datetime,
D.bag_receive_datetime,
case when (unix_timestamp(A.error_time) - unix_timestamp(D.bag_first_closed_datetime))/3600 < 0 then 100000 else (unix_timestamp(A.error_time) - unix_timestamp(D.bag_first_closed_datetime))/3600 end as flag,
case when (unix_timestamp(A.error_time) - unix_timestamp(E.consignment_create_datetime))/3600 < 0 then 100000 else (unix_timestamp(A.error_time) - unix_timestamp(E.consignment_create_datetime))/3600 end as flag1,
case when A.notes like 'Misrouted to%' then 'Missort' when A.notes like 'Misrouted :(With Bag%' then 'Misroute' else 'RCA' end as error_flag
from bigfoot_external_neo.scp_ekl__shipment_closedbag_map_l1_90_fact as B 
left join
(
select 
data.vendor_tracking_id, 
data.shipment_type,
data.current_address.id as cid,
cast(updatedat as TIMESTAMP) as error_time,
case when data.notes.type[0] = 'Hub Notes' then data.notes.flag[0] 
when data.notes.type[1] = 'Hub Notes' then data.notes.flag[1]
when data.notes.type[2] = 'Hub Notes' then data.notes.flag[2] else null end as notes
from bigfoot_journal.dart_wsr_scp_ekl_shipment_4 where day > date_format(date_sub(current_date,30),'yyyyMMdd') and `data`.status = 'Error'
) A on A.vendor_tracking_id = B.shipment_id
left join
bigfoot_external_neo.scp_ekl__bag_consignment_map_90_fact C on cast(split(B.bag,"-")[1] as INT) = C.bag_id
left join
bigfoot_external_neo.scp_ekl__bag_l1_90_fact D on D.bag_id = cast(split(B.bag,"-")[1] as INT) 
left join
bigfoot_external_neo.scp_ekl__consignment_l1_90_fact E on E.consignment_id = C.consignment_id
left join
bigfoot_external_neo.scp_ekl__shipment_l1_90_fact F on F.vendor_tracking_id = B.shipment_id) as X
