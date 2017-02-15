INSERT OVERWRITE TABLE rto_ship_l1_hive_fact
Select A.vendor_tracking_id,
max(case when A.rvp_current_hub_type='DELIVERY_HUB' AND A.row_num_a=1
then A.rvp_current_hub_id end) 
as first_received_dh_id,
max(case when A.rvp_current_hub_type='DELIVERY_HUB' AND A.row_num_a=1 
then A.updated_time end) 
as first_received_dh_time,
max(case when A.rvp_current_hub_type='PICKUP_CENTER' AND A.row_num_a=1 
then A.rvp_current_hub_id end )
as first_received_pc_id,
max(case when A.rvp_current_hub_type='PICKUP_CENTER' AND A.row_num_a=1 
then A.updated_time end) 
as first_received_pc_time,
max(case when A.rvp_current_hub_type='MOTHER_HUB' AND A.row_num_a=1 
then A.rvp_current_hub_id end) 
as first_received_mh_id,
max(case when A.rvp_current_hub_type='MOTHER_HUB' AND A.row_num_a=1 
then A.updated_time end) 
as first_received_mh_time,
max(case when A.rvp_current_hub_type='MOTHER_HUB' AND A.row_num_a=2 
then A.rvp_current_hub_id end)
as second_received_mh_id,
max(case when A.rvp_current_hub_type='MOTHER_HUB' AND A.row_num_a=2 
then A.updated_time end) 
as second_received_mh_time,
max(case when A.rvp_current_hub_type='MOTHER_HUB' AND A.row_num_a=3 
then A.rvp_current_hub_id end)
as third_received_mh_id,
max(case when A.rvp_current_hub_type='MOTHER_HUB' AND A.row_num_a=3 
then A.updated_time end)
as third_received_mh_time,
max(case when A.rvp_current_hub_type='MOTHER_HUB' AND A.row_num_a=4 
then A.rvp_current_hub_id end)
as fourth_received_mh_id,
max(case when A.rvp_current_hub_type='MOTHER_HUB' AND A.row_num_a=4 
then A.updated_time end) 
as fourth_received_mh_time,
max(case when A.row_num_b=1 AND A.rvp_current_hub_type='DELIVERY_HUB' 
then A.rvp_current_hub_id end) 
as last_received_dh_id,
max(case when A.row_num_b=1 AND A.rvp_current_hub_type='DELIVERY_HUB' 
then A.updated_time end) 
as last_received_dh_time,
max(case when A.row_num_b=1 AND A.rvp_current_hub_type='MOTHER_HUB' 
then A.rvp_current_hub_id end) 
as last_received_mh_id,
max(case when A.row_num_b=1 AND A.rvp_current_hub_type='MOTHER_HUB' 
then A.updated_time end) 
as last_received_mh_time,
max(case when A.row_num_b=2 AND A.rvp_current_hub_type='MOTHER_HUB' 
then A.rvp_current_hub_id end) 
as second_last_received_mh_id,
max(case when A.row_num_b=2 AND A.rvp_current_hub_type='MOTHER_HUB' 
then A.updated_time end) 
as second_last_received_mh_time
from
(select D.vendor_tracking_id as vendor_tracking_id,
D.rvp_current_hub_id as rvp_current_hub_id,
D.rvp_current_hub_type as rvp_current_hub_type,
CAST(D.updatedat as timestamp) as updated_time, 
ROW_NUMBER() OVER (PARTITION BY D.vendor_tracking_id,D.rvp_current_hub_type ORDER BY D.updatedat ASC) as row_num_a,
ROW_NUMBER() OVER (PARTITION BY D.vendor_tracking_id,D.rvp_current_hub_type ORDER BY D.updatedat DESC) as row_num_b
from
(Select `data`.vendor_tracking_id as vendor_tracking_id,
`data`.current_address.id as rvp_current_hub_id,
`data`.current_address.type as rvp_current_hub_type,
max(updatedat) as updatedat
from bigfoot_journal.dart_wsr_scp_ekl_shipment_4 His
left join bigfoot_external_neo.scp_ekl__rto_cons_l0_hive_fact rto On His.`data`.vendor_tracking_id=rto.vendor_tracking_id
left join 
(select vendor_tracking_id,fsd_assigned_hub_id,shipment_first_received_dh_id,rto_create_datetime,fsd_first_dh_received_datetime
from bigfoot_external_neo.scp_ekl__shipment_l1_90_fact 
where ekl_shipment_type IN ('approved_rto','unapproved_rto')
AND fsd_assigned_hub_id=shipment_first_received_dh_id and rto_create_datetime>=fsd_first_dh_received_datetime) sl1 on His.`data`.vendor_tracking_id=sl1.vendor_tracking_id
where His.`data`.status IN ('Received','Error')
and CAST(His.updatedat as timestamp)>=rto.first_rto_cons_create_time
and sl1.vendor_tracking_id is null 
and His.`data`.shipment_type IN ('approved_rto','unapproved_rto') 
and His.day >date_format(date_sub(current_date,100),'yyyyMMdd')
--#120#DAY#
group by `data`.vendor_tracking_id,`data`.current_address.id,`data`.current_address.type
union all

Select `data`.vendor_tracking_id as vendor_tracking_id,
`data`.current_address.id as rvp_current_hub_id,
`data`.current_address.type as rvp_current_hub_type,
max(updatedat) as updatedat
from bigfoot_journal.dart_wsr_scp_ekl_shipment_4 His
left join 
(select vendor_tracking_id,fsd_assigned_hub_id,shipment_first_received_dh_id,rto_create_datetime,fsd_first_dh_received_datetime
from bigfoot_external_neo.scp_ekl__shipment_l1_90_fact 
where ekl_shipment_type IN ('approved_rto','unapproved_rto')
AND fsd_assigned_hub_id=shipment_first_received_dh_id and rto_create_datetime>=fsd_first_dh_received_datetime) sl1 on His.`data`.vendor_tracking_id=sl1.vendor_tracking_id
where His.`data`.status IN ('Received','Error')
and sl1.vendor_tracking_id is not null 
and His.`data`.shipment_type IN ('approved_rto','unapproved_rto')
and His.day >date_format(date_sub(current_date,100),'yyyyMMdd')
--#120#DAY#
group by `data`.vendor_tracking_id,`data`.current_address.id,`data`.current_address.type
)
D
)
A
group by A.vendor_tracking_id
;
