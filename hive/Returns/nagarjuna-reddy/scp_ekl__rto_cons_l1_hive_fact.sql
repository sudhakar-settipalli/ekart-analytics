INSERT OVERWRITE TABLE rto_cons_l1_hive_fact
Select
B.vendor_tracking_id as vendor_tracking_id,
max(case when B.consignment_source_hub_type='DELIVERY_HUB' AND B.row_num_a=1 
then B.consignment_source_hub_id end)
as first_cons_create_dh_id,
max(case when B.consignment_source_hub_type='DELIVERY_HUB' AND B.row_num_a=1 
then B.consignment_destination_hub_id end)
as first_cons_create_dh_dest_id,
max(case when B.consignment_source_hub_type='DELIVERY_HUB' AND B.row_num_a=1 
then B.consignment_create_datetime end)
as first_cons_create_dh_time,
max(case when B.consignment_source_hub_type='DELIVERY_HUB' AND B.row_num_a=1 
then B.consignment_received_datetime end)
as first_cons_create_dh_receive_time,
max(case when B.consignment_source_hub_type ='MOTHER_HUB' AND B.row_num_a=1
then B.consignment_source_hub_id end)
as first_cons_create_mh_id,
max(case when B.consignment_source_hub_type='MOTHER_HUB' AND B.row_num_a=1 
then B.consignment_destination_hub_id end)
as first_cons_create_mh_dest_id,
max(case when B.consignment_source_hub_type ='MOTHER_HUB' AND B.row_num_a=1
then B.consignment_create_datetime end)
as first_cons_create_mh_time,
max(case when B.consignment_source_hub_type='MOTHER_HUB' AND B.row_num_a=1 
then B.consignment_received_datetime end)
as first_cons_create_mh_receive_time,
max(case when B.consignment_source_hub_type ='MOTHER_HUB' AND B.row_num_a=2
then B.consignment_source_hub_id end)
as second_cons_create_mh_id,
max(case when B.consignment_source_hub_type='MOTHER_HUB' AND B.row_num_a=2
then B.consignment_destination_hub_id end)
as second_cons_create_mh_dest_id,
max(case when B.consignment_source_hub_type ='MOTHER_HUB' AND B.row_num_a=2
then B.consignment_create_datetime end)
as second_cons_create_mh_time,
max(case when B.consignment_source_hub_type='MOTHER_HUB' AND B.row_num_a=2
then B.consignment_received_datetime end)
as second_cons_create_mh_receive_time,
max(case when B.consignment_source_hub_type ='MOTHER_HUB' AND B.row_num_a=3
then B.consignment_source_hub_id end)
as third_cons_create_mh_id,
max(case when B.consignment_source_hub_type='MOTHER_HUB' AND B.row_num_a=3
then B.consignment_destination_hub_id end)
as third_cons_create_mh_dest_id,
max(case when B.consignment_source_hub_type ='MOTHER_HUB' AND B.row_num_a=3
then B.consignment_create_datetime end)
as third_cons_create_mh_time,
max(case when B.consignment_source_hub_type='MOTHER_HUB' AND B.row_num_a=3
then B.consignment_received_datetime end)
as third_cons_create_mh_receive_time,
max(case when B.consignment_source_hub_type='MOTHER_HUB' AND B.row_num_b=1
then B.consignment_source_hub_id end)
as last_cons_create_mh_id,
max(case when B.consignment_source_hub_type='MOTHER_HUB' AND B.row_num_b=1
then B.consignment_destination_hub_id end)
as last_cons_create_mh_dest_id,
max(case when B.consignment_source_hub_type='MOTHER_HUB' AND B.row_num_b=1
then B.consignment_create_datetime end)
as last_cons_create_mh_time,
max(case when B.consignment_source_hub_type='MOTHER_HUB' AND B.row_num_b=1
then B.consignment_received_datetime end)
as last_cons_create_mh_receive_time,
max(case when B.consignment_source_hub_type='NOTHER_HUB' AND B.row_num_b=2
then B.consignment_source_hub_id end)
as sec_last_cons_create_mh_id,
max(case when B.consignment_source_hub_type='MOTHER_HUB' AND B.row_num_b=2
then B.consignment_destination_hub_id end)
as sec_last_cons_create_mh_dest_id,
max(case when B.consignment_source_hub_type='MOTHER_HUB' AND B.row_num_b=2
then B.consignment_create_datetime end)
as sec_last_cons_create_mh_time,
max(case when B.consignment_source_hub_type='MOTHER_HUB' AND B.row_num_b=2
then B.consignment_received_datetime end)
as sec_last_cons_create_mh_receive_time,
max(case when B.consignment_destination_hub_type='DELIVERY_HUB' AND B.row_num_b=1
then B.consignment_source_hub_id end)
as last_cons_dh_source_hub_id,
max(case when B.consignment_destination_hub_type='DELIVERY_HUB' AND B.row_num_b=1
then B.consignment_destination_hub_id end)
as last_cons_dest_dh_id,
max(case when B.consignment_destination_hub_type='DELIVERY_HUB' AND B.row_num_b=1
then B.consignment_create_datetime end)
as last_cons_dh_source_create_time,
max(case when B.consignment_destination_hub_type='DELIVERY_HUB' AND B.row_num_b=1
then B.consignment_received_datetime end)
as last_cons_dh_receive_time

FROM 


(Select hops.vendor_tracking_id as vendor_tracking_id,
hops.consignment_id as consignment_id,
hops.consignment_source_hub_id as consignment_source_hub_id,
hops.consignment_destination_hub_id as consignment_destination_hub_id,
hops.consignment_create_datetime as consignment_create_datetime,
hops.consignment_source_hub_type as consignment_source_hub_type,
hops.consignment_destination_hub_type as consignment_destination_hub_type,
hops.consignment_received_datetime as consignment_received_datetime,
hops.bag_source_hub_id as bag_source_hub_id,
hops.bag_assigned_hub_id as bag_destination_hub_id,
ROW_NUMBER() OVER (PARTITION BY hops.vendor_tracking_id,hops.consignment_source_hub_type ORDER BY hops.consignment_create_datetime) as row_num_a,
ROW_NUMBER() OVER (PARTITION BY hops.vendor_tracking_id,hops.consignment_source_hub_type ORDER BY hops.consignment_create_datetime DESC) as row_num_b
--RANK() OVER (PARTITION BY hops.vendor_tracking_id ORDER BY hops.consignment_create_datetime) as ranks
from 
bigfoot_external_neo.scp_ekl__tracking_shipment_hive_fact hops
left join
bigfoot_external_neo.scp_ekl__rto_cons_l0_hive_fact rto
ON hops.vendor_tracking_id=rto.vendor_tracking_id
where hops.consignment_create_datetime>=rto.first_rto_cons_create_time
and hops.ekl_shipment_type IN ('approved_rto','unapproved_rto')) B
group by B.vendor_tracking_id;
