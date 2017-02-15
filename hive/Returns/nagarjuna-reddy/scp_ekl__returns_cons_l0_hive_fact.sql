INSERT OVERWRITE TABLE returns_cons_l0_hive_fact

Select 
B.vendor_tracking_id_b as vendor_tracking_id,
max(case when B.consignment_source_hub_type_b='DELIVERY_HUB' AND B.row_num_b=1 
then B.consignment_source_hub_id_b end)
as first_cons_create_dh_id,
max(case when B.consignment_source_hub_type_b='DELIVERY_HUB' AND B.row_num_b=1 
then B.consignment_destination_hub_id_b end)
as first_cons_create_dh_dest_id,
max(case when B.consignment_source_hub_type_b='DELIVERY_HUB' AND B.row_num_b=1 
then B.consignment_create_datetime_b end)
as first_cons_create_dh_time,
max(case when B.consignment_source_hub_type_b='DELIVERY_HUB' AND B.row_num_b=1 
then B.consignment_received_datetime_b end)
as first_cons_create_dh_receive_time,
max(case when B.consignment_source_hub_type_b ='MOTHER_HUB' AND B.row_num_b=1
then B.consignment_source_hub_id_b end)
as first_cons_create_mh_id,
max(case when B.consignment_source_hub_type_b='MOTHER_HUB' AND B.row_num_b=1 
then B.consignment_destination_hub_id_b end)
as first_cons_create_mh_dest_id,
max(case when B.consignment_source_hub_type_b ='MOTHER_HUB' AND B.row_num_b=1
then B.consignment_create_datetime_b end)
as first_cons_create_mh_time,
max(case when B.consignment_source_hub_type_b='MOTHER_HUB' AND B.row_num_b=1 
then B.consignment_received_datetime_b end)
as first_cons_create_mh_receive_time,
max(case when B.consignment_source_hub_type_b ='MOTHER_HUB' AND B.row_num_b=2
then B.consignment_source_hub_id_b end)
as second_cons_create_mh_id,
max(case when B.consignment_source_hub_type_b='MOTHER_HUB' AND B.row_num_b=2
then B.consignment_destination_hub_id_b end)
as second_cons_create_mh_dest_id,
max(case when B.consignment_source_hub_type_b ='MOTHER_HUB' AND B.row_num_b=2
then B.consignment_create_datetime_b end)
as second_cons_create_mh_time,
max(case when B.consignment_source_hub_type_b='MOTHER_HUB' AND B.row_num_b=2
then B.consignment_received_datetime_b end)
as second_cons_create_mh_receive_time,
max(case when B.consignment_source_hub_type_b ='MOTHER_HUB' AND B.row_num_b=3
then B.consignment_source_hub_id_b end)
as third_cons_create_mh_id,
max(case when B.consignment_source_hub_type_b='MOTHER_HUB' AND B.row_num_b=3
then B.consignment_destination_hub_id_b end)
as third_cons_create_mh_dest_id,
max(case when B.consignment_source_hub_type_b ='MOTHER_HUB' AND B.row_num_b=3
then B.consignment_create_datetime_b end)
as third_cons_create_mh_time,
max(case when B.consignment_source_hub_type_b='MOTHER_HUB' AND B.row_num_b=3
then B.consignment_received_datetime_b end)
as third_cons_create_mh_receive_time,
max(case when D.consignment_source_hub_type_d='MOTHER_HUB' AND D.row_num_d=1
then D.consignment_source_hub_id_d end)
as last_cons_create_mh_id,
max(case when D.consignment_source_hub_type_d='MOTHER_HUB' AND D.row_num_d=1
then D.consignment_destination_hub_id_d end)
as last_cons_create_mh_dest_id,
max(case when D.consignment_source_hub_type_d='MOTHER_HUB' AND D.row_num_d=1
then D.consignment_create_datetime_d end)
as last_cons_create_mh_time,
max(case when D.consignment_source_hub_type_d='MOTHER_HUB' AND D.row_num_d=1
then D.consignment_received_datetime_d end)
as last_cons_create_mh_receive_time,
max(case when D.consignment_source_hub_type_d='NOTHER_HUB' AND D.row_num_d=2
then D.consignment_source_hub_id_d end)
as sec_last_cons_create_mh_id,
max(case when D.consignment_source_hub_type_d='MOTHER_HUB' AND D.row_num_d=2
then D.consignment_destination_hub_id_d end)
as sec_last_cons_create_mh_dest_id,
max(case when D.consignment_source_hub_type_d='MOTHER_HUB' AND D.row_num_d=2
then D.consignment_create_datetime_d end)
as sec_last_cons_create_mh_time,
max(case when D.consignment_source_hub_type_d='MOTHER_HUB' AND D.row_num_d=2
then D.consignment_received_datetime_d end)
as sec_last_cons_create_mh_receive_time,
max(case when D.consignment_destination_hub_type_d='DELIVERY_HUB' AND D.row_num_d=1
then D.consignment_source_hub_id_d end)
as last_cons_dh_source_hub_id,
max(case when D.consignment_destination_hub_type_d='DELIVERY_HUB' AND D.row_num_d=1
then D.consignment_destination_hub_id_d end)
as last_cons_dest_dh_id,
max(case when D.consignment_destination_hub_type_d='DELIVERY_HUB' AND D.row_num_d=1
then D.consignment_create_datetime_d end)
as last_cons_dh_source_create_time,
max(case when D.consignment_destination_hub_type_d='DELIVERY_HUB' AND D.row_num_d=1
then D.consignment_received_datetime_d end)
as last_cons_dh_receive_time
from
(Select A.vendor_tracking_id as vendor_tracking_id_b,
A.consignment_source_hub_id as consignment_source_hub_id_b,
A.consignment_destination_hub_id as consignment_destination_hub_id_b,
A.consignment_create_datetime as consignment_create_datetime_b,
A.consignment_source_hub_type as consignment_source_hub_type_b,
A.consignment_destination_hub_type as consignment_destination_hub_type_b,
A.consignment_received_datetime as consignment_received_datetime_b,
ROW_NUMBER() OVER (PARTITION BY A.vendor_tracking_id,A.consignment_source_hub_type ORDER BY A.consignment_create_datetime ASC) as row_num_b
from

(Select vendor_tracking_id,
consignment_id,
consignment_source_hub_id,
consignment_destination_hub_id,
consignment_create_datetime,
consignment_source_hub_type,
consignment_destination_hub_type,
consignment_received_datetime
from 
bigfoot_external_neo.scp_ekl__tracking_shipment_hive_fact
where ekl_shipment_type='rvp') A
) B


left join 

(Select C.vendor_tracking_id as vendor_tracking_id_d,
C.consignment_source_hub_id as consignment_source_hub_id_d,
C.consignment_destination_hub_id as consignment_destination_hub_id_d,
C.consignment_create_datetime as consignment_create_datetime_d,
C.consignment_source_hub_type as consignment_source_hub_type_d,
C.consignment_destination_hub_type as consignment_destination_hub_type_d,
C.consignment_received_datetime as consignment_received_datetime_d,
ROW_NUMBER() OVER (PARTITION BY C.vendor_tracking_id,C.consignment_source_hub_type ORDER BY C.consignment_create_datetime DESC) as row_num_d
from

(Select vendor_tracking_id,
consignment_id,
consignment_source_hub_id,
consignment_destination_hub_id,
consignment_create_datetime,
consignment_source_hub_type,
consignment_destination_hub_type,
consignment_received_datetime
from 
bigfoot_external_neo.scp_ekl__tracking_shipment_hive_fact
where ekl_shipment_type='rvp') C
) D

ON B.vendor_tracking_id_b=D.vendor_tracking_id_d
group by B.vendor_tracking_id_b;
