Insert overwrite table mh_breach_large_fact
Select 
MD.vendor_tracking_id,
MD.ideal_connection_id,
MD.groupId,
MD.MH_ready_secs,
MD.dest_facility_id,
MD.cutoffTime,
MD.Time_diff,
MD.fsd_assigned_hub_id_key,
MD.shipment_origin_facility_id_key,
MD.actual_conn_id,
MD.actual_cons_id,
MD.first_conn,
MD.tpt_compliance_flag,
MD.TPT_intransit_breach_flag,
MD.shipment_tpt_dh_eta_datetime,
MD.shipment_tpt_dh_eta_date_key,
MD.fulfill_item_unit_dispatch_expected_time,
MD.fulfill_item_unit_dispatch_actual_time,
MD.dispatch_max_time,
MD.shipment_first_consignment_create_datetime,
MD.DH_recieve_datetime,
MD.DH_recieve_date_key,
MD.next_day_flag,
lookup_date(to_date(MD.fulfill_item_unit_dispatch_expected_time)) as dispatch_expected_date_key,
lookup_date(to_date(MD.fulfill_item_unit_dispatch_actual_time)) as dispatch_actual_date_key,
lookup_date(to_date(MD.dispatch_max_time)) as dispatch_max_date_key,
lookup_date(to_date(MD.shipment_first_consignment_create_datetime)) as consignment_create_date_key,
unix_timestamp(dispatch_max_time)+7200+Time_diff as MH_promise_timestamp,
from_unixtime(unix_timestamp(dispatch_max_time)+7200+Time_diff) as MH_promise_dateTime,
if((unix_timestamp(dispatch_max_time)+7200+Time_diff)<unix_timestamp(shipment_first_consignment_create_datetime),1,0) as MH_breach_flag,
lookup_date(to_date(from_unixtime(unix_timestamp(dispatch_max_time)+7200+Time_diff))) as MH_promise_date_key

from
(SELECT
M.vendor_tracking_id,
M.ideal_connection_id,
M.groupId,
M.MH_ready_secs,
M.dest_facility_id,
M.cutoffTime,
M.Time_diff,
M.fsd_assigned_hub_id_key,
M.shipment_origin_facility_id_key,
M.actual_conn_id,
M.actual_cons_id,
M.first_conn,
M.tpt_compliance_flag,
M.TPT_intransit_breach_flag,
M.shipment_tpt_dh_eta_datetime,
M.shipment_tpt_dh_eta_date_key,
M.fulfill_item_unit_dispatch_expected_time,
M.fulfill_item_unit_dispatch_actual_time,
M.dispatch_max_time,
M.shipment_first_consignment_create_datetime,
M.DH_recieve_datetime,
M.DH_recieve_date_key,
M.next_day_flag,
first_value(M.ideal_connection_id) Over (PARTITION By M.vendor_tracking_id order by M.Time_diff ASC rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as shipment_ideal_conn_id,
row_number() Over (PARTITION By M.vendor_tracking_id order by M.Time_diff ASC rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as r_no

From 
(Select 
Q.vendor_tracking_id as vendor_tracking_id,
Q.ideal_connection_id as ideal_connection_id,
Q.groupId as groupId,
Q.MH_ready_time as MH_ready_secs,
Q.dest_facility_id as dest_facility_id,
Q.cutoffTime as cutoffTime,
if(Q.cutoffTime-Q.MH_ready_time>=0,Q.cutoffTime-Q.MH_ready_time,Q.cutoffTime-Q.MH_ready_time+86400) as Time_diff,
Q.fsd_assigned_hub_id_key as fsd_assigned_hub_id_key,
Q.shipment_origin_facility_id_key as shipment_origin_facility_id_key,
Q.actual_conn_id as actual_conn_id,
Q.actual_cons_id as actual_cons_id,
Q.first_conn as first_conn,
Q.tpt_compliance_flag as tpt_compliance_flag,
Q.TPT_intransit_breach_flag as TPT_intransit_breach_flag,
Q.shipment_tpt_dh_eta_datetime as shipment_tpt_dh_eta_datetime,
Q.shipment_tpt_dh_eta_date_key as shipment_tpt_dh_eta_date_key,
Q.fulfill_item_unit_dispatch_expected_time as fulfill_item_unit_dispatch_expected_time,
Q.fulfill_item_unit_dispatch_actual_time as fulfill_item_unit_dispatch_actual_time,
if(Q.fulfill_item_unit_dispatch_expected_time>Q.fulfill_item_unit_dispatch_actual_time,fulfill_item_unit_dispatch_expected_time,fulfill_item_unit_dispatch_actual_time) as dispatch_max_time,
Q.shipment_first_consignment_create_datetime as shipment_first_consignment_create_datetime,
Q.DH_recieve_datetime as DH_recieve_datetime,
Q.DH_recieve_date_key as DH_recieve_date_key,
Q.next_day_flag as next_day_flag
From
(Select 
A.vendor_tracking_id as vendor_tracking_id,
C.ideal_connection_id as ideal_connection_id,
C.groupId as groupId,
C.cutoffTime as cutoffTime,
Origin.origin_facility_id as origin_facility_id,
Dest.dest_facility_id as dest_facility_id,
A.fsd_assigned_hub_id_key as fsd_assigned_hub_id_key,
A.shipment_origin_facility_id_key as shipment_origin_facility_id_key,
A.actual_conn_id as actual_conn_id,
A.actual_cons_id as actual_cons_id,
A.first_conn as first_conn,
A.tpt_compliance_flag as tpt_compliance_flag,
A.TPT_intransit_breach_flag as TPT_intransit_breach_flag,
A.shipment_tpt_dh_eta_datetime as shipment_tpt_dh_eta_datetime,
A.shipment_tpt_dh_eta_date_key as shipment_tpt_dh_eta_date_key,
if(unix_timestamp(A.fulfill_item_unit_dispatch_actual_time)>=unix_timestamp(A.fulfill_item_unit_dispatch_expected_time),
if(A.D_act_sec+7200<86400,A.D_act_sec+7200,A.D_act_sec-77400),
if(A.D_exp_sec+7200<86400,A.D_exp_sec+7200,A.D_exp_sec-77400)
) as MH_ready_time,
if(unix_timestamp(A.fulfill_item_unit_dispatch_actual_time)>=unix_timestamp(A.fulfill_item_unit_dispatch_expected_time),
if(A.D_act_sec+7200<86400,0,1),
if(A.D_exp_sec+7200<86400,0,1)
) as next_day_flag,
A.fulfill_item_unit_dispatch_expected_time as fulfill_item_unit_dispatch_expected_time,
A.fulfill_item_unit_dispatch_actual_time as fulfill_item_unit_dispatch_actual_time,
A.shipment_first_consignment_create_datetime as shipment_first_consignment_create_datetime,
A.actual_dh_recieve_date_time as DH_recieve_datetime,
lookup_date(to_date(A.actual_dh_recieve_date_time)) as DH_recieve_date_key

From

(Select 
Sh.fsd_assigned_hub_id_key as fsd_assigned_hub_id_key, 
Sh.shipment_origin_facility_id_key as shipment_origin_facility_id_key, 
Sh.vendor_tracking_id as vendor_tracking_id,
Sh.shipment_first_consignment_create_datetime as shipment_first_consignment_create_datetime,
ConsiTr.Conn_id as actual_conn_id,
ConsiTr.consignment_id as actual_cons_id,
ConsiA.first_conn as first_conn,
if(isnull(ConsiA.first_conn),1,0) as tpt_compliance_flag, 
if(unix_timestamp(Sh.shipment_first_consignment_create_datetime)+ConsiTr.eta_in_sec>unix_timestamp(Sh.fsd_first_dh_received_datetime),1,0) as TPT_intransit_breach_flag,
from_unixtime(unix_timestamp(Sh.shipment_first_consignment_create_datetime)+ConsiTr.eta_in_sec) as shipment_tpt_dh_eta_datetime,
lookup_date(to_date(from_unixtime(unix_timestamp(Sh.shipment_first_consignment_create_datetime)+ConsiTr.eta_in_sec))) as shipment_tpt_dh_eta_date_key,
ff.fulfill_item_unit_dispatch_expected_time as fulfill_item_unit_dispatch_expected_time,
ff.fulfill_item_unit_dispatch_actual_time as fulfill_item_unit_dispatch_actual_time,
hour(ff.fulfill_item_unit_dispatch_actual_time)*3600+minute(ff.fulfill_item_unit_dispatch_actual_time)*60+second(ff.fulfill_item_unit_dispatch_actual_time) as D_act_sec,
hour(ff.fulfill_item_unit_dispatch_expected_time)*3600+minute(ff.fulfill_item_unit_dispatch_expected_time)*60+second(ff.fulfill_item_unit_dispatch_expected_time) as D_exp_sec,
Sh.fsd_first_dh_received_datetime as actual_dh_recieve_date_time
 from
(Select is_large,product_categorization_hive_dim_key  
from bigfoot_external_neo.sp_product__product_categorization_hive_dim where is_large=1) prod_1 
left outer join 
(SELECT 
distinct order_item_id,
order_item_product_id_key
FROM bigfoot_external_neo.scp_oms__la_oms_fact 
) oiu_1 
on prod_1.product_categorization_hive_dim_key=oiu_1.order_item_product_id_key
Left Outer Join 
(Select 
fulfill_item_unit_dispatch_expected_time,
fulfill_item_unit_dispatch_actual_time,
shipment_merchant_reference_id,
fulfillment_order_item_id
from
bigfoot_external_neo.scp_fulfillment__la_fulfilment_fact) ff
on ff.fulfillment_order_item_id = oiu_1.order_item_id
 Left join 
(Select 
merchant_reference_id,
fsd_assigned_hub_id_key,
shipment_origin_facility_id_key,
vendor_tracking_id,
fsd_first_dh_received_datetime,
from_unixtime(unix_timestamp(concat(shipment_first_consignment_create_date_key, SUBSTR(LPAD(shipment_first_consignment_create_time_key,4,'0'),1,4)),'yyyyMMddHHmm')) as shipment_first_consignment_create_datetime,
shipment_first_consignment_id,
shipment_last_consignment_id
from 
bigfoot_external_neo.scp_ekl__la_shipment_l0_fact where shipment_carrier = 'FSD') Sh
on 
ff.shipment_merchant_reference_id = Sh.merchant_reference_id
Left join
(SELECT 
entityid,
cast(split(entityid, "-")[1] AS INT) as consignment_id,
unix_timestamp(`data`.created_at) as first_time,
`data`.connection_actual_tat as eta_in_sec,
`data`.connection_id as Conn_id
FROM 
bigfoot_snapshot.dart_wsr_scp_ekl_shipmentgroup_3_view_total where `data`.type = 'consignment')ConsiTr
on
Sh.shipment_last_consignment_id = ConsiTr.consignment_id
Left join
(Select 
 cast(split(entityid, "-")[1] AS INT) as Consignment_id,
 `data`.connection_id as first_conn
 from bigfoot_snapshot.dart_wsr_scp_ekl_shipmentgroup_3_view_total) ConsiA
on Sh.shipment_first_consignment_id = ConsiA.Consignment_id) A
 
Left join 
(Select
ekl_facility_hive_dim_key,
facility_id as origin_facility_id
From
bigfoot_external_neo.scp_ekl__ekl_facility_hive_dim) Origin
On A.shipment_origin_facility_id_key = Origin.ekl_facility_hive_dim_key

Left join 
(Select
ekl_facility_hive_dim_key as dest_fid_dim_key,
facility_id as dest_facility_id
From
bigfoot_external_neo.scp_ekl__ekl_facility_hive_dim) Dest
On A.fsd_assigned_hub_id_key = Dest.dest_fid_dim_key

Left join
(Select 
fkl_facility_id, 
mh_facility_id
from bigfoot_common.ekl_fkl_facility_mother_hub_mapping) B
on B.fkl_facility_id = Origin.origin_facility_id 

Left join 
(Select 
entityId as ideal_connection_id,
`data`.source_address.id as Origin_id,
`data`.destination_address.id as Dest_id,
concat(`data`.source_address.id,`data`.destination_address.id) as Conn_id,
`data`.group_id as groupId,
`data`.cutoff as cutoffTime
From
bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_view_total where `data`.state = 'active') C
 on concat(B.mh_facility_id,Dest.dest_facility_id) = concat(C.Origin_id,C.Dest_id))Q) 
M
where not(isnull(M.vendor_tracking_id)) and not(isnull(M.Time_diff))
)

MD where MD.r_no=1;
