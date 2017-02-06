INSERT OVERWRITE TABLE shipment_facility_l0_90_fact
select
consignment_id
,bag_id
,vendor_tracking_id
,consignment_system_weight
,consignment_connection_id
,consignment_type
,consignment_mode
,connection_type
,connection_transit_type
,connection_code
,connection_frequency_type
,consignment_status
,consignment_co_loader
,consignment_breach_flag
,consignment_movement_flag
,consignment_no_of_bags
,consignment_no_of_shipments
,consignment_tms_compliant_flag
,consignment_creation_delay_in_hours
,consignment_source_hub_type
,consignment_destination_hub_type
,consignment_source_hub_id
,consignment_destination_hub_id
,consignment_source_hub_id_key
,consignment_destination_hub_id_key
,connection_cut_off_datetime
,consignment_create_datetime
,consignment_create_unixseconds
,consignment_status_date_time
,consignment_eta_datetime
,consignment_received_datetime
,bag_tracking_id
,bag_service_type
,bag_seal_id
,bag_current_status
,bag_category
,bag_no_of_shipments
,bag_eta_update_reason
,bag_system_weight
,bag_source_hub_id
,bag_current_hub_id
,bag_destination_hub_id
,bag_source_hub_id_key
,bag_current_hub_id_key
,bag_destination_hub_id_key
,bag_created_hub_type
,bag_current_hub_type
,bag_destination_hub_type
,bag_created_at_datetime
,bag_open_datetime
,bag_closed_datetime
,bag_connected_datetime
,bag_destination_hub_inscan_datetime
,bag_receive_datetime
,bag_last_update_datetime
,hop_source_type
from
(
select 
x.consignment_id as consignment_id,
b.bag_id as bag_id,
c.shipment_id as vendor_tracking_id,
x.consignment_system_weight,
x.consignment_connection_id,
x.consignment_type,
x.consignment_mode,
x.connection_type,
x.connection_transit_type,
x.connection_code,
x.connection_frequency_type,
x.consignment_status,
x.consignment_co_loader,
x.consignment_breach_flag,
x.consignment_movement_flag,
x.consignment_no_of_bags,
x.consignment_no_of_shipments,
x.tms_compliant_consignments as consignment_tms_compliant_flag,
x.consignment_creation_delay_in_hours,
x.consignment_source_hub_type,
x.consignment_destination_hub_type,
x.consignment_source_hub_id as consignment_source_hub_id,
x.consignment_destination_hub_id as consignment_destination_hub_id,
lookupkey('facility_id',x.consignment_source_hub_id) AS consignment_source_hub_id_key,
lookupkey('facility_id',x.consignment_destination_hub_id) AS consignment_destination_hub_id_key,
x.connection_cut_off_datetime AS connection_cut_off_datetime,
x.consignment_create_datetime AS consignment_create_datetime,
unix_timestamp(x.consignment_create_datetime) AS consignment_create_unixseconds,
x.consignment_status_date_time AS consignment_status_date_time,
x.consignment_eta_datetime AS consignment_eta_datetime,
x.consignment_received_datetime AS consignment_received_datetime,
bag_table.bag_tracking_id,
bag_table.bag_service_type,
bag_table.bag_seal_id,
bag_table.bag_current_status,
bag_table.bag_category,
bag_table.bag_no_of_shipments,
bag_table.bag_eta_update_reason,
bag_table.bag_system_weight,
bag_table.bag_source_hub_id,
bag_table.bag_current_hub_id,
bag_table.bag_destination_hub_id,
bag_table.bag_source_hub_id_key,
bag_table.bag_current_hub_id_key,
bag_table.bag_destination_hub_id_key,
bag_table.bag_created_hub_type,
bag_table.bag_current_hub_type,
bag_table.bag_destination_hub_type,
bag_table.bag_created_at_datetime,
bag_table.bag_open_datetime,
bag_table.bag_closed_datetime,
bag_table.bag_connected_datetime,
bag_table.bag_destination_hub_inscan_datetime,
bag_table.bag_receive_datetime,
bag_table.bag_last_update_datetime,
'BAG' as hop_source_type
from bigfoot_external_neo.scp_ekl__consignment_l1_90_fact x
left join bigfoot_external_neo.scp_ekl__bag_consignment_map_90_fact b on b.consignment_id = x.consignment_id
left join bigfoot_external_neo.scp_ekl__shipment_closedbag_map_l1_90_fact c on CAST(SPLIT(c.bag, "-")[1] AS INT)  = b.bag_id
left join 
(
SELECT
CAST( SPLIT(bag_history.entityid, "-")[1] AS INT) AS bag_id,
bag_history.location_id,
bag_snapshot.`data`.vendor_tracking_id AS bag_tracking_id,
if(substr(bag_snapshot.`data`.vendor_tracking_id,1,1)='E',
    'ECONOMY',
    if(substr(bag_snapshot.`data`.vendor_tracking_id,1,1)='R',
        'REGULAR',
        'NONE'
        )
    ) AS bag_service_type,
bag_snapshot.`data`.seal_id AS bag_seal_id,
bag_snapshot.`data`.status AS bag_current_status,
concat_ws('-', bag_snapshot.`data`.attributes) AS bag_category,
size(bag_snapshot.`data`.shipments) AS bag_no_of_shipments,
If(size(bag_snapshot.`data`.notes) = 0, NULL, If(bag_snapshot.`data`.notes[0].type = 'EtaUpdate Notes',bag_snapshot.`data`.notes[0].flag,NULL)) as bag_eta_update_reason,
bag_snapshot.`data`.system_weight.physical AS bag_system_weight,
bag_snapshot.`data`.source_location.id AS bag_source_hub_id,
bag_snapshot.`data`.current_location.id AS bag_current_hub_id,
bag_snapshot.`data`.destination_location.id AS bag_destination_hub_id,
lookupkey('facility_id',bag_snapshot.`data`.source_location.id) AS bag_source_hub_id_key,
lookupkey('facility_id',bag_snapshot.`data`.current_location.id) AS bag_current_hub_id_key,
lookupkey('facility_id',bag_snapshot.`data`.destination_location.id) AS bag_destination_hub_id_key,
bag_snapshot.`data`.source_location.type as bag_created_hub_type,
bag_snapshot.`data`.current_location.type as bag_current_hub_type,
bag_snapshot.`data`.destination_location.type as bag_destination_hub_type,
bag_snapshot.`data`.created_at as bag_created_at_datetime,
CAST(bag_history.first_open_time AS TIMESTAMP) AS bag_open_datetime,
CAST(bag_history.first_close_time AS TIMESTAMP) as bag_closed_datetime,
CAST(bag_history.bag_first_connected_time AS TIMESTAMP) as bag_connected_datetime,
CAST(bag_history.bag_destination_hub_inscan_time AS TIMESTAMP) as bag_destination_hub_inscan_datetime,
CAST(bag_history.bag_receive_time AS TIMESTAMP) AS bag_receive_datetime,
CAST(bag_snapshot.updatedat AS TIMESTAMP) AS bag_last_update_datetime
FROM
(SELECT entityid,
       `data`.current_location.id as location_id,
       MIN(IF(`data`.status='OPEN', updatedat, NULL)) AS first_open_time,
       MIN(IF(`data`.status='CLOSED', updatedat, NULL)) AS first_close_time,
       MIN(IF(`data`.status='INTRANSIT', updatedat, NULL)) AS bag_first_connected_time,
       MIN(IF(`data`.status='REACHED', updatedat, NULL)) AS bag_destination_hub_inscan_time,
       MIN(IF(`data`.status='RECEIVED', updatedat, NULL)) AS bag_receive_time
FROM bigfoot_journal.dart_wsr_scp_ekl_shipmentgroup_3
WHERE lower(`data`.type) = 'bag'
and  day  > date_format(date_sub(current_date,120),'yyyyMMdd')
GROUP BY entityid,
`data`.current_location.id
) bag_history
INNER JOIN bigfoot_snapshot.dart_wsr_scp_ekl_shipmentgroup_3_view bag_snapshot ON (bag_snapshot.entityid = bag_history.entityid)
) bag_table
on bag_table.bag_id =  b.bag_id and bag_table.location_id = x.consignment_destination_hub_id
where x.consignment_type = 'BAG' 
union all
select i.consignment_id,
null as bag_id, 
h.shipment_id as vendor_tracking_id,
i.consignment_system_weight as consignment_system_weight,
i.consignment_connection_id,
i.consignment_type,
i.consignment_mode,
i.connection_type,
i.connection_transit_type,
i.connection_code,
i.connection_frequency_type,
i.consignment_status,
i.consignment_co_loader,
i.consignment_breach_flag,
i.consignment_movement_flag,
i.consignment_no_of_bags,
i.consignment_no_of_shipments,
i.tms_compliant_consignments as consignment_tms_compliant_flag,
i.consignment_creation_delay_in_hours,
i.consignment_source_hub_type,
i.consignment_destination_hub_type,
i.consignment_source_hub_id as consignment_source_hub_id,
i.consignment_destination_hub_id as consignment_destination_hub_id,
lookupkey('facility_id',i.consignment_source_hub_id) AS consignment_source_hub_id_key,
lookupkey('facility_id',i.consignment_destination_hub_id) AS consignment_destination_hub_id_key,
i.connection_cut_off_datetime AS connection_cut_off_datetime,
i.consignment_create_datetime AS consignment_create_datetime,
unix_timestamp(i.consignment_create_datetime) AS consignment_create_unixseconds,
i.consignment_status_date_time AS consignment_status_date_time,
i.consignment_eta_datetime AS consignment_eta_datetime,
i.consignment_received_datetime AS consignment_received_datetime,
null as bag_tracking_id,
null as bag_service_type,
null as bag_seal_id,
null as bag_current_status,
null as bag_category,
null as bag_no_of_shipments,
null as bag_eta_update_reason,
null as bag_system_weight,
null as bag_source_hub_id,
null as bag_current_hub_id,
null as bag_destination_hub_id,
null as bag_source_hub_id_key,
null as bag_current_hub_id_key,
null as bag_destination_hub_id_key,
null as bag_created_hub_type,
null as bag_current_hub_type,
null as bag_destination_hub_type,
null as bag_created_at_datetime,
null as bag_open_datetime,
null as bag_closed_datetime,
null as bag_connected_datetime,
null as bag_destination_hub_inscan_datetime,
null as bag_receive_datetime,
null as bag_last_update_datetime,
'SHIPMENT' as hop_source_type
from bigfoot_external_neo.scp_ekl__consignment_l1_90_fact i
left join bigfoot_external_neo.scp_ekl__shipment_consignment_map_l1_90_fact h on i.consignment_id = CAST(SPLIT(h.consignment_id, "-")[1] AS INT)
where i.consignment_type = 'SHIPMENT' 

union all 
select 
FMPR.bag_consignment_id as consignment_id,
FMPR.bag_tracking_id  as bag_id,
FMPR.tracking_id as vendor_tracking_id,
null as consignment_system_weight,
null as consignment_connection_id,
null as consignment_type,
null as consignment_mode,
null as connection_type,
null as connection_transit_type,
null as connection_code,
null as connection_frequency_type,
null as consignment_status,
null as consignment_co_loader,
null as consignment_breach_flag,
null as consignment_movement_flag,
null as consignment_no_of_bags,
null as consignment_no_of_shipments,
null as consignment_tms_compliant_flag,
null as consignment_creation_delay_in_hours,
null as consignment_source_hub_type,
null as consignment_destination_hub_type,
FMPR.mp_pickup_hub_id as consignment_source_hub_id ,
null as consignment_destination_hub_id,
FMPR.mp_pickup_hub_key as  consignment_source_hub_id_key,
null as  consignment_destination_hub_id_key,
null as connection_cut_off_datetime,
FMPR.mp_dispatched_to_tc_date_time as  consignment_create_datetime,
unix_timestamp(FMPR.mp_dispatched_to_tc_date_time) AS consignment_create_unixseconds,
null as  consignment_status_date_time,
null as consignment_eta_datetime,
FMPR.consignment_receive_at_mh_datetime as  consignment_received_datetime,
FMPR.bag_tracking_id as bag_tracking_id,
null as bag_service_type,
null as bag_seal_id,
null as bag_current_status,
null as bag_category,
null as bag_no_of_shipments,
null as bag_eta_update_reason,
null as bag_system_weight,
null as bag_source_hub_id,
null as bag_current_hub_id,
null as bag_destination_hub_id,
null as bag_source_hub_id_key,
null as bag_current_hub_id_key,
null as bag_destination_hub_id_key,
null as bag_created_hub_type,
null as bag_current_hub_type,
null as bag_destination_hub_type,
null as bag_created_at_datetime,
null as bag_open_datetime,
null as bag_closed_datetime,
null as bag_connected_datetime,
FMPR.bag_receive_at_mh_datetime as bag_destination_hub_inscan_datetime,
FMPR.bag_receive_at_mh_datetime as bag_receive_datetime,
null as bag_last_update_datetime,
'MP_BAG' as hop_source_type
FROM 
(select
bag_consignment_id,
bag_tracking_id,
tracking_id,
mp_pickup_hub_id,
mp_pickup_hub_key,
mp_dispatched_to_tc_date_time,
consignment_receive_at_mh_datetime,
bag_receive_at_mh_datetime
FROM 
(select
bag_consignment_id,
bag_tracking_id,
tracking_id,
mp_pickup_hub_id,
mp_pickup_hub_key,
mp_dispatched_to_tc_date_time,
consignment_receive_at_mh_datetime,
bag_receive_at_mh_datetime,
ROW_NUMBER() over (PARTITION BY tracking_id order by consignment_receive_at_mh_datetime DESC
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as rank 
from bigfoot_external_neo.scp_ekl__first_mile_pickup_request_hive_fact) ranked
where rank = 1 
)FMPR
) as final where vendor_tracking_id is not null;
