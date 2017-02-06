INSERT OVERWRITE TABLE first_mile_shipment_bag_hive_fact
SELECT
fmbf.bag_id,
fmbf.bag_tracking_id,
fmbf.bag_seal_id,
fmbf.bag_destination_hub_id,
fmbf.bag_created_at_datetime,
fmbf.bag_source_hub_id,
fmbf.bag_lzn_tag,
fmbf.bag_current_status,
fmbf.bag_current_updated_at_datetime,
fmbf.bag_close_datetime,
fmbf.bag_dispatched_datetime,
fmbf.bag_open_datetime,
fmbf.bag_receive_at_mh_datetime,
fmbf.bag_created_at_date_key,
fmbf.bag_created_at_time_key,
fmbf.bag_current_updated_at_date_key,
fmbf.bag_current_updated_at_time_key,
fmbf.bag_close_date_key,
fmbf.bag_close_time_key,
fmbf.bag_dispatched_date_key,
fmbf.bag_dispatched_time_key,
fmbf.bag_open_date_key,
fmbf.bag_open_time_key,
fmbf.bag_receive_at_mh_date_key,
fmbf.bag_receive_at_mh_time_key,
fmbf.bag_destination_hub_id_key,
fmbf.bag_source_hub_id_key,
fmbsmf.shipment_id,
if(ship.`data`.servicerequesttype='PickupRequest',ship.`data`.servicerequestid,NULL) as pickup_request_id,
pr.`data`.status as shipment_current_status,
pr.`data`.updatedat as shipment_current_status_datetime,
lookup_date(cast(pr.`data`.updatedat as timestamp)) as shipment_current_status_date_key,
lookup_time(cast(pr.`data`.updatedat as timestamp)) as shipment_current_status_time_key,
pr.`data`.trackingid as tracking_id,
cons.`data`.consignmentid as bag_consignment_id,
fmbf.consignment_receive_at_mh_datetime,
lookup_date(cast(fmbf.consignment_receive_at_mh_datetime as timestamp)) as consignment_receive_at_mh_date_key,
lookup_time(cast(fmbf.consignment_receive_at_mh_datetime as timestamp)) as consignment_receive_at_mh_time_key,

C.bag_timestamp,
lookup_date(cast(C.bag_timestamp as timestamp)) as bag_date_key,
lookup_time(cast(C.bag_timestamp as timestamp)) as bag_time_key,
C.bag_by_handheld_timestamp as bag_by_handheld_timestamp,
lookup_date(cast(C.bag_by_handheld_timestamp as timestamp)) as bag_by_handheld_date_key,
lookup_time(cast(C.bag_by_handheld_timestamp as timestamp)) as bag_by_handheld_time_key,
null as bag_event
FROM bigfoot_external_neo.scp_ekl__first_mile_bag_hive_fact fmbf
left join bigfoot_external_neo.scp_ekl__first_mile_bag_shipmment_map_hive_fact fmbsmf
ON fmbsmf.bag_tracking_id=fmbf.Bag_id
left join bigfoot_snapshot.dart_wsr_scp_ekl_firtmileshipment_5_0_view ship
ON ship.`data`.shipmentid=fmbsmf.shipment_id

left join (Select 
A.shipmentid,
min(A.bag_timestamp) as bag_timestamp,
min(A.bag_by_handheld_timestamp) as bag_by_handheld_timestamp
from
(Select shipv2.`data`.shipmentid as shipmentid,
if(shipv2.`data`.event='bag',shipv2.`data`.updatedat,NULL) as bag_timestamp,
if(shipv2.`data`.event='bag_by_handheld',shipv2.`data`.updatedat,NULL) as bag_by_handheld_timestamp,
shipv2.`data`.event as bag_event 
from
bigfoot_journal.dart_wsr_scp_ekl_firtmileshipment_5_0 shipv2
where shipv2.`data`.event in('bag','bag_by_handheld') and shipv2.`data`.servicerequesttype='PickupRequest'
)A group by A.shipmentid)C
ON C.shipmentid=fmbsmf.shipment_id
left join bigfoot_snapshot.dart_wsr_scp_ekl_firstmilepickuprequest_2_view pr
on ship.`data`.servicerequestid=pr.`data`.pickuprequestid
left join
bigfoot_snapshot.dart_wsr_scp_ekl_firtmilebag_2_0_view cons
ON fmbsmf.bag_tracking_id=cons.`data`.bagid;
