INSERT OVERWRITE TABLE first_mile_bag_hive_fact
SELECT 
Bag_view.bag_id,
Bag_view.bag_tracking_id,
Bag_view.bag_seal_id,
Bag_view.bag_destination_hub_id,
Bag_view.bag_created_at_datetime,
Bag_view.bag_source_hub_id,
Bag_view.bag_lzn_tag,
Bag_view.bag_current_status,
Bag_view.bag_current_updated_at_datetime,
Bag_history.bag_close_datetime,
Bag_history.bag_dispatched_datetime,
Bag_history.bag_open_datetime,
Bag_history.bag_receive_at_mh_datetime,
lookup_date(cast(Bag_view.bag_created_at_datetime as timestamp)) as bag_created_at_date_key,
lookup_time(cast(Bag_view.bag_created_at_datetime as timestamp)) as bag_created_at_time_key,
lookup_date(cast(Bag_view.bag_current_updated_at_datetime as timestamp)) as bag_current_updated_at_date_key,
lookup_time(cast(Bag_view.bag_current_updated_at_datetime as timestamp)) as bag_current_updated_at_time_key,
lookup_date(cast(Bag_history.bag_close_datetime as timestamp)) as bag_close_date_key,
lookup_time(cast(Bag_history.bag_close_datetime as timestamp)) as bag_close_time_key,
lookup_date(cast(Bag_history.bag_dispatched_datetime as timestamp)) as bag_dispatched_date_key,
lookup_time(cast(Bag_history.bag_dispatched_datetime as timestamp)) as bag_dispatched_time_key,
lookup_date(cast(Bag_history.bag_open_datetime as timestamp)) as bag_open_date_key,
lookup_time(cast(Bag_history.bag_open_datetime as timestamp)) as bag_open_time_key,
lookup_date(cast(Bag_history.bag_receive_at_mh_datetime as timestamp)) as bag_receive_at_mh_date_key,
lookup_time(cast(Bag_history.bag_receive_at_mh_datetime as timestamp)) as bag_receive_at_mh_time_key,
lookupkey('facility_id',Bag_view.bag_destination_hub_id) as bag_destination_hub_id_key,
lookupkey('facility_id',Bag_view.bag_source_hub_id) as bag_source_hub_id_key,
Bag_view.bag_consignment_id,
consignmentid_history.consignment_receive_at_mh_datetime
FROM
(select
`data`.bagid as bag_id,
`data`.bagtrackingid as bag_tracking_id,
`data`.sealid as bag_seal_id,
if(`data`.destinationfacility.owner="fkl_facility",`data`.destinationfacility.ownerid, NULL) as bag_destination_hub_id,
`data`.createdat as bag_created_at_datetime,
if(`data`.fulfillmentfacility.owner="pickup_hub",`data`.fulfillmentfacility.ownerid, NULL) as bag_source_hub_id,
`data`.bagtype as bag_LZN_tag,
`data`.status as bag_current_status,
`data`.updatedat as bag_current_updated_at_datetime,
`data`.consignmentid as bag_consignment_id
FROM 
bigfoot_snapshot.dart_wsr_scp_ekl_firtmilebag_2_0_view) Bag_view
left join 
(select 
`data`.bagid as Bag_id,
min(if(lower(`data`.status) IN ('closed', 'closed_by_handheld'),`data`.updatedat,NULL)) as bag_close_datetime,
min(if(lower(`data`.status) IN ('dispatched', 'dispatched_to_ph','outscanned'),`data`.updatedat,NULL)) as bag_dispatched_datetime,
min(if(lower(`data`.status) = 'opened',`data`.updatedat,NULL)) as bag_open_datetime,
min(if(lower(`data`.status) = 'received_at_mh',`data`.updatedat,NULL)) as bag_receive_at_mh_datetime
FROM 
bigfoot_journal.dart_wsr_scp_ekl_firtmilebag_2_0
group by `data`.bagid) Bag_history
ON Bag_view.bag_id=Bag_history.Bag_id 
left join 
(select 
`data`.consignmentid as bag_consignment_id,
min(if(lower(`data`.status) = ('received_at_mh'),`data`.updatedat,NULL)) as consignment_receive_at_mh_datetime
FROM 
bigfoot_journal.dart_wsr_scp_ekl_firtmilebag_2_0
group by `data`.consignmentid) consignmentid_history 
ON Bag_view.bag_consignment_id=consignmentid_history.bag_consignment_id where Bag_view.bag_consignment_id is not null

