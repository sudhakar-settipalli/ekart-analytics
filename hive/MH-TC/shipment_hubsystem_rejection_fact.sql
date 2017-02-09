INSERT OVERWRITE TABLE shipment_hubsystem_rejection_fact
select
SP.shipment_id as shipment_id
,SP.shipment_origin_mh_facility_id
,lookupkey('facility_id',SP.shipment_origin_mh_facility_id) AS shipment_origin_mh_facility_id_key
,P.data.rejectionreason as primary_error_message
,S.data.rejectionreason as secondary_error_message
,(CASE 
     WHEN P.data.rejectionreason='SHIPMENT_OUT_OF_WAVE' THEN 'NEXT WAVE'
     WHEN P.data.rejectionreason='MAPPING_NOT_FOUND' THEN 'NO MAPPING'
     WHEN P.data.rejectionreason='NONE' AND S.data.currentprocessingareaname='ExceptionStation' THEN 'EXCEPTION STATION'
ELSE 'NO EXCEPTION'
END) as rejection_type
--,Actual_Pickup_time
--,WH_Dispatch_time
,from_unixtime(cast (P.updatedat/1000 as INT)) as primary_inscan_datetime
,lookup_date(from_unixtime(cast (P.updatedat/1000 as INT))) as primary_inscan_date_key
,lookup_time(from_unixtime(cast (P.updatedat/1000 as INT))) as primary_inscan_time_key
--,null as primary_station_name
,P.data.currentprocessingareaname as primary_station_name
--,null as secondary_station_name
,S.data.currentprocessingareaname as secondary_station_name
,from_unixtime(cast (S.updatedat/1000 as INT)) as secondary_inscan_datetime
,lookup_date(from_unixtime(cast (S.updatedat/1000 as INT))) as secondary_inscan_date_key
,lookup_time(from_unixtime(cast (S.updatedat/1000 as INT))) as secondary_inscan_time_key
--,split(split(CF.entityid,'_')[4],'-')[0] as mh_slot_start_time
--,split(split(CF.entityid,'_')[4],'-')[1] as mh_slot_end_time
,null as mh_slot_start_time
,null as mh_slot_end_time
--MH Slot start (ideal)
--MH slot end (ideal)
,SP.service_tier service_tier
,SP.ekl_shipment_type as ekl_shipment_type
,S.data.bagdestinationcoc coc_code
,SP.fsd_assigned_hub_id as fsd_assigned_hub_id
,lookupkey('facility_id',SP.fsd_assigned_hub_id) AS fsd_assigned_hub_id_key
,P.data.sortfacilityid as sort_facility_id
,lookupkey('facility_id',P.data.sortfacilityid) AS sort_facility_id_key
,P.data.currentslotid as current_slotid
,SP.vendor_tracking_id as vendor_tracking_id
from
bigfoot_external_neo.scp_ekl__shipment_l1_90_fact SP
LEFT JOIN
(
select 
data
,updatedat
,eventid
,eventtime
,entityid
from bigfoot_journal.dart_wsr_scp_ekl_hubsystemsortationdetail_3 where data.scantype='PRIMARY'
) P
ON (P.data.trackingid=SP.vendor_tracking_id)
LEFT JOIN
(select 
data
,updatedat
,eventid
,eventtime
,entityid
 from bigfoot_journal.dart_wsr_scp_ekl_hubsystemsortationdetail_3 where data.scantype='SECONDARY') S
ON (S.data.trackingid=P.data.trackingid and P.data.sortfacilityid=S.data.sortfacilityid and P.data.nextprocessingareaid=S.data.currentprocessingareaid)