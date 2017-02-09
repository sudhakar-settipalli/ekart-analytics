INSERT OVERWRITE TABLE shipment_hubsystem_l0_hive_fact
select
SP.shipment_id as shipment_id
,P.data.sortfacilityid as sort_facility_id
,lookupkey('facility_id',SP.shipment_origin_mh_facility_id) AS shipment_origin_mh_facility_id_key
,P.data.rejectionreason as error_message
,from_unixtime(cast (updatedat/1000 as INT)) as inscan_datetime
,lookup_date(from_unixtime(cast (updatedat/1000 as INT))) as inscan_date_key
,lookup_time(from_unixtime(cast (updatedat/1000 as INT))) as inscan_time_key
,data.currentprocessingareaname as inscan_area_name
,data.processingareatype as inscan_area_type
,data.nextprocessingareaname as next_area_name
,SP.service_tier as service_tier
,SP.ekl_shipment_type as ekl_shipment_type
,data.bagdestinationcoc as coc_code
,SP.fsd_assigned_hub_id as fsd_assigned_hub_id
,lookupkey('facility_id',SP.fsd_assigned_hub_id) AS fsd_assigned_hub_id_key
,SP.shipment_origin_mh_facility_id
,lookupkey('facility_id',P.data.sortfacilityid) AS sort_facility_id_key
,data.currentslotid as current_slotid
,P.data.trackingid as vendor_tracking_id
,P.data.sortedby as sortedby
,SP.shipment_carrier
,SP.ekl_fin_zone
,SP.seller_type
from bigfoot_journal.dart_wsr_scp_ekl_hubsystemsortationdetail_3 as P
LEFT JOIN 
bigfoot_external_neo.scp_ekl__shipment_l1_90_fact SP
ON (P.data.trackingid=SP.vendor_tracking_id)
where P.day > date_format(date_sub(current_date,90),'yyyyMMdd');