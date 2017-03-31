INSERT OVERWRITE TABLE first_mile_returns_vertica_fact

Select
returntrackingid
,created_time
,first_inscan_at_ph_timestamp
,last_inscan_at_ph_timestamp
,outscanned_from_ph_time
,dispatched_to_seller_time
,scanned_at_seller_time
,returned_to_seller_time
,source_hub_key
,destination_hub_key
,sourcetype
,destinationtype
,last_updated_at
,current_return_status
,created_date_key
,created_time_key
,first_inscan_at_ph_date_key
,first_inscan_at_ph_time_key
,last_inscan_at_ph_date_key
,last_inscan_at_ph_time_key
,outscanned_from_ph_date_key
,outscanned_from_ph_time_key
,dispatched_to_seller_date_key
,dispatched_to_seller_time_key
,scanned_at_seller_daste_key
,scanned_at_seller_time_key
,mp_returm_promise_date
,returned_to_seller_date_key
,returned_to_seller_time_key
,rejected_by__seller_date_key
,rejected_by__seller_time_key
,undelivered_atempted_date_key
,undelivered_atempted_time_key
,undelivered_unatempted_date_key
,undelivered_unatempted_time_key
,untraceable_date_key
,untraceable_time_key
,termainating_timestamp
,termainating_date_key
,termainating_time_key
,new_promise_date_key
,return_type
,delivery_type
,current_facility_id_key,
flyer_id,
source_id,
new_promise_date_old_key,
flyer_status,

event_reason

from bigfoot_external_neo.scp_ekl__first_mile_retruns_hive_fact;