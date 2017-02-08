INSERT OVERWRITE TABLE first_mile_retruns_hive_fact
Select 
D.returntrackingid,
D.created_time,
D.first_inscan_at_ph_timestamp,
D.last_inscan_at_ph_timestamp,
D.outscanned_from_ph_time,
D.dispatched_to_seller_time,
D.scanned_at_seller_time,
D.returned_to_seller_time,
D.current_source_key,
D.current_destination_key,
D.sourcetype,
D.destinationtype,
D.last_updated_at,
D.current_return_status,
D.created_date_key,
D.created_time_key,
D.first_inscan_at_ph_date_key,
D.first_inscan_at_ph_time_key,
D.last_inscan_at_ph_date_key,
D.last_inscan_at_ph_time_key,
D.outscanned_from_ph_date_key,
D.outscanned_from_ph_time_key,
D.dispatched_to_seller_date_key,
D.dispatched_to_seller_time_key,
D.scanned_at_seller_date_key,
D.scanned_at_seller_time_key,
D.mp_return_promise_date,
D.returned_to_seller_date_key,
D.returned_to_seller_time_key,
D.rejected_by__seller_date_key,
D.rejected_by__seller_time_key,
D.undelivered_atempted_date_key,
D.undelivered_atempted_time_key,
D.undelivered_unatempted_date_key,
D.undelivered_unatempted_time_key,
D.untraceable_date_key,
D.untraceable_time_key,
D.termainating_timestamp,
D.termainating_date_key,
D.termainating_time_key,

if((D.mp_return_promise_date is null and D.termainating_date_key is not null),D.termainating_date_key,
if(D.termainating_date_key<=lookup_date(D.mp_return_promise_date),D.termainating_date_key,
if(HH.HH_next_working_date_key IS NOT NULL,HH.HH_next_working_date_key,lookup_date(D.mp_return_promise_date)))) as new_promise_date_key,

D.return_type,
D.delivery_type,
D.current_location,

D.flyer_id,
D.source_id,

if(Upper(D.current_return_status) in ('REJECTED_BY_SELLER','RETURNED_TO_SELLER','UNTRACEABLE'), termainating_date_key,
if(FROM_UNIXTIME( UNIX_TIMESTAMP() ,'u')=1,lookup_date(D.mp_return_promise_date),
if((datediff(to_date(FROM_UNIXTIME(UNIX_TIMESTAMP())),
CONCAT(SUBSTR(D.mp_return_promise_date,1,4),'-',SUBSTR(D.mp_return_promise_date,5,2),'-',SUBSTR(D.mp_return_promise_date,7,2))
)>=2 AND D.current_return_status NOT IN ('EXPECTED','not_received') AND (D.termainating_date_key IS NULL OR datediff(to_date(FROM_UNIXTIME( UNIX_TIMESTAMP())),
CONCAT(SUBSTR(D.termainating_date_key,1,4),'-',SUBSTR(D.termainating_date_key,5,2),'-',SUBSTR(D.termainating_date_key,7,2))
)=1)) ,
lookup_date(date_sub(FROM_UNIXTIME(UNIX_TIMESTAMP()),1)),
if(D.last_inscan_at_ph_date_key=D.termainating_date_key, D.last_inscan_at_ph_date_key,lookup_date(D.mp_return_promise_date))))) as new_promise_date_old_key,
D.flyer_status

from

(Select 
B.returntrackingid,
B.created_time as created_time,
B.first_inscan_at_ph_timestamp as first_inscan_at_ph_timestamp,
B.last_inscan_at_ph_timestamp as last_inscan_at_ph_timestamp,
B.outscanned_from_ph_time as outscanned_from_ph_time,
B.dispatched_to_seller_time as dispatched_to_seller_time,
B.scanned_at_seller_time as scanned_at_seller_time,
B.returned_to_seller_time as returned_to_seller_time,
D.current_source_key as current_source_key,
D.current_destination_key as current_destination_key,
D.sourcetype,
D.destinationtype,
D.last_updated_at as last_updated_at,
D.current_return_status as current_return_status,
lookup_date(cast(B.created_time as timestamp)) as created_date_key,
lookup_time(cast(B.created_time as timestamp)) as created_time_key,
lookup_date(cast(B.first_inscan_at_ph_timestamp as timestamp)) as first_inscan_at_ph_date_key,
lookup_time(cast(B.first_inscan_at_ph_timestamp as timestamp)) as first_inscan_at_ph_time_key,
lookup_date(cast(B.last_inscan_at_ph_timestamp as timestamp)) as last_inscan_at_ph_date_key,
lookup_time(cast(B.last_inscan_at_ph_timestamp as timestamp)) as last_inscan_at_ph_time_key,
lookup_date(cast(B.outscanned_from_ph_time as timestamp)) as outscanned_from_ph_date_key,
lookup_time(cast(B.outscanned_from_ph_time as timestamp)) as outscanned_from_ph_time_key,
lookup_date(cast(B.dispatched_to_seller_time as timestamp)) as dispatched_to_seller_date_key,
lookup_time(cast(B.dispatched_to_seller_time as timestamp)) as dispatched_to_seller_time_key,
lookup_date(cast(B.scanned_at_seller_time as timestamp)) as scanned_at_seller_date_key,
lookup_time(cast(B.scanned_at_seller_time as timestamp)) as scanned_at_seller_time_key,
if(B.first_inscan_at_ph_timestamp IS NOT NULL,
if((from_unixtime(unix_timestamp(B.first_inscan_at_ph_timestamp,'yyyyMMdd'),'u')=6 and hour(B.first_inscan_at_ph_timestamp)>11),
date_add(to_date(B.first_inscan_at_ph_timestamp),2),
if((from_unixtime(unix_timestamp(B.first_inscan_at_ph_timestamp,'yyyyMMdd'),'u')=7 and hour(B.first_inscan_at_ph_timestamp)<11),
date_add(to_date(B.first_inscan_at_ph_timestamp),1),
if(hour(B.first_inscan_at_ph_timestamp)>11,date_add(to_date(B.first_inscan_at_ph_timestamp),1),to_date(B.first_inscan_at_ph_timestamp)))),NULL) as mp_return_promise_date,

B.mp_num_of_return_reattempts_temp,
B.rejected_by__seller_time,
B.undelivered_atempted_time,
B.undelivered_unatempted_time,
B.untraceable_time,
lookup_date(cast(B.returned_to_seller_time as timestamp)) as returned_to_seller_date_key,
lookup_time(cast(B.returned_to_seller_time as timestamp)) as returned_to_seller_time_key,
lookup_date(cast(B.rejected_by__seller_time as timestamp)) as rejected_by__seller_date_key,
lookup_time(cast(B.rejected_by__seller_time as timestamp)) as rejected_by__seller_time_key,
lookup_date(cast(B.undelivered_atempted_time as timestamp)) as undelivered_atempted_date_key,
lookup_time(cast(B.undelivered_atempted_time as timestamp)) as undelivered_atempted_time_key,
lookup_date(cast(B.undelivered_unatempted_time as timestamp)) as undelivered_unatempted_date_key,
lookup_time(cast(B.undelivered_unatempted_time as timestamp)) as undelivered_unatempted_time_key,
lookup_date(cast(B.untraceable_time as timestamp)) as untraceable_date_key,
lookup_time(cast(B.untraceable_time as timestamp)) as untraceable_time_key,
B.termainating_timestamp,
lookup_date(cast(B.termainating_timestamp as timestamp)) as termainating_date_key,
lookup_time(cast(B.termainating_timestamp as timestamp)) as termainating_time_key,
D.return_type,
D.delivery_type,
D.current_location,
D.flyer_id,
D.source_id,
D.flyer_status



from
(Select 
A.returntrackingid,
min(A.created_time) as created_time,
min(A.inscan_at_ph_timestamp) as first_inscan_at_ph_timestamp,
max(A.inscan_at_ph_timestamp) as last_inscan_at_ph_timestamp,
max(A.outscanned_from_ph_time) as outscanned_from_ph_time,
min(A.dispatched_to_seller_time) as dispatched_to_seller_time,
min(A.scanned_at_seller_time) as scanned_at_seller_time,
min(A.returned_to_seller_time) as returned_to_seller_time,
sum(if(A.status='return_reattempt',1,0)) as mp_num_of_return_reattempts_temp,
min(A.rejected_by__seller_time) as rejected_by__seller_time ,
min(A.undelivered_atempted_time) as undelivered_atempted_time,
min(A.undelivered_unatempted_time) as undelivered_unatempted_time,
min(A.untraceable_time) as untraceable_time,
min(A.termainating_timestamp) as termainating_timestamp
from
(Select 
fmret.data.sourceid,
fmret.data.returntrackingid,
fmret.data.sourcetype,
fmret.data.destinationtype,
fmret.data.destinationid,
fmret.data.remarks,
fmret.data.updatedat,
fmret.data.trackingid,
fmret.data.status,
if(fmret.data.status='expected',data.updatedat,NULL) as created_time,
if(fmret.data.status='received_at_ph',data.updatedat,NULL) as inscan_at_ph_timestamp,
if(fmret.data.status='outscanned',data.updatedat,NULL) as outscanned_from_ph_time,
if(fmret.data.status='dispatched_to_seller',data.updatedat,NULL) as dispatched_to_seller_time,
if(fmret.data.status='scanned_at_seller',data.updatedat,NULL) as scanned_at_seller_time,
if(fmret.data.status='returned_to_seller',data.updatedat,NULL) as returned_to_seller_time,
if(fmret.data.status='rejected_by_seller',data.updatedat,NULL) as rejected_by__seller_time,
if(fmret.data.status='undelivered_attempted',data.updatedat,NULL) as undelivered_atempted_time,
if(fmret.data.status='undelivered_unattempted',data.updatedat,NULL) as undelivered_unatempted_time,
if(fmret.data.status='untraceable',data.updatedat,NULL) as untraceable_time,
if(fmret.data.status in('untraceable','returned_to_seller','rejected_by_seller'),data.updatedat,NULL) as termainating_timestamp

from bigfoot_journal.dart_wsr_scp_ekl_firstmilereturnrequest_1 fmret) A group by A.returntrackingid)B
left join
( 
 Select
 lookupkey('facility_id', data.sourceid) as current_source_key,
 lookupkey('facility_id', data.destinationid) as current_destination_key,
 data.returntrackingid,
 data.sourcetype,
 data.destinationtype,
 data.updatedat as last_updated_at,
 data.status as current_return_status,
 data.returntype as return_type,
 data.deliveryType as delivery_type,
data.currentfacilityId as Current_location,
data.capturedflyer as flyer_id,
data.sourceid as source_id,
data.flyerstatus as flyer_status
  
from bigfoot_snapshot.dart_wsr_scp_ekl_firstmilereturnrequest_1_view
)D
on B.returntrackingid=D.returntrackingid) D

left join

(select 
A.`data`.hubId as hh_hub_id,lookup_date(A.`data`.holidayDate) as hh_holiday_date_key,min(case when C.`data`.holidayDate is null then lookup_date(date_add(B.`data`.holidayDate,1)) else null end) as hh_next_working_date_key
from bigfoot_snapshot.dart_wsr_scp_ekl_firstmilehubholidays_1_view_total A 
left join bigfoot_snapshot.dart_wsr_scp_ekl_firstmilehubholidays_1_view_total B on (A.`data`.hubId=B.`data`.hubId  )
left join bigfoot_snapshot.dart_wsr_scp_ekl_firstmilehubholidays_1_view_total C on (B.`data`.hubId=C.`data`.hubId and C.`data`.holidayDate=date_add(B.`data`.holidayDate,1))
where A.`data`.holidayDate<=B.`data`.holidayDate and A.`data`.isholidayflag=TRUE
group by A.`data`.hubId,lookup_date(A.`data`.holidayDate)) HH

on (HH.hh_hub_id=D.source_id and HH.hh_holiday_date_key=lookup_date(D.mp_return_promise_date));
INSERT OVERWRITE TABLE first_mile_retruns_hive_fact
Select 
D.returntrackingid,
D.created_time,
D.first_inscan_at_ph_timestamp,
D.last_inscan_at_ph_timestamp,
D.outscanned_from_ph_time,
D.dispatched_to_seller_time,
D.scanned_at_seller_time,
D.returned_to_seller_time,
D.current_source_key,
D.current_destination_key,
D.sourcetype,
D.destinationtype,
D.last_updated_at,
D.current_return_status,
D.created_date_key,
D.created_time_key,
D.first_inscan_at_ph_date_key,
D.first_inscan_at_ph_time_key,
D.last_inscan_at_ph_date_key,
D.last_inscan_at_ph_time_key,
D.outscanned_from_ph_date_key,
D.outscanned_from_ph_time_key,
D.dispatched_to_seller_date_key,
D.dispatched_to_seller_time_key,
D.scanned_at_seller_date_key,
D.scanned_at_seller_time_key,
D.mp_return_promise_date,
D.returned_to_seller_date_key,
D.returned_to_seller_time_key,
D.rejected_by__seller_date_key,
D.rejected_by__seller_time_key,
D.undelivered_atempted_date_key,
D.undelivered_atempted_time_key,
D.undelivered_unatempted_date_key,
D.undelivered_unatempted_time_key,
D.untraceable_date_key,
D.untraceable_time_key,
D.termainating_timestamp,
D.termainating_date_key,
D.termainating_time_key,

if((D.mp_return_promise_date is null and D.termainating_date_key is not null),D.termainating_date_key,
if(D.termainating_date_key<=lookup_date(D.mp_return_promise_date),D.termainating_date_key,
if(HH.HH_next_working_date_key IS NOT NULL,HH.HH_next_working_date_key,lookup_date(D.mp_return_promise_date)))) as new_promise_date_key,

D.return_type,
D.delivery_type,
D.current_location,

D.flyer_id,
D.source_id,

if(Upper(D.current_return_status) in ('REJECTED_BY_SELLER','RETURNED_TO_SELLER','UNTRACEABLE'), termainating_date_key,
if(FROM_UNIXTIME( UNIX_TIMESTAMP() ,'u')=1,lookup_date(D.mp_return_promise_date),
if((datediff(to_date(FROM_UNIXTIME(UNIX_TIMESTAMP())),
CONCAT(SUBSTR(D.mp_return_promise_date,1,4),'-',SUBSTR(D.mp_return_promise_date,5,2),'-',SUBSTR(D.mp_return_promise_date,7,2))
)>=2 AND D.current_return_status NOT IN ('EXPECTED','not_received') AND (D.termainating_date_key IS NULL OR datediff(to_date(FROM_UNIXTIME( UNIX_TIMESTAMP())),
CONCAT(SUBSTR(D.termainating_date_key,1,4),'-',SUBSTR(D.termainating_date_key,5,2),'-',SUBSTR(D.termainating_date_key,7,2))
)=1)) ,
lookup_date(date_sub(FROM_UNIXTIME(UNIX_TIMESTAMP()),1)),
if(D.last_inscan_at_ph_date_key=D.termainating_date_key, D.last_inscan_at_ph_date_key,lookup_date(D.mp_return_promise_date))))) as new_promise_date_old_key,
D.flyer_status,

D.event_reason

from

(Select 
B.returntrackingid,
B.created_time as created_time,
B.first_inscan_at_ph_timestamp as first_inscan_at_ph_timestamp,
B.last_inscan_at_ph_timestamp as last_inscan_at_ph_timestamp,
B.outscanned_from_ph_time as outscanned_from_ph_time,
B.dispatched_to_seller_time as dispatched_to_seller_time,
B.scanned_at_seller_time as scanned_at_seller_time,
B.returned_to_seller_time as returned_to_seller_time,
D.current_source_key as current_source_key,
D.current_destination_key as current_destination_key,
D.sourcetype,
D.destinationtype,
D.last_updated_at as last_updated_at,
D.current_return_status as current_return_status,
lookup_date(cast(B.created_time as timestamp)) as created_date_key,
lookup_time(cast(B.created_time as timestamp)) as created_time_key,
lookup_date(cast(B.first_inscan_at_ph_timestamp as timestamp)) as first_inscan_at_ph_date_key,
lookup_time(cast(B.first_inscan_at_ph_timestamp as timestamp)) as first_inscan_at_ph_time_key,
lookup_date(cast(B.last_inscan_at_ph_timestamp as timestamp)) as last_inscan_at_ph_date_key,
lookup_time(cast(B.last_inscan_at_ph_timestamp as timestamp)) as last_inscan_at_ph_time_key,
lookup_date(cast(B.outscanned_from_ph_time as timestamp)) as outscanned_from_ph_date_key,
lookup_time(cast(B.outscanned_from_ph_time as timestamp)) as outscanned_from_ph_time_key,
lookup_date(cast(B.dispatched_to_seller_time as timestamp)) as dispatched_to_seller_date_key,
lookup_time(cast(B.dispatched_to_seller_time as timestamp)) as dispatched_to_seller_time_key,
lookup_date(cast(B.scanned_at_seller_time as timestamp)) as scanned_at_seller_date_key,
lookup_time(cast(B.scanned_at_seller_time as timestamp)) as scanned_at_seller_time_key,
if(B.first_inscan_at_ph_timestamp IS NOT NULL,
if((from_unixtime(unix_timestamp(B.first_inscan_at_ph_timestamp,'yyyyMMdd'),'u')=6 and hour(B.first_inscan_at_ph_timestamp)>11),
date_add(to_date(B.first_inscan_at_ph_timestamp),2),
if((from_unixtime(unix_timestamp(B.first_inscan_at_ph_timestamp,'yyyyMMdd'),'u')=7 and hour(B.first_inscan_at_ph_timestamp)<11),
date_add(to_date(B.first_inscan_at_ph_timestamp),1),
if(hour(B.first_inscan_at_ph_timestamp)>11,date_add(to_date(B.first_inscan_at_ph_timestamp),1),to_date(B.first_inscan_at_ph_timestamp)))),NULL) as mp_return_promise_date,

B.mp_num_of_return_reattempts_temp,
B.rejected_by__seller_time,
B.undelivered_atempted_time,
B.undelivered_unatempted_time,
B.untraceable_time,
lookup_date(cast(B.returned_to_seller_time as timestamp)) as returned_to_seller_date_key,
lookup_time(cast(B.returned_to_seller_time as timestamp)) as returned_to_seller_time_key,
lookup_date(cast(B.rejected_by__seller_time as timestamp)) as rejected_by__seller_date_key,
lookup_time(cast(B.rejected_by__seller_time as timestamp)) as rejected_by__seller_time_key,
lookup_date(cast(B.undelivered_atempted_time as timestamp)) as undelivered_atempted_date_key,
lookup_time(cast(B.undelivered_atempted_time as timestamp)) as undelivered_atempted_time_key,
lookup_date(cast(B.undelivered_unatempted_time as timestamp)) as undelivered_unatempted_date_key,
lookup_time(cast(B.undelivered_unatempted_time as timestamp)) as undelivered_unatempted_time_key,
lookup_date(cast(B.untraceable_time as timestamp)) as untraceable_date_key,
lookup_time(cast(B.untraceable_time as timestamp)) as untraceable_time_key,
B.termainating_timestamp,
lookup_date(cast(B.termainating_timestamp as timestamp)) as termainating_date_key,
lookup_time(cast(B.termainating_timestamp as timestamp)) as termainating_time_key,
D.return_type,
D.delivery_type,
D.current_location,
D.flyer_id,
D.source_id,
D.flyer_status,

D.event_reason

from
(Select 
A.returntrackingid,
min(A.created_time) as created_time,
min(A.inscan_at_ph_timestamp) as first_inscan_at_ph_timestamp,
max(A.inscan_at_ph_timestamp) as last_inscan_at_ph_timestamp,
max(A.outscanned_from_ph_time) as outscanned_from_ph_time,
min(A.dispatched_to_seller_time) as dispatched_to_seller_time,
min(A.scanned_at_seller_time) as scanned_at_seller_time,
min(A.returned_to_seller_time) as returned_to_seller_time,
sum(if(A.status='return_reattempt',1,0)) as mp_num_of_return_reattempts_temp,
min(A.rejected_by__seller_time) as rejected_by__seller_time ,
min(A.undelivered_atempted_time) as undelivered_atempted_time,
min(A.undelivered_unatempted_time) as undelivered_unatempted_time,
min(A.untraceable_time) as untraceable_time,
min(A.termainating_timestamp) as termainating_timestamp
from
(Select 
fmret.data.sourceid,
fmret.data.returntrackingid,
fmret.data.sourcetype,
fmret.data.destinationtype,
fmret.data.destinationid,
fmret.data.remarks,
fmret.data.updatedat,
fmret.data.trackingid,
fmret.data.status,
if(fmret.data.status='expected',data.updatedat,NULL) as created_time,
if(fmret.data.status='received_at_ph',data.updatedat,NULL) as inscan_at_ph_timestamp,
if(fmret.data.status='outscanned',data.updatedat,NULL) as outscanned_from_ph_time,
if(fmret.data.status='dispatched_to_seller',data.updatedat,NULL) as dispatched_to_seller_time,
if(fmret.data.status='scanned_at_seller',data.updatedat,NULL) as scanned_at_seller_time,
if(fmret.data.status='returned_to_seller',data.updatedat,NULL) as returned_to_seller_time,
if(fmret.data.status='rejected_by_seller',data.updatedat,NULL) as rejected_by__seller_time,
if(fmret.data.status='undelivered_attempted',data.updatedat,NULL) as undelivered_atempted_time,
if(fmret.data.status='undelivered_unattempted',data.updatedat,NULL) as undelivered_unatempted_time,
if(fmret.data.status='untraceable',data.updatedat,NULL) as untraceable_time,
if(fmret.data.status in('untraceable','returned_to_seller','rejected_by_seller'),data.updatedat,NULL) as termainating_timestamp

from bigfoot_journal.dart_wsr_scp_ekl_firstmilereturnrequest_1 fmret) A group by A.returntrackingid)B
left join
( 
 Select
 lookupkey('facility_id', data.sourceid) as current_source_key,
 lookupkey('facility_id', data.destinationid) as current_destination_key,
 data.returntrackingid,
 data.sourcetype,
 data.destinationtype,
 data.updatedat as last_updated_at,
 data.status as current_return_status,
 data.returntype as return_type,
 data.deliveryType as delivery_type,
data.currentfacilityId as Current_location,
data.capturedflyer as flyer_id,
data.sourceid as source_id,
data.flyerstatus as flyer_status,

data.eventreason as event_reason
  
from bigfoot_snapshot.dart_wsr_scp_ekl_firstmilereturnrequest_1_view
)D
on B.returntrackingid=D.returntrackingid) D

left join

(select 
A.`data`.hubId as hh_hub_id,lookup_date(A.`data`.holidayDate) as hh_holiday_date_key,min(case when C.`data`.holidayDate is null then lookup_date(date_add(B.`data`.holidayDate,1)) else null end) as hh_next_working_date_key
from bigfoot_snapshot.dart_wsr_scp_ekl_firstmilehubholidays_1_view_total A 
left join bigfoot_snapshot.dart_wsr_scp_ekl_firstmilehubholidays_1_view_total B on (A.`data`.hubId=B.`data`.hubId  )
left join bigfoot_snapshot.dart_wsr_scp_ekl_firstmilehubholidays_1_view_total C on (B.`data`.hubId=C.`data`.hubId and C.`data`.holidayDate=date_add(B.`data`.holidayDate,1))
where A.`data`.holidayDate<=B.`data`.holidayDate and A.`data`.isholidayflag=TRUE
group by A.`data`.hubId,lookup_date(A.`data`.holidayDate)) HH

on (HH.hh_hub_id=D.source_id and HH.hh_holiday_date_key=lookup_date(D.mp_return_promise_date));
