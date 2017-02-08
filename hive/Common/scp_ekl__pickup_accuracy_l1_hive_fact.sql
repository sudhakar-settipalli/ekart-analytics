insert overwrite table pickup_accuracy_l1_hive_fact

select DISTINCT
T1.vendor_tracking_id as vendor_tracking_id,
T1.rvp_current_hub_id as rvp_current_hub_id_key,
T1.shipment_type as shipment_type,
T1.fkl_current_status as fkl_current_status,
T1.fkl_pending_status as fkl_pending_status,
T1.rvp_pickup_completed_datetime as rvp_pickup_completed_datetime,
T1.rvp_dh_received_datetime as rvp_dh_received_datetime,
T1.rvp_schedule_datetime as rvp_schedule_datetime,
T1.rvp_first_pickup_attempt_datetime as rvp_first_pickup_attempt_datetime,
T1.no_of_attempts as no_of_attempts,
T1.no_of_attempts_customer_dependency as no_of_attempts_customer_dependency,
T1.first_customer_reattempt_datetime as first_customer_reattempt_datetime,
T1.no_of_attempts_ekl_dependency as no_of_attempts_ekl_dependency,
T1.first_ekl_reattempt_datetime as first_ekl_reattempt_datetime,
T1.shipment_current_status as shipment_current_status,
T1.shipment_current_status_datetime as shipment_current_status_datetime,
T1.reverse_shipment_type as reverse_shipment_type,
T1.ekl_fin_zone as ekl_fin_zone,
T1.ekart_lzn_flag as ekart_lzn_flag,
T1.shipment_first_rvp_pickup_datetime as shipment_first_rvp_pickup_datetime,
T1.shipment_last_rvp_pickup_datetime as shipment_last_rvp_pickup_datetime,
T1.rvp_schedule_dateno as rvp_schedule_dateno,
T1.rvp_pickup_completed_dateno as rvp_pickup_completed_dateno,
T1.rvp_first_pickup_attempt_dateno as rvp_first_pickup_attempt_dateno,
T1.today_dateno as today_dateno,
T1.holiday_exception as holiday_exception,
T1.pickup_difference as pickup_difference,
T1.rvp_pending_flag as rvp_pending_flag,
T1.rvp_pickup_pending_days as rvp_pickup_pending_days,
T1.rvp_pickup_complete_days as rvp_pickup_complete_days,
T1.create_to_attempt_days as create_to_attempt_days,
if(T1.no_of_attempts_customer_dependency>0 and 
( T1.first_customer_reattempt_datetime < T1.first_ekl_reattempt_datetime or T1.first_ekl_reattempt_datetime is NULL ),
'Customer', if(T1.no_of_attempts_ekl_dependency>0,'EKL',NULL)) as first_attempt_owner,

if(T1.pickup_difference<=1,'Picked_within_Promise',
if(T1.create_to_attempt_days>1,'Attempt_breach',
if(T1.no_of_attempts_ekl_dependency>0,'Reattempt_EKL',
if(T1.no_of_attempts_customer_dependency>0,'Reattempt_customer','Not_attempt_breach')))) as pickup_breach_bucket,

if( to_date(T1.first_customer_reattempt_datetime) < date_add(to_date(T1.rvp_schedule_datetime),2) 
and T1.first_ekl_reattempt_datetime is NOT Null ,1,0) as ekl_breach,

if( pickup_difference > 1,'breach','met') as p1d_breach_bucket,
T1.is_valid as is_valid,
T1.no_of_attempts_customer_dependency_old as no_of_attempts_customer_dependency_old,
T1.no_of_attempts_ekl_dependency_old as no_of_attempts_ekl_dependency_old


from
(
select
E.vendor_tracking_id as vendor_tracking_id,
E.rvp_current_hub_id as rvp_current_hub_id,
E.shipment_type as shipment_type,
E.fkl_current_status as fkl_current_status,
E.fkl_pending_status as fkl_pending_status,
E.rvp_pickup_completed_datetime as rvp_pickup_completed_datetime,
E.rvp_dh_received_datetime as rvp_dh_received_datetime,
E.rvp_schedule_datetime as rvp_schedule_datetime,
E.rvp_first_pickup_attempt_datetime as rvp_first_pickup_attempt_datetime,
E.no_of_attempts as no_of_attempts,
E.no_of_attempts_customer_dependency as no_of_attempts_customer_dependency,
E.first_customer_reattempt_datetime as first_customer_reattempt_datetime,
E.no_of_attempts_ekl_dependency as no_of_attempts_ekl_dependency,
E.first_ekl_reattempt_datetime as first_ekl_reattempt_datetime,
E.shipment_current_status as shipment_current_status,
E.shipment_current_status_datetime as shipment_current_status_datetime,
E.reverse_shipment_type as reverse_shipment_type,
E.ekl_fin_zone as ekl_fin_zone,
E.ekart_lzn_flag as ekart_lzn_flag,
E.shipment_first_rvp_pickup_datetime as shipment_first_rvp_pickup_datetime,
E.shipment_last_rvp_pickup_datetime as shipment_last_rvp_pickup_datetime,
E.rvp_schedule_dateno as rvp_schedule_dateno,
E.rvp_pickup_completed_dateno as rvp_pickup_completed_dateno,
E.rvp_first_pickup_attempt_dateno as rvp_first_pickup_attempt_dateno,
E.today_dateno as today_dateno,
E.holiday_exception as holiday_exception,
E.is_valid as is_valid,
if(upper(E.reverse_shipment_type) in ('PICKUP_ONLY'),  
if(E.rvp_pickup_completed_datetime is not NULL AND E.rvp_schedule_datetime is not NULL, 
(E.rvp_pickup_completed_dateno - E.rvp_schedule_dateno - E.holiday_exception) , NULL ),NULL) as pickup_difference,

if(upper(E.reverse_shipment_type) in ('PICKUP_ONLY'),  
if(E.fkl_current_status IN ('Not_Received','Lost','Cancelled','PICKUP_Picked_Partial','PICKUP_Cancelled'),E.fkl_current_status,
if(E.fkl_Pending_status IN ('PICKUP_Picked_Complete'),if(E.rvp_dh_received_datetime is NULL,'Expected_at_DH','Received_at_dh'),
'PICKUP_PENDING' )),NULL) as rvp_pending_flag,

if(upper(E.reverse_shipment_type) in ('PICKUP_ONLY'),  
if(E.rvp_schedule_datetime is not NULL, E.today_dateno - E.rvp_schedule_dateno, NULL), NULL) as rvp_pickup_pending_days,

if(upper(E.reverse_shipment_type) in ('PICKUP_ONLY'),  
if(E.rvp_pickup_completed_datetime is not NULL AND E.rvp_schedule_datetime is not NULL,
E.rvp_pickup_completed_dateno - E.rvp_schedule_dateno , NULL),NULL) as rvp_pickup_complete_days,

if(upper(E.reverse_shipment_type) in ('PICKUP_ONLY'),  
if(E.rvp_first_pickup_attempt_datetime is not NULL AND E.rvp_schedule_datetime is not NULL,
E.rvp_first_pickup_attempt_dateno - E.rvp_schedule_dateno - E.holiday_exception, NULL),NULL) as create_to_attempt_days,

E.no_of_attempts_customer_dependency_old,
E.no_of_attempts_ekl_dependency_old

from 
(
select
B.vendor_tracking_id as  vendor_tracking_id,
B.rvp_current_hub_id as rvp_current_hub_id,
B.shipment_type as shipment_type,
B.fkl_current_status as fkl_current_status,
B.fkl_pending_status as fkl_pending_status,
B.is_valid as is_valid,
B.rvp_pickup_completed_datetime as rvp_pickup_completed_datetime,
B.rvp_dh_received_datetime as rvp_dh_received_datetime,
B.rvp_schedule_datetime as rvp_schedule_datetime,
B.rvp_first_pickup_attempt_datetime as rvp_first_pickup_attempt_datetime,
B.no_of_attempts as no_of_attempts,
B.no_of_attempts_customer_dependency as no_of_attempts_customer_dependency,
B.first_customer_reattempt_datetime as first_customer_reattempt_datetime,
B.no_of_attempts_ekl_dependency as no_of_attempts_ekl_dependency,
B.first_ekl_reattempt_datetime as first_ekl_reattempt_datetime,
D.shipment_current_status as shipment_current_status,
D.shipment_current_status_datetime as shipment_current_status_datetime,
D.ekl_shipment_type as ekl_shipment_type,
D.reverse_shipment_type as reverse_shipment_type,
D.ekl_fin_zone as ekl_fin_zone,
D.ekart_lzn_flag as ekart_lzn_flag,
D.shipment_first_rvp_pickup_datetime as shipment_first_rvp_pickup_datetime,
D.shipment_last_rvp_pickup_datetime as shipment_last_rvp_pickup_datetime,
H1.rvp_schedule_dateno as rvp_schedule_dateno,
H2.rvp_pickup_completed_dateno as rvp_pickup_completed_dateno,
H3.rvp_first_pickup_attempt_dateno as rvp_first_pickup_attempt_dateno,
H4.today_dateno as today_dateno,
B.no_of_attempts_customer_dependency_old as no_of_attempts_customer_dependency_old,
B.no_of_attempts_ekl_dependency_old as no_of_attempts_ekl_dependency_old,

count(if(lookup_date(B.rvp_schedule_datetime) <= lookup_date(H5.holiday_date) 
AND lookup_date(B.rvp_pickup_completed_datetime) >=  lookup_date(H5.holiday_date),H5.holiday_date,NULL)) as holiday_exception

from
(
select
A.vendor_tracking_id as vendor_tracking_id,
min(struct(A.updated_at, A.rvp_current_hub_id)).col2 as rvp_current_hub_id,
max(if(A.row_num1 = 1,A.shipment_type, NULL)) as shipment_type,
max(if(A.row_num1 = 1,A.status, NULL)) as fkl_current_status,
max(if(A.status in ('PICKUP_Picked_Complete','PICKUP_Cancelled'), A.status, if(A.row_num1 = 1,A.status,NULL))) as fkl_pending_status,
max(if(A.status in ('PICKUP_Scheduled','PICKUP_Out_For_Pickup','PICKUP_AddedToPickupSheet'),1,0)) as is_valid,
min(if(A.status in ('PICKUP_Picked_Complete'), A.updated_at,NULL)) as rvp_pickup_completed_datetime,
max(if(A.status in ('PICKUP_Picked_Complete'), A.updated_at,NULL)) as rvp_dh_received_datetime,
min(A.updated_at) as rvp_schedule_datetime,
min(DISTINCT(if(A.status in ('PICKUP_Out_For_Pickup'),A.updated_at,NULL))) as rvp_first_pickup_attempt_datetime,
count(DISTINCT(if(A.status in ('PICKUP_Out_For_Pickup'),A.updated_at,NULL))) as no_of_attempts,
count(DISTINCT(if(A.status in (
'Undelivered_Customer_Not_Available', 'Undelivered_Door_Lock', 'Undelivered_COD_Not_Ready', 'Undelivered_Misroute', 'Undelivered_Order_Rejected_By_Customer',
'Undelivered_No_Response', 'Undelivered_Incomplete_Address', 'Undelivered_Invalid_Time_Frame', 'Undelivered_Shipment_On_Hold', 'Undelivered_Address_Not_Found',
'PICKUP_NotPicked_Attempted_CustomerNotAvailable', 'PICKUP_NotPicked_Attempted_DoorLock', 'PICKUP_NotPicked_Attempted_IncompleteAddress',
'PICKUP_NotPicked_Attempted_CSInstructed', 'PICKUP_NotPicked_Attempted_Holiday', 'PICKUP_NotPicked_NotAttempted_CustomerNotAvailable',
'PICKUP_NotPicked_NotAttempted_DoorLock', 'PICKUP_NotPicked_NotAttempted_CustomerRefund', 'PICKUP_NotPicked_NotAttempted_IncompleteAddress',
'PICKUP_NotPicked_NotAttempted_CSInstructed', 'PICKUP_NotPicked_NotAttempted_Holiday', 'PICKUP_Picked_Partial', 'PICKUP_NotPicked_Attempted_AddressChange',
'PICKUP_NotPicked_Attempted_CustomerRefused', 'PICKUP_NotPicked_NotAttempted_AddressChange', 'Undelivered_Order_Rejected_OpenDelivery', 'Received_InHoldShelf',
'Undelivered_Request_For_Reschedule', 'NotPicked_Attempted_CustomerHappyWithProduct', 'NotPicked_Attempted_ReplacementOrRefundRequested',
'NotPicked_Attempted_MissingContents', 'NotPicked_Attempted_CustomerNoResponse', 'NotPicked_Attempted_AddressChangeOrIncomplete',
'NotPicked_Attempted_AlreadyDoneByEKL', 'NotPicked_Attempted_RequestForReschedule', 'Undelivered_OutOfDeliveryArea', 'Undelivered_NonServiceablePincode',
'Undelivered_SameCityMisroute', 'Undelivered_OtherCityMisroute', 'NotPicked_Attempted_ProductMismatch', 'NotPicked_Attempted_ProductDamaged',
'Undelivered_Corresponding_Pickup_Rejected', 'Delivered_To_Locker', 'Undelivered_Potential_Fraud', 'NotPicked_Attempted_OutOfPickupArea',
'NotPicked_Attempted_CustomerNotHappyWithPricing', 'NotPicked_Attempted_RestrictedItemNotPicked', 'NotPicked_Attempted_PackagingNotAvailable',
'NotPicked_Attempted_PickupRejectedByCustomer', 'NotPicked_Attempted_AmountNotReady'),A.updated_at,NULL))) as no_of_attempts_customer_dependency,

min(DISTINCT(if(A.status in (
'Undelivered_Customer_Not_Available', 'Undelivered_Door_Lock', 'Undelivered_COD_Not_Ready', 'Undelivered_Misroute', 'Undelivered_Order_Rejected_By_Customer',
'Undelivered_No_Response', 'Undelivered_Incomplete_Address', 'Undelivered_Invalid_Time_Frame', 'Undelivered_Shipment_On_Hold', 'Undelivered_Address_Not_Found',
'PICKUP_NotPicked_Attempted_CustomerNotAvailable', 'PICKUP_NotPicked_Attempted_DoorLock', 'PICKUP_NotPicked_Attempted_IncompleteAddress',
'PICKUP_NotPicked_Attempted_CSInstructed', 'PICKUP_NotPicked_Attempted_Holiday', 'PICKUP_NotPicked_NotAttempted_CustomerNotAvailable',
'PICKUP_NotPicked_NotAttempted_DoorLock', 'PICKUP_NotPicked_NotAttempted_CustomerRefund', 'PICKUP_NotPicked_NotAttempted_IncompleteAddress',
'PICKUP_NotPicked_NotAttempted_CSInstructed', 'PICKUP_NotPicked_NotAttempted_Holiday', 'PICKUP_Picked_Partial', 'PICKUP_NotPicked_Attempted_AddressChange',
'PICKUP_NotPicked_Attempted_CustomerRefused', 'PICKUP_NotPicked_NotAttempted_AddressChange', 'Undelivered_Order_Rejected_OpenDelivery', 'Received_InHoldShelf',
'Undelivered_Request_For_Reschedule', 'NotPicked_Attempted_CustomerHappyWithProduct', 'NotPicked_Attempted_ReplacementOrRefundRequested',
'NotPicked_Attempted_MissingContents', 'NotPicked_Attempted_CustomerNoResponse', 'NotPicked_Attempted_AddressChangeOrIncomplete',
'NotPicked_Attempted_AlreadyDoneByEKL', 'NotPicked_Attempted_RequestForReschedule', 'Undelivered_OutOfDeliveryArea', 'Undelivered_NonServiceablePincode',
'Undelivered_SameCityMisroute', 'Undelivered_OtherCityMisroute', 'NotPicked_Attempted_ProductMismatch', 'NotPicked_Attempted_ProductDamaged',
'Undelivered_Corresponding_Pickup_Rejected', 'Delivered_To_Locker', 'Undelivered_Potential_Fraud', 'NotPicked_Attempted_OutOfPickupArea',
'NotPicked_Attempted_CustomerNotHappyWithPricing', 'NotPicked_Attempted_RestrictedItemNotPicked', 'NotPicked_Attempted_PackagingNotAvailable',
'NotPicked_Attempted_PickupRejectedByCustomer', 'NotPicked_Attempted_AmountNotReady'),A.updated_at,NULL))) as first_customer_reattempt_datetime,

count(DISTINCT(if(A.status in (
'Undelivered_Not_Attempted', 'Undelivered_Shipment_Damage', 'Undelivered_Not_Attended', 'Error', 'Lost', 'Undelivered_Attempted', 'Undelivered_Heavy_Traffic',
'Undelivered_Vehicle_Breakdown', 'Undelivered_Security_Instability', 'PICKUP_NotPicked_Attempted', 'PICKUP_NotPicked_Attempted_ShipmentDamage',
'PICKUP_NotPicked_NotAttempted', 'PICKUP_NotPicked_NotAttempted_ShipmentDamage', 'PICKUP_NotPicked_Attempted_HeavyTraffic',
'PICKUP_NotPicked_Attempted_VehicleBreakDown', 'PICKUP_NotPicked_NotAttempted_VehicleBreakDown', 'Untraceable', 'PICKUP_NotPicked_NotAttempted_BadWeather',
'Undelivered_Heavy_Rain', 'Damaged', 'Undelivered_For_Consolidation', 'Undelivered_HeavyLoad', 'NotPicked_NotAttempted_BreakDownOrAccident',
'NotPicked_NotAttempted_RoadBlockOrStrike', 'NotPicked_NotAttempted_HeavyRain', 'NotPicked_NotAttempted_HeavyLoad', 'Undelivered_UntraceableFromHub',
'Untraceable_BRSNR', 'Undelivered_Locker_Issue', 'NotPicked_Attempted_CustomerNotHappyWithPackaging', 'NotPicked_Attempted_MissingContents',
'NotPicked_Attempted_CustomerNoResponse', 'NotPicked_Attempted_AddressChangeOrIncomplete', 'NotPicked_Attempted_AlreadyDoneByEKL',
'NotPicked_Attempted_RequestForReschedule', 'Undelivered_OutOfDeliveryArea', 'Undelivered_NonServiceablePincode', 'Undelivered_SameCityMisroute',
'Undelivered_OtherCityMisroute', 'NotPicked_Attempted_ProductMismatch', 'NotPicked_Attempted_ProductDamaged', 'Undelivered_Corresponding_Pickup_Rejected',
'Delivered_To_Locker', 'Undelivered_Potential_Fraud', 'NotPicked_Attempted_OutOfPickupArea', 'NotPicked_Attempted_CustomerNotHappyWithPricing',
'NotPicked_Attempted_RestrictedItemNotPicked', 'NotPicked_Attempted_PackagingNotAvailable', 'NotPicked_Attempted_PickupRejectedByCustomer',
'NotPicked_Attempted_AmountNotReady'),A.updated_at,NULL))) as no_of_attempts_ekl_dependency,

min(DISTINCT(if(A.status in (
'Undelivered_Not_Attempted', 'Undelivered_Shipment_Damage', 'Undelivered_Not_Attended', 'Error', 'Lost', 'Undelivered_Attempted', 'Undelivered_Heavy_Traffic',
'Undelivered_Vehicle_Breakdown', 'Undelivered_Security_Instability', 'PICKUP_NotPicked_Attempted', 'PICKUP_NotPicked_Attempted_ShipmentDamage',
'PICKUP_NotPicked_NotAttempted', 'PICKUP_NotPicked_NotAttempted_ShipmentDamage', 'PICKUP_NotPicked_Attempted_HeavyTraffic',
'PICKUP_NotPicked_Attempted_VehicleBreakDown', 'PICKUP_NotPicked_NotAttempted_VehicleBreakDown', 'Untraceable', 'PICKUP_NotPicked_NotAttempted_BadWeather',
'Undelivered_Heavy_Rain', 'Damaged', 'Undelivered_For_Consolidation', 'Undelivered_HeavyLoad', 'NotPicked_NotAttempted_BreakDownOrAccident',
'NotPicked_NotAttempted_RoadBlockOrStrike', 'NotPicked_NotAttempted_HeavyRain', 'NotPicked_NotAttempted_HeavyLoad', 'Undelivered_UntraceableFromHub',
'Untraceable_BRSNR', 'Undelivered_Locker_Issue', 'NotPicked_Attempted_CustomerNotHappyWithPackaging', 'NotPicked_Attempted_MissingContents',
'NotPicked_Attempted_CustomerNoResponse', 'NotPicked_Attempted_AddressChangeOrIncomplete', 'NotPicked_Attempted_AlreadyDoneByEKL',
'NotPicked_Attempted_RequestForReschedule', 'Undelivered_OutOfDeliveryArea', 'Undelivered_NonServiceablePincode', 'Undelivered_SameCityMisroute',
'Undelivered_OtherCityMisroute', 'NotPicked_Attempted_ProductMismatch', 'NotPicked_Attempted_ProductDamaged', 'Undelivered_Corresponding_Pickup_Rejected',
'Delivered_To_Locker', 'Undelivered_Potential_Fraud', 'NotPicked_Attempted_OutOfPickupArea', 'NotPicked_Attempted_CustomerNotHappyWithPricing',
'NotPicked_Attempted_RestrictedItemNotPicked', 'NotPicked_Attempted_PackagingNotAvailable', 'NotPicked_Attempted_PickupRejectedByCustomer',
'NotPicked_Attempted_AmountNotReady'),A.updated_at,NULL))) as first_ekl_reattempt_datetime,

count(DISTINCT(if(A.status in ('NotPicked_NotAttempted_BreakDownOrAccident', 'NotPicked_NotAttempted_RoadBlockOrStrike',
'NotPicked_NotAttempted_HeavyRain'),A.updated_at,NULL))) as no_of_attempts_ekl_dependency_old,

count(DISTINCT(if(A.status in (
'NotPicked_Attempted_CustomerHappyWithProduct','NotPicked_Attempted_ReplacementOrRefundRequested','NotPicked_Attempted_MissingContents',
'NotPicked_Attempted_CustomerNoResponse','NotPicked_Attempted_AddressChangeOrIncomplete','NotPicked_Attempted_AlreadyDoneByEKL',
'NotPicked_Attempted_RequestForReschedule'),A.updated_at,NULL))) as no_of_attempts_customer_dependency_old

from
(

Select Distinct 
`data`.vendor_tracking_id as vendor_tracking_id,
`data`.current_address.id as rvp_current_hub_id,
`data`.shipment_type as shipment_type,
`data`.status as status,
`data`.updated_at as updated_at,
ROW_NUMBER() OVER (PARTITION BY `data`.vendor_tracking_id ,`data`.current_address.id ORDER BY `data`.updated_at DESC) as row_num1
from bigfoot_journal.dart_wsr_scp_ekl_shipment_4
where UPPER(`data`.current_address.type) = 'DELIVERY_HUB' 
and  day  > date_format(date_sub(current_date,120),'yyyyMMdd')
and `data`.shipment_type IN ('rvp') 
and lower(`data`.status) NOT IN ('reshipped')
) A

Group by 
A.vendor_tracking_id

) B

left outer join
(
select 
C.vendor_tracking_id as vendor_tracking_id,
C.shipment_current_status as shipment_current_status,
C.shipment_current_status_datetime as shipment_current_status_datetime,
C.ekl_shipment_type as ekl_shipment_type,
C.reverse_shipment_type as reverse_shipment_type,
C.ekl_fin_zone as ekl_fin_zone,
C.ekart_lzn_flag as ekart_lzn_flag,
C.shipment_first_rvp_pickup_time as shipment_first_rvp_pickup_datetime,
C.shipment_last_rvp_pickup_time as shipment_last_rvp_pickup_datetime
from 
bigfoot_external_neo.scp_ekl__shipment_l0_90_fact C
where C.ekl_shipment_type in ('rvp')) D
on D.vendor_tracking_id = B.vendor_tracking_id

left outer join
( select `date` as work_date1, dt.dateno as rvp_schedule_dateno 
from bigfoot_common.ekl_workday_map dt
) H1 on to_date(H1.work_date1) = to_date(B.rvp_schedule_datetime)

left outer join
( select `date` as work_date2, dt.dateno as rvp_pickup_completed_dateno
from bigfoot_common.ekl_workday_map dt
) H2 on to_date(H2.work_date2) = to_date(B.rvp_pickup_completed_datetime)

left outer join
( select `date` as work_date3, dt.dateno as rvp_first_pickup_attempt_dateno
from bigfoot_common.ekl_workday_map dt
) H3 on to_date(H3.work_date3) = to_date(B.rvp_first_pickup_attempt_datetime)

left outer join
( select `date` as work_date4, dt.dateno as today_dateno
from bigfoot_common.ekl_workday_map dt
) H4 on to_date(H4.work_date4) = to_date(FROM_UNIXTIME( UNIX_TIMESTAMP() ))

left outer join
( select HD1.hub_id,
from_unixtime(unix_timestamp(HD1.holidaydate ,'MM/dd/yyyy'), 'yyyy-MM-dd') as holiday_date
from bigfoot_common.hub_holiday_mapping HD1
) H5 on H5.hub_id = B.rvp_current_hub_id 

Group by


B.vendor_tracking_id,
B.rvp_current_hub_id,
B.shipment_type,
B.fkl_current_status,
B.fkl_pending_status,
B.is_valid,
B.rvp_pickup_completed_datetime,
B.rvp_dh_received_datetime,
B.rvp_schedule_datetime,
B.rvp_first_pickup_attempt_datetime,
B.no_of_attempts,
B.no_of_attempts_customer_dependency,
B.first_customer_reattempt_datetime,
B.no_of_attempts_ekl_dependency,
B.first_ekl_reattempt_datetime,
D.shipment_current_status,
D.shipment_current_status_datetime,
D.ekl_shipment_type,
D.reverse_shipment_type,
D.ekl_fin_zone,
D.ekart_lzn_flag,
D.shipment_first_rvp_pickup_datetime,
D.shipment_last_rvp_pickup_datetime,
H1.rvp_schedule_dateno,
H2.rvp_pickup_completed_dateno,
H3.rvp_first_pickup_attempt_dateno,
H4.today_dateno,
B.no_of_attempts_customer_dependency_old,
B.no_of_attempts_ekl_dependency_old

)E
)T1

where T1.is_valid = 1
