
insert overwrite table returns_customer_ekl_dependency_l0_hive_fact

Select distinct 
T1.vendor_tracking_id as vendor_tracking_id,
T1.rvp_current_hub_id as dh_hub_id,
T1.shipment_type as ekl_shipment_type,
T1.no_of_attempts_pickup as no_of_attempts_pickup,
T1.first_attempt_pickup_datetime as first_attempt_pickup_datetime,
T1.no_of_attempts_customer_dependency as no_of_attempts_customer_dependency,
T1.first_customer_reattempt_datetime as first_customer_reattempt_datetime,
T1.no_of_attempts_ekl_dependency as no_of_attempts_ekl_dependency,
T1.first_ekl_reattempt_datetime as first_ekl_reattempt_datetime

from
(

select 
A.vendor_tracking_id,
A.rvp_current_hub_id,
A.shipment_type,
count(DISTINCT(if(A.status in ('PICKUP_Out_For_Pickup'),A.updated_at,NULL))) as no_of_attempts_pickup,

min(DISTINCT(if(A.status in ('PICKUP_Out_For_Pickup'),A.updated_at,NULL))) as first_attempt_pickup_datetime,
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
'NotPicked_Attempted_AmountNotReady'),A.updated_at,NULL))) as first_ekl_reattempt_datetime

from
(
Select Distinct 
`data`.vendor_tracking_id as vendor_tracking_id,
`data`.current_address.id as rvp_current_hub_id,
`data`.shipment_type as shipment_type,
`data`.status as status,
`data`.updated_at as updated_at
from bigfoot_journal.dart_wsr_scp_ekl_shipment_4
where UPPER(`data`.current_address.type) = 'DELIVERY_HUB'
) A
group by 
A.vendor_tracking_id,
A.rvp_current_hub_id,
A.shipment_type

) T1;
