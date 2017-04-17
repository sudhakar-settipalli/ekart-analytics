
select Z.vendor_tracking_id,Z.Hub_Name, COUNT(Z.vendor_tracking_id),Z.Age

from 
(
select A.vendor_tracking_id,A.tasklist_id,A.tasklist_created_date_key ,B.first_undelivery_status,
B.first_undelivery_status_date_key,A.attempt_no, A.attempt_type,A.undelivered_status,A.shipment_actioned_flag,
A.shipment_delivered_at_date_key,A.shipment_type,A.rto_create_date_key,D.name as Hub_Name,
E.pincode,
(CAST(CAST(A.shipment_delivered_at_date_key as VARCHAR)as DATE)-CAST(CAST(B.first_undelivery_status_date_key as VARCHAR)as DATE)) as Age
from ekl.scp_ekl__runsheet_shipment_fact A
join 
(select vendor_tracking_id,first_undelivery_status,first_undelivery_status_date_key
from ekl.scp_ekl__shipment_90_fact
where first_undelivery_status 
IN ('Undelivered_NonServiceablePincode','Undelivered_OutOfDeliveryArea')
and seller_type IN ('FA','Non-FA','WSR') 
and shipment_created_at_date_key BETWEEN 20170301 AND 20170331)B
on A.vendor_tracking_id=B.vendor_tracking_id and
A.tasklist_created_date_key>=B.first_undelivery_status_date_key
left join (
select vendor_tracking_id,facility_id_key  from ekl.scp_ekl__runsheet_shipment_fact 
where shipment_actioned_flag=1 and seller_type IN ('FA','Non-FA','WSR'))C on A.vendor_tracking_id=C.vendor_tracking_id
left join ekl.scp_ekl__ekl_facility_dim D on C.facility_id_key=D.ekl_facility_dim_key
left join ekl.scp_ekl__logistics_geo_dim E on E.logistics_geo_dim_key=A.customer_pincode_key
where A.attempt_type='Customer'
and A.vendor_tracking_id IN (select vendor_tracking_id from ekl.scp_ekl__runsheet_shipment_fact
where shipment_actioned_flag=1 and seller_type IN ('FA','Non-FA','WSR'))
and E.pincode not in (
'363020''521201',
'421002'
)

order by vendor_tracking_id )Z
GROUP BY Z.vendor_tracking_id,Z.Hub_Name,Z.Age
having COUNT(Z.vendor_tracking_id)>=1




