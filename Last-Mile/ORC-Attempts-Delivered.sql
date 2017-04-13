--Report HubWise ORC-Delivered Total Attempts--
select Z.vendor_tracking_id,Z.HubName,Z.seller_type, count(Z.vendor_tracking_id)-1 as AfterORC_Attempt from 
(
select B.vendor_tracking_id,A.tasklist_id,
CAST(CAST(A.tasklist_created_date_key as VARCHAR)as DATE),E.name as HubName,A.attempt_no,A.attempt_type,
A.shipment_type,A.shipment_actioned_flag,A.undelivered_status,A.seller_type,
CAST(CAST(A.shipment_delivered_at_date_key as VARCHAR)as DATE) AS Delivered_Date
from ekl.scp_ekl__runsheet_shipment_fact A

join 

(select vendor_tracking_id, min(tasklist_created_date_key) as First_ORC_date from ekl.scp_ekl__runsheet_shipment_fact
where undelivered_status like '%Rejected%'   and
vendor_tracking_id in (select vendor_tracking_id from ekl.scp_ekl__runsheet_shipment_fact WHERE
 shipment_actioned_flag=1 and attempt_type='Customer')group by vendor_tracking_id )B 
on A.vendor_tracking_id=B.vendor_tracking_id 
and CAST(CAST(A.tasklist_created_date_key as VARCHAR)as DATE)>=CAST(CAST(B.First_ORC_date as VARCHAR)as DATE)

left join ekl.scp_ekl__shipment_90_fact C on A.vendor_tracking_id=C.vendor_tracking_id

left join 
(select vendor_tracking_id, facility_id_key from ekl.scp_ekl__runsheet_shipment_fact 
where shipment_actioned_flag=1 and 
attempt_type='Customer') D on A.vendor_tracking_id=D.vendor_tracking_id 

left join ekl.scp_ekl__ekl_facility_dim E on D.facility_id_key=E.ekl_facility_dim_key

where UPPER(A.seller_type) IN ('VOONIK','MR VOONIK') and  C.shipment_created_at_date_key BETWEEN 20170101 and 20170228
)Z
group BY
Z.vendor_tracking_id,Z.HubName,Z.seller_type


---------------------------------------------------------------------------------------------------------------



