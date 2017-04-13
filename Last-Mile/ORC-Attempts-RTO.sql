--Report HubWise ORC-RTO Total Attempts--

select Z.vendor_tracking_id,Z.HubName,Z.seller_type, count(Z.vendor_tracking_id) as Attempt from 
(
select B.vendor_tracking_id,A.attempt_no,A.tasklist_id, A.seller_type,
CAST(CAST(A.tasklist_created_date_key as VARCHAR)as DATE) as tasklist_created_date_key,
E.name as HubName,A.attempt_type,
A.shipment_type,A.shipment_actioned_flag,A.undelivered_status,
CAST(CAST(A.rto_create_date_key as VARCHAR)as DATE) AS RTO_Create_Date
from 

ekl.scp_ekl__runsheet_shipment_fact A

left join ekl.scp_ekl__shipment_90_fact C on A.vendor_tracking_id=C.vendor_tracking_id

join 
(select vendor_tracking_id, min(tasklist_created_date_key) as First_ORC_date from ekl.scp_ekl__runsheet_shipment_fact
where
undelivered_status like '%Rejected%' and attempt_type='Customer' and
shipment_type in ('approved_rto','unapproved_rto') and UPPER(seller_type) IN ('VOONIK','MR VOONIK')
group by vendor_tracking_id
 )B 
on A.vendor_tracking_id=B.vendor_tracking_id 
and CAST(CAST(A.tasklist_created_date_key as VARCHAR)as DATE)>=CAST(CAST(B.First_ORC_date as VARCHAR)as DATE)

left join 
(select vendor_tracking_id, max(attempt_no) as m from ekl.scp_ekl__runsheet_shipment_fact 
where attempt_type='Customer' 
and shipment_type in ('approved_rto','unapproved_rto') and UPPER(seller_type) IN ('VOONIK','MR VOONIK')
group by vendor_tracking_id
)F on B.vendor_tracking_id=F.vendor_tracking_id

left join 
(select vendor_tracking_id,attempt_no, facility_id_key from ekl.scp_ekl__runsheet_shipment_fact 
where attempt_type='Customer' and
UPPER(seller_type) IN ('VOONIK','MR VOONIK')
) D
on F.vendor_tracking_id=D.vendor_tracking_id and F.m=D.attempt_no


--left join 
--(select vendor_tracking_id, facility_id_key from ekl.scp_ekl__runsheet_shipment_fact 
--where attempt_type='Customer' and
--seller_type='JBN'
--) G on G.vendor_tracking_id=D.vendor_tracking_id





left join ekl.scp_ekl__ekl_facility_dim E on D.facility_id_key=E.ekl_facility_dim_key
where UPPER(A.seller_type) IN ('VOONIK','MR VOONIK') 
and C.shipment_created_at_date_key BETWEEN 20170101 and 20170228
and A.attempt_type='Customer'

order by B.vendor_tracking_id,A.tasklist_created_date_key


)Z
group BY
Z.vendor_tracking_id,Z.HubName,Z.seller_type

