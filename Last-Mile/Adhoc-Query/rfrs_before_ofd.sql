set mapred.reduce.tasks = 1;
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/anurag.p/fraude' ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
select vendor_tracking_id from 
(select case when del.rnk<rfrs.rnk then 1 else 0 end as flag,
del.vendor_tracking_id
from 
(select max(rnk) as rnk,vendor_tracking_id,status  from 
(select vendor_tracking_id,
status,
row_number() over(partition by vendor_tracking_id order by updatedat desc) as rnk 
from (select 
`data`.status as status,
`data`.current_address.type as current_address,  
`data`.destination_address.type as destination_address,
`data`.vendor_tracking_id as vendor_tracking_id,
`data`.updated_at as updatedat from dart_wsr_scp_ekl_shipment_4 ship
left outer join bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim facility
on ship.`data`.current_address.id=facility.facility_id
where (lookup_date(`data`.updated_at)  between  20161001 and 20161018) and upper(facility.name) not like '%MPHUB%'
and upper(`data`.current_address.type)='DELIVERY_HUB' and upper(`data`.destination_address.type)='CUSTOMER' ) a 
 --where fl=1
) del group by vendor_tracking_id,status having  lower(del.status)='out_for_delivery')  del 
left outer join 
(select max(rnk) as rnk,vendor_tracking_id,status  from 
(select  vendor_tracking_id, status,
row_number() over(partition by vendor_tracking_id order by updatedat desc) as rnk 
from (select 
--case when array_contains (`data`.notes.flag,'Marked_As_RTO')= TRUE then 0 else 1 end as fl,
`data`.status as status,
`data`.current_address.type as current_address,
`data`.destination_address.type as destination_address,
`data`.vendor_tracking_id as vendor_tracking_id,
`data`.updated_at as updatedat 
from dart_wsr_scp_ekl_shipment_4 ship
left outer join bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim facility
on ship.`data`.current_address.id=facility.facility_id
where (lookup_date(`data`.updated_at)  between  20161001 and 20161018) and upper(facility.name) not like '%MPHUB%'
and upper(`data`.current_address.type)='DELIVERY_HUB' and upper(`data`.destination_address.type)='CUSTOMER'  ) a 
--where fl=1
) rfrs group by vendor_tracking_id,status having lower(rfrs.status)='undelivered_request_for_reschedule' ) rfrs
on rfrs.vendor_tracking_id=del.vendor_tracking_id) a
where flag =1