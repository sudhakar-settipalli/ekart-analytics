INSERT OVERWRITE TABLE runsheet_shipment_map_l1_fact
SELECT distinct runsheet.tasklist_id,
runsheet.tasklist_type, 
runsheet.facility_id,
runsheet.vehicle_id as vehicle_number,
runsheet.primary_agent_id,
runsheet.secondary_agent_id_1,
runsheet.secondary_agent_id_2,
runsheet.secondary_agent_id_3,
runsheet.secondary_agent_id_4,
runsheet.secondary_agent_id_5,
runsheet.tasklist_closed_date_time,
runsheet.tasklist_start_date_time,
runsheet.tasklist_end_date_time,
runsheet.tasklist_no_of_shipments_attempted,
runsheet.tasklist_first_action_date_time,
runsheet.tasklist_last_action_date_time,
runsheet.tasklist_pos_id, 
runsheet.tasklist_smart_device_id,
runsheet.agent_and_date,
runsheet.vehicle_and_date,
runsheet.vehicle_type,
runsheet.tasklist_created_date_time,
shipment.vendor_tracking_id,
shipment.shipment_actioned_flag,
shipment.shipment_delivered_datetime_from_runsheet,
shipment.shipment_type,
shipment.shipment_priority_flag,
shipment.shipment_delivered_at_datetime,
shipment.shipment_first_delivery_update_datetime,
shipment.shipment_last_delivery_update_datetime,
shipment.payment_type,
shipment.payment_mode,
shipment.undelivered_status,
shipment_90.seller_type,
geo.address_full,
geo.address_hash,
geo.latitude,
geo.longitude,
geo.device_id,
geo.type,
shipment_90.ekl_fin_zone,
shipment_90.shipment_weight,
row_number() OVER(PARTITION BY shipment.vendor_tracking_id ORDER BY runsheet.tasklist_id) AS attempt_no,
geo.city as geo_city,
geo.state as geo_state,
geo.accuracy_level as geo_accuracy_level,
shipment_90.seller_id_key,
shipment_90.destination_pincode_key as customer_pincode_key,
shipment_90.shipment_carrier,
shipment_90.service_tier,
shipment_90.shipment_value,
shipment_90.customer_promise_date_key,
shipment_90.customer_promise_time_key,
shipment_90.logistics_promise_date_key,
shipment_90.logistics_promise_time_key,
shipment_90.new_customer_promise_date_key,
shipment_90.new_customer_promise_time_key,
shipment_90.fsd_first_dh_received_date_key,
shipment_90.fsd_first_dh_received_time_key,
shipment_90.customer_address_id_key,
shipment_90.shipping_category,
shipment_90.billable_weight,
shipment_90.order_item_date_key,
shipment_90.order_item_time_key,
shipment_90.shipment_flash_flag,
dim.analytic_super_category,
call.call_per_attempt,
call.leg1duration,
call.leg2duration,
call.max_leg1_duration,
call.min_leg1_duration,
call.max_leg2_duration,
call.min_leg2_duration,
shipment_90.shipment_item_unit_dispatch_service_tier,
Case when upper(shipment_type)='FORWARD'
then 'Customer'
when upper(shipment_type) in ('RVP','MERCHANT_RETURN')
then 'Seller'
when upper(shipment_type) in ('APPROVED_RTO','UNAPPROVED_RTO')
then 
case when (shipment_90.rto_create_date_key*10000+shipment_90.rto_create_time_key) 
< (lookup_date(runsheet.tasklist_created_date_time)*10000+lookup_time(runsheet.tasklist_created_date_time))
then 'Seller' 
when (shipment_90.rto_create_date_key*10000+shipment_90.rto_create_time_key) 
> (lookup_date(runsheet.tasklist_created_date_time)*10000+lookup_time(runsheet.tasklist_created_date_time))
then 'Customer' 
else 'Seller'
end
else NULL
end as attempt_type,
if (upper(dim2.name) like 'URS_%','URS',(if (upper(dim2.name) like 'VENDOR_%','LMA','EKART'))) as  adm_flag,
shipment_90.core_fsn_flag,
shipment_90.shipment_lzn_classification,
if(device.sync_createdat is not null,device.sync_createdat,device.sync_uploadedat) as device_update_time,
device.deviceid,
Case when shipment_90.shipment_value >= 0 and shipment_90.shipment_value <= 600 then "0_600"
when shipment_90.shipment_value > 600 and shipment_90.shipment_value <= 1000 then "600_1000"
when shipment_90.shipment_value > 1000 and shipment_90.shipment_value <= 2000 then "1000_2000"
when shipment_90.shipment_value > 2000 and shipment_90.shipment_value <= 3000 then "2000_3000"
when shipment_90.shipment_value > 3000 and shipment_90.shipment_value <= 4000 then "3000_4000"
when shipment_90.shipment_value > 4000 and shipment_90.shipment_value <= 5000 then "4000_5000"
when shipment_90.shipment_value > 5000  then ">5000" end as shipment_value_bucket,
Case when lookup_time(runsheet.tasklist_created_date_time) <= 1100 then 1 
when lookup_time(runsheet.tasklist_created_date_time) >1100 and lookup_time(runsheet.tasklist_created_date_time) <=1600 then 2
when lookup_time(runsheet.tasklist_created_date_time) >1600 then 3
end as wave,
shipment_90.primary_product_key,
shipment.undelivered_date_time,
case when upper(dim2.name) like '%_PAKETTS' then 'URS_PAKETTS' when upper(dim2.name) like '%_CI' then 'URS_CONNECT_INDIA' else 'NA' end vendor_type,
case when shipment_90.shipment_weight>0  and shipment_90.shipment_weight<1000 then "0_1000"
when shipment_90.shipment_weight>=1000 and shipment_90.shipment_weight<3000 then "1000_3000"
when shipment_90.shipment_weight>=3000 and shipment_90.shipment_weight<6000 then "3000_6000"
when shipment_90.shipment_weight>=6000 then "6000" end as shipment_weight_bucket,
shipment_90.rto_create_date_key,
shipment_90.rto_create_time_key,
case when upper(dim2.name) like '%_CB' then 1 else 0 end is_clawback,
device.isrepeataddress
from bigfoot_external_neo.scp_ekl__runsheet_l0_fact runsheet
left outer join bigfoot_external_neo.scp_ekl__runsheet_shipment_level_l0_fact shipment
ON runsheet.tasklist_id = shipment.tasklist_id
left outer join bigfoot_external_neo.scp_ekl__shipment_hive_90_fact shipment_90
ON shipment_90.vendor_tracking_id = shipment.vendor_tracking_id
LEFT JOIN  bigfoot_external_neo.scp_ekl__geotag_hive_fact geo
ON geo.vendor_tracking_id = shipment.vendor_tracking_id and upper(geo.tasklist_id)=upper(shipment.tasklist_id)
left outer join bigfoot_external_neo.sp_product__product_categorization_hive_dim dim
on dim.product_categorization_hive_dim_key=shipment_90.primary_product_key
left outer join (select count(concat(interactionid,`timestamp`)) call_per_attempt,sum(leg1duration) as leg1duration,sum(leg2duration) as leg2duration,
shipmentid,runsheet_id,max(leg1duration) as max_leg1_duration,min(leg1duration) as min_leg1_duration,
max(leg2duration) as max_leg2_duration,min(leg2duration) as min_leg2_duration
from bigfoot_external_neo.scp_ekl__callbridging_l0_fact group by
shipmentid,runsheet_id) call
on call.runsheet_id=runsheet.tasklist_id and call.shipmentid=shipment.vendor_tracking_id
left outer join bigfoot_external_neo.scp_ekl__agent_hive_dim dim2
on runsheet.primary_agent_id=dim2.agent_id
left outer join bigfoot_external_neo.scp_ekl__ekartapp_data_l1_fact device
on device.shipment_id=shipment.vendor_tracking_id and device.sheetid=SPLIT(runsheet.tasklist_id,"-")[1]
and device.entitytype='FORWARD_SHIPMENT';
