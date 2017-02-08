INSERT OVERWRITE TABLE flash_last_mile_breach_90_fact
select distinct a.vendor_tracking_id,
if(b.updated_at is null,0,if(lookup_date(b.updated_at)=pickup_slot_start_date_key and lookup_time(b.updated_at) between pickup_slot_start_time_key and pickup_slot_end_time_key, 0,if(a.shipment_first_consignment_create_date_key=lookup_date(b.updated_at) and a.shipment_first_consignment_create_time_key<=2359,0,1))) breach_flag,
if(b.updated_at is null,'NOT_A_LASTMILE_BREACH',if(lookup_date(b.updated_at)=pickup_slot_start_date_key and lookup_time(b.updated_at) between pickup_slot_start_time_key and pickup_slot_end_time_key,'NOT_A_LASTMILE_BREACH',if(a.shipment_first_consignment_create_date_key=lookup_date(b.updated_at) and a.shipment_first_consignment_create_time_key<=2359,'NOT_A_LASTMILE_BREACH','DH_BREACH'))) breach_bucket
from bigfoot_external_neo.scp_ekl__shipment_hive_90_fact a
left outer join (select `data`.updated_at as updated_at , `data`.vendor_tracking_id as vendor_tracking_id from
bigfoot_journal.dart_wsr_scp_ekl_shipment_4 where  day > date_format(date_sub(current_date,100),'yyyyMMdd') 
and upper(`data`.status)="EKART_COURIER_PICKUP_UPDATE" and upper(concat_ws("-",`data`.attributes)) like '%FLASH%') b
on a.vendor_tracking_id=b.vendor_tracking_id
where a.shipment_flash_flag=TRUE and a.ekl_shipment_type IN ('unapproved_rto','approved_rto','forward') and a.shipment_carrier IN ('FSD');
