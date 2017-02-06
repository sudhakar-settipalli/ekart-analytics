INSERT OVERWRITE TABLE runsheet_shipment_level_l0_fact
select distinct runsheet_info.entityid as tasklist_id,
runsheet_info.shipment_id as vendor_tracking_id,
if(shipment_info.shipment_actioned_flag = 'y',1,0) as shipment_actioned_flag,
cast(shipment_info.min_update as timestamp) as shipment_delivered_datetime_from_runsheet,
shipment_fact.ekl_shipment_type as shipment_type,
shipment_fact.shipment_priority_flag as shipment_priority_flag,
shipment_fact.shipment_delivered_at_datetime as shipment_delivered_at_datetime,
shipment_fact.shipment_first_delivery_update_datetime as shipment_first_delivery_update_datetime,
shipment_fact.shipment_last_delivery_update_datetime as shipment_last_delivery_update_datetime,
shipment_fact.payment_type as payment_type,
shipment_fact.payment_mode as payment_mode,
shipment_undelivery_info.undelivered_status as undelivered_status,
shipment_undelivery_info.undelivered_date_time
FROM
(
select entityid,exp,split(exp,'-')[1] as shipment_id
from bigfoot_snapshot.dart_wsr_scp_ekl_lastmiletasklist_1_view
lateral view explode(`data`.task_ids) exploded_table as exp 
where lower(`data`.document_type) = 'runsheet'
) runsheet_info
left outer join 
(
select LMTL_NOT_EXP.entityid AS entityid,
LMT_EXP.exp1 AS LMT_shipment_id,
LMTL_EXP.exp,
LMT_EXP.min_update as min_update,
LMT_EXP.shipment_actioned_flag as shipment_actioned_flag
from 
(
select entityid from bigfoot_snapshot.dart_wsr_scp_ekl_lastmiletasklist_1_view
where `data`.document_type = 'runsheet'
) LMTL_NOT_EXP
LEFT OUTER JOIN ( 
select entityid,exp FROM
(
select entityid,`data`.task_ids as task_ids from bigfoot_snapshot.dart_wsr_scp_ekl_lastmiletasklist_1_view
) d
lateral view explode(task_ids) exploded_table as exp
)  LMTL_EXP
ON LMTL_EXP.entityid = LMTL_NOT_EXP.entityid
LEFT OUTER JOIN (
select 'y' as shipment_actioned_flag,first_id,second_id,exp1,min_update from
(
select `data`.shipment_ids as shipment_ids,
SPLIT(entityid,"-")[1] as first_id,
SPLIT(entityid,"-")[2] as second_id,
min(updatedat) as min_update
from bigfoot_snapshot.dart_wsr_scp_ekl_lastmiletask_1_view
where lower(`data`.status) IN ('delivered','delivery_update')
group by `data`.shipment_ids,SPLIT(entityid,"-")[1],SPLIT(entityid,"-")[2]
) h
lateral view explode(shipment_ids) exploded_table as exp1
) LMT_EXP
ON LMTL_EXP.exp = concat(first_id,'-',second_id)
) shipment_info
ON runsheet_info.exp = shipment_info.exp 
left outer JOIN
bigfoot_external_neo.scp_ekl__shipment_l1_90_fact shipment_fact
ON shipment_fact.vendor_tracking_id = runsheet_info.shipment_id
LEFT OUTER JOIN 
(
  
  select undelivery_intermediate.runsheet_id,undelivery_intermediate.shipment_id,undelivery_intermediate.undelivered_status,undelivery_intermediate.undelivered_date_time from (
select split(entityid,'-')[1] as runsheet_id,split(entityid,'-')[2] as shipment_id,`data`.status as undelivered_status,`data`.in_time as undelivered_date_time,
row_number() over ( partition by entityid order by updatedat desc ) as rank  from bigfoot_snapshot.dart_wsr_scp_ekl_lastmiletask_1_view
      where lower(`data`.status) like ('%undelivered%') and `data`.task_type = 'delivery' and size(split(`data`.status,'-')) <> 1
      ) undelivery_intermediate where undelivery_intermediate.rank = 1
     
) shipment_undelivery_info
ON shipment_undelivery_info.runsheet_id = split(runsheet_info.entityid,'-')[1] and shipment_undelivery_info.shipment_id = runsheet_info.shipment_id ;
INSERT OVERWRITE TABLE runsheet_shipment_level_l0_fact
select distinct runsheet_info.entityid as tasklist_id,
runsheet_info.shipment_id as vendor_tracking_id,
if(shipment_info.shipment_actioned_flag = 'y',1,0) as shipment_actioned_flag,
cast(shipment_info.min_update as timestamp) as shipment_delivered_datetime_from_runsheet,
shipment_fact.ekl_shipment_type as shipment_type,
shipment_fact.shipment_priority_flag as shipment_priority_flag,
shipment_fact.shipment_delivered_at_datetime as shipment_delivered_at_datetime,
shipment_fact.shipment_first_delivery_update_datetime as shipment_first_delivery_update_datetime,
shipment_fact.shipment_last_delivery_update_datetime as shipment_last_delivery_update_datetime,
shipment_fact.payment_type as payment_type,
shipment_fact.payment_mode as payment_mode,
shipment_undelivery_info.undelivered_status as undelivered_status,
shipment_undelivery_info.undelivered_date_time
FROM
(
select entityid,exp,split(exp,'-')[1] as shipment_id,updatedat
from bigfoot_snapshot.dart_wsr_scp_ekl_lastmiletasklist_1_view_total
lateral view explode(`data`.task_ids) exploded_table as exp 
where lower(`data`.document_type) = 'runsheet'
) runsheet_info
left outer join 
(
select LMTL_NOT_EXP.entityid AS entityid,
LMT_EXP.exp1 AS LMT_shipment_id,
LMTL_EXP.exp,
LMT_EXP.min_update as min_update,
LMT_EXP.shipment_actioned_flag as shipment_actioned_flag
from 
(
select entityid from bigfoot_snapshot.dart_wsr_scp_ekl_lastmiletasklist_1_view_total
where `data`.document_type = 'runsheet'
) LMTL_NOT_EXP
LEFT OUTER JOIN ( 
select entityid,exp FROM
(
select entityid,`data`.task_ids as task_ids from bigfoot_snapshot.dart_wsr_scp_ekl_lastmiletasklist_1_view_total
) d
lateral view explode(task_ids) exploded_table as exp
)  LMTL_EXP
ON LMTL_EXP.entityid = LMTL_NOT_EXP.entityid
LEFT OUTER JOIN (
select 'y' as shipment_actioned_flag,first_id,second_id,exp1,min_update from
(
select `data`.shipment_ids as shipment_ids,
SPLIT(entityid,"-")[1] as first_id,
SPLIT(entityid,"-")[2] as second_id,
min(updatedat) as min_update
from bigfoot_snapshot.dart_wsr_scp_ekl_lastmiletask_1_view_total
where lower(`data`.status) IN ('delivered','delivery_update')
group by `data`.shipment_ids,SPLIT(entityid,"-")[1],SPLIT(entityid,"-")[2]
) h
lateral view explode(shipment_ids) exploded_table as exp1
) LMT_EXP
ON LMTL_EXP.exp = concat(first_id,'-',second_id)
) shipment_info
ON runsheet_info.exp = shipment_info.exp 
left outer JOIN
test_bigfoot_external_neo.scp_ekl__shipment_l1_90_fact shipment_fact
ON shipment_fact.vendor_tracking_id = runsheet_info.shipment_id
LEFT OUTER JOIN 
(
  
  select undelivery_intermediate.runsheet_id,undelivery_intermediate.shipment_id,undelivery_intermediate.undelivered_status,undelivery_intermediate.undelivered_date_time from (
select split(entityid,'-')[1] as runsheet_id,split(entityid,'-')[2] as shipment_id,`data`.status as undelivered_status,`data`.in_time as undelivered_date_time,
row_number() over ( partition by entityid order by updatedat desc ) as rank  from bigfoot_snapshot.dart_wsr_scp_ekl_lastmiletask_1_view_total
      where lower(`data`.status) like ('%undelivered%') and `data`.task_type = 'delivery' and size(split(`data`.status,'-')) <> 1
      ) undelivery_intermediate where undelivery_intermediate.rank = 1
     
) shipment_undelivery_info
ON shipment_undelivery_info.runsheet_id = split(runsheet_info.entityid,'-')[1] and shipment_undelivery_info.shipment_id = runsheet_info.shipment_id 
where cast(shipment_info.min_update as timestamp) > '2016-09-31 00:00:00';
