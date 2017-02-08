
insert overwrite table fc_packing_box_l0_hive_fact 
select
final_table.packing_box_company as packing_box_company,
final_table.packing_box_id as packing_box_id,
final_table.packing_box_name as packing_box_name,
final_table.packing_box_is_valid as packing_box_is_valid,
final_table.packing_box_is_active as packing_box_is_active,
final_table.packing_box_is_available_for_packing as packing_box_is_available_for_packing,
final_table.packing_box_is_deleted as packing_box_is_deleted,
final_table.packing_box_bucket as packing_box_bucket,
final_table.packing_box_length as packing_box_length,
final_table.packing_box_breadth as packing_box_breadth,
final_table.packing_box_height as packing_box_height,
final_table.packing_box_volume as packing_box_volume,
final_table.packing_box_volume_order as packing_box_volume_order,
final_table.packing_box_bucket_volume_order as packing_box_bucket_volume_order,
final_table.packing_box_warehouse_id as packing_box_warehouse_id,
final_table.packing_box_warehouse_dim_key as packing_box_warehouse_dim_key,
final_table.packing_box_created_at as packing_box_created_at,
final_table.packing_box_created_date_key as packing_box_created_date_key,
final_table.packing_box_updated_at as packing_box_updated_at,
final_table.packing_box_updated_date_key as packing_box_updated_date_key,
final_table.packing_box_category as packing_box_category,
final_table.internal_name as packing_box_internal_name
from
(
select 'fki' as packing_box_company,
 box.entityid as packing_box_id, 
 box.`data`.display_name as packing_box_name, 
if(box.`data`.display_name not in 
('Box','BX','DBX','DP','KB','LS','LSX','OC','OCX','SB','SBX','V','W','X','Y','Z','LCL','GFT','BNS'),1,0) 
as packing_box_is_valid,if(box.`data`.is_active = TRUE,1,0) as packing_box_is_active, 
if(box.`data`.is_available_for_packing = TRUE,1,0) as packing_box_is_available_for_packing,
 if(box.`data`.is_deleted = TRUE,1,0) as packing_box_is_deleted, box.`data`.packing_bucket as packing_box_bucket,
 box.`data`.length as packing_box_length, box.`data`.breadth as packing_box_breadth, box.`data`.height as packing_box_height,
 box.`data`.length*`data`.breadth*`data`.height as packing_box_volume, box.`data`.warehouse_id as packing_box_warehouse_id, 
 lookupkey('warehouse_id',box.`data`.warehouse_id) as packing_box_warehouse_dim_key, 
 from_unixtime(unix_timestamp(box.`data`.created_at)) as packing_box_created_at, 
 lookup_date(box.`data`.created_at) as packing_box_created_date_key, from_unixtime(unix_timestamp(box.`data`.updated_at)) as packing_box_updated_at,
 lookup_date(box.`data`.updated_at) as packing_box_updated_date_key, box_rank.rank as packing_box_volume_order,
 box_bucket_rank.rank as packing_box_bucket_volume_order, if(box.`data`.packing_bucket like '%ag','Not carton','Carton') as packing_box_category,
 box.data.name as internal_name  from bigfoot_snapshot.dart_fki_scp_warehouse_packing_box_2_view_total box left outer join 

 (
 select max_vol.display_name as display_name,
 rank() over (ORDER BY max_vol.max_volume) as rank from 
 (
 select 
`data`.display_name as display_name,
 max(`data`.length*`data`.breadth*`data`.height) as max_volume
 from
  bigfoot_snapshot.dart_fki_scp_warehouse_packing_box_2_view_total
  group by `data`.display_name 
 ) max_vol
 ) box_rank 
 on box.`data`.display_name = box_rank.display_name left outer join 
 
(
select  max_vol.display_name as display_name, 
 rank() over (partition by max_vol.max_bucket ORDER BY max_vol.max_bucket, max_vol.max_volume) as rank 
 from 
 (
  select 
`data`.display_name as display_name,
 max(`data`.length*`data`.breadth*`data`.height) as max_volume,
 max(`data`.packing_bucket) as max_bucket
 from
  bigfoot_snapshot.dart_fki_scp_warehouse_packing_box_2_view_total
  group by `data`.display_name 
  ) max_vol
 ) box_bucket_rank 
 on box.`data`.display_name = box_bucket_rank.display_name


union all  

select 'wsr' as packing_box_company, box.entityid as packing_box_id, box.`data`.display_name as packing_box_name,
if(box.`data`.display_name not in 
('Box','BX','DBX','DP','KB','LS','LSX','OC','OCX','SB','SBX','V','W','X','Y','Z','LCL','GFT','BNS'),1,0) 
as packing_box_is_valid,if(box.`data`.is_active = TRUE,1,0) as packing_box_is_active, 
if(box.`data`.is_available_for_packing = TRUE,1,0) as packing_box_is_available_for_packing, 
if(box.`data`.is_deleted = TRUE,1,0) as packing_box_is_deleted, box.`data`.packing_bucket as packing_box_bucket,
 box.`data`.length as packing_box_length, box.`data`.breadth as packing_box_breadth, box.`data`.height as packing_box_height,
 box.`data`.length*`data`.breadth*`data`.height as packing_box_volume, box.`data`.warehouse_id as packing_box_warehouse_id, 
 lookupkey('warehouse_id',box.`data`.warehouse_id) as packing_box_warehouse_dim_key, 
 from_unixtime(unix_timestamp(box.`data`.created_at)) as packing_box_created_at, 
 lookup_date(box.`data`.created_at) as packing_box_created_date_key, 
 from_unixtime(unix_timestamp(box.`data`.updated_at)) as packing_box_updated_at, 
 lookup_date(box.`data`.updated_at) as packing_box_updated_date_key, 
 box_rank.rank as packing_box_volume_order, box_bucket_rank.rank as packing_box_bucket_volume_order, 
 if(box.`data`.packing_bucket like '%ag','Not carton','Carton') as packing_box_category, box.data.name as internal_name
 from bigfoot_snapshot.dart_wsr_scp_warehouse_packing_box_3_view_total box left outer join

  (
 select max_vol.display_name as display_name,
 rank() over (ORDER BY max_vol.max_volume) as rank from 
 (
 select 
`data`.display_name as display_name,
 max(`data`.length*`data`.breadth*`data`.height) as max_volume
 from
  bigfoot_snapshot.dart_wsr_scp_warehouse_packing_box_3_view_total
  group by `data`.display_name 
 ) max_vol
 ) box_rank 
 on box.`data`.display_name = box_rank.display_name left outer join 
 
(
select  max_vol.display_name as display_name, 
 rank() over (partition by max_vol.max_bucket ORDER BY max_vol.max_bucket, max_vol.max_volume) as rank 
 from 
 (
  select 
`data`.display_name as display_name,
 max(`data`.length*`data`.breadth*`data`.height) as max_volume,
 max(`data`.packing_bucket) as max_bucket
 from
  bigfoot_snapshot.dart_wsr_scp_warehouse_packing_box_3_view_total
  group by `data`.display_name 
  ) max_vol
 ) box_bucket_rank 
 on box.`data`.display_name = box_bucket_rank.display_name
 ) final_table
 ;

