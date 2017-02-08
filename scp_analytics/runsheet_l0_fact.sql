INSERT OVERWRITE TABLE runsheet_l0_fact
SELECT distinct
All_Data.entityid1 AS tasklist_id,
All_Data.document_type AS tasklist_type, 
All_Data.ekl_facility_id,
All_Data.vehicle_id1 AS vehicle_id,
All_Data.agent_id,
All_Data.secondary_agent_ids[0],
All_Data.secondary_agent_ids[1],
All_Data.secondary_agent_ids[2],
All_Data.secondary_agent_ids[3],
All_Data.secondary_agent_ids[4],
CAST(All_Data.closed_date AS TIMESTAMP),
CAST(All_Data.start_time AS TIMESTAMP),
CAST(All_Data.end_time AS TIMESTAMP),
size(All_Data.tot_ship)  AS tasklist_no_of_shipments_attempted,
CAST(All_Data_L.F_T_Act AS TIMESTAMP),
CAST(All_Data_L.L_T_Act AS TIMESTAMP),
All_Data.device_ids[0] AS tasklist_pos_id, 
All_Data.device_ids[1] AS tasklist_smart_device_id,
concat(lookup_date(CAST(All_Data.created_at AS TIMESTAMP)),All_Data.agent_id) as agent_and_date,
concat(lookup_date(CAST(All_Data.created_at AS TIMESTAMP)),All_Data.vehicle_id1) as vehicle_and_date,
All_Data.vehicle_type as vehicle_type,
CAST(All_Data.created_at AS TIMESTAMP)
FROM
(
select LMTL_NOT_EXP.entityid AS entityid1, 
LMTL_NOT_EXP.updatedat AS updatedat ,
LMTL_NOT_EXP.document_id AS document_id ,
LMTL_NOT_EXP.document_type AS document_type,
LMTL_NOT_EXP.vehicle_type AS vehicle_type,
LMTL_NOT_EXP.start_time,
LMTL_NOT_EXP.end_time,
LMTL_NOT_EXP.created_by AS created_by,
LMTL_NOT_EXP.device_ids AS device_ids ,
LMTL_NOT_EXP.status AS status,
LMTL_NOT_EXP.closed_date AS closed_date,
LMTL_NOT_EXP.task_ids AS tot_ship,
LMTL_NOT_EXP.ekl_facility_id AS ekl_facility_id,
LMTL_NOT_EXP.agent_id AS agent_id,
LMTL_NOT_EXP.vehicle_id AS vehicle_id1,
LMTL_NOT_EXP.secondary_agent_ids AS secondary_agent_ids,
LMTL_NOT_EXP.created_at AS created_at 
from 
(
select entityid,
updatedat,`data`.document_id as document_id,
`data`.document_type as document_type,
`data`.vehicle_type as vehicle_type,
`data`.start_time as start_time,
`data`.end_time as end_time,
`data`.created_by as created_by,
`data`.device_ids as device_ids,
`data`.status  as status,
`data`.closed_date as closed_date,
`data`.task_ids as task_ids,
`data`.ekl_facility_id as ekl_facility_id,
`data`.agent_id as agent_id,
`data`.vehicle_id as vehicle_id,
`data`.secondary_agent_ids as secondary_agent_ids,
`data`.created_at as created_at 
from bigfoot_snapshot.dart_wsr_scp_ekl_lastmiletasklist_1_view
where `data`.document_type = 'runsheet'
) LMTL_NOT_EXP
) All_Data  
LEFT OUTER JOIN 
(
select LMTL_NOT_EXP.entityid AS entityid3,MIN(LMT_EXP.out_time) AS F_T_Act ,MAX(LMT_EXP.out_time) AS L_T_Act from 
(select entityid from bigfoot_snapshot.dart_wsr_scp_ekl_lastmiletasklist_1_view
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
select first_id,second_id,exp1,out_time from
(
select `data`.shipment_ids as shipment_ids,
SPLIT(entityid,"-")[1] as first_id,
SPLIT(entityid,"-")[2] as second_id,
`data`.out_time as out_time
from bigfoot_snapshot.dart_wsr_scp_ekl_lastmiletask_1_view
) h
lateral view explode(shipment_ids) exploded_table as exp1
) LMT_EXP
ON LMTL_EXP.exp = concat(first_id,'-',second_id)
group by LMTL_NOT_EXP.entityid
) All_Data_L
ON All_Data.entityid1= All_Data_L.entityid3;
INSERT OVERWRITE TABLE runsheet_l0_fact
SELECT distinct
All_Data.entityid1 AS tasklist_id,
All_Data.document_type AS tasklist_type, 
All_Data.ekl_facility_id,
All_Data.vehicle_id1 AS vehicle_id,
All_Data.agent_id,
All_Data.secondary_agent_ids[0],
All_Data.secondary_agent_ids[1],
All_Data.secondary_agent_ids[2],
All_Data.secondary_agent_ids[3],
All_Data.secondary_agent_ids[4],
CAST(All_Data.closed_date AS TIMESTAMP),
CAST(All_Data.start_time AS TIMESTAMP),
CAST(All_Data.end_time AS TIMESTAMP),
size(All_Data.tot_ship)  AS tasklist_no_of_shipments_attempted,
CAST(All_Data_L.F_T_Act AS TIMESTAMP),
CAST(All_Data_L.L_T_Act AS TIMESTAMP),
All_Data.device_ids[0] AS tasklist_pos_id, 
All_Data.device_ids[1] AS tasklist_smart_device_id,
concat(lookup_date(CAST(All_Data.created_at AS TIMESTAMP)),All_Data.agent_id) as agent_and_date,
concat(lookup_date(CAST(All_Data.created_at AS TIMESTAMP)),All_Data.vehicle_id1) as vehicle_and_date,
All_Data.vehicle_type as vehicle_type,
CAST(All_Data.created_at AS TIMESTAMP)
FROM
(
select LMTL_NOT_EXP.entityid AS entityid1, 
LMTL_NOT_EXP.updatedat AS updatedat ,
LMTL_NOT_EXP.document_id AS document_id ,
LMTL_NOT_EXP.document_type AS document_type,
LMTL_NOT_EXP.vehicle_type AS vehicle_type,
LMTL_NOT_EXP.start_time,
LMTL_NOT_EXP.end_time,
LMTL_NOT_EXP.created_by AS created_by,
LMTL_NOT_EXP.device_ids AS device_ids ,
LMTL_NOT_EXP.status AS status,
LMTL_NOT_EXP.closed_date AS closed_date,
LMTL_NOT_EXP.task_ids AS tot_ship,
LMTL_NOT_EXP.ekl_facility_id AS ekl_facility_id,
LMTL_NOT_EXP.agent_id AS agent_id,
LMTL_NOT_EXP.vehicle_id AS vehicle_id1,
LMTL_NOT_EXP.secondary_agent_ids AS secondary_agent_ids,
LMTL_NOT_EXP.created_at AS created_at 
from 
(
select entityid,
updatedat,`data`.document_id as document_id,
`data`.document_type as document_type,
`data`.vehicle_type as vehicle_type,
`data`.start_time as start_time,
`data`.end_time as end_time,
`data`.created_by as created_by,
`data`.device_ids as device_ids,
`data`.status  as status,
`data`.closed_date as closed_date,
`data`.task_ids as task_ids,
`data`.ekl_facility_id as ekl_facility_id,
`data`.agent_id as agent_id,
`data`.vehicle_id as vehicle_id,
`data`.secondary_agent_ids as secondary_agent_ids,
`data`.created_at as created_at 
from bigfoot_snapshot.dart_wsr_scp_ekl_lastmiletasklist_1_view_total
where `data`.document_type = 'runsheet'
) LMTL_NOT_EXP
) All_Data  
LEFT OUTER JOIN 
(
select LMTL_NOT_EXP.entityid AS entityid3,MIN(LMT_EXP.out_time) AS F_T_Act ,MAX(LMT_EXP.out_time) AS L_T_Act from 
(select entityid from bigfoot_snapshot.dart_wsr_scp_ekl_lastmiletasklist_1_view_total
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
select first_id,second_id,exp1,out_time from
(
select `data`.shipment_ids as shipment_ids,
SPLIT(entityid,"-")[1] as first_id,
SPLIT(entityid,"-")[2] as second_id,
`data`.out_time as out_time
from bigfoot_snapshot.dart_wsr_scp_ekl_lastmiletask_1_view_total
) h
lateral view explode(shipment_ids) exploded_table as exp1
) LMT_EXP
ON LMTL_EXP.exp = concat(first_id,'-',second_id)
group by LMTL_NOT_EXP.entityid
) All_Data_L
ON All_Data.entityid1= All_Data_L.entityid3
where lookup_date(CAST(All_Data.created_at AS TIMESTAMP))>=20161001;
