INSERT OVERWRITE TABLE runsheet_task_mapping_fact
SELECT DISTINCT
	tasklist.vendor_tracking_id as shipment_id,
	tasklist.vehicle_type,
	tasklist.tasklist_smart_device_id as device_id,
	substr(tasklist.tasklist_id, instr(tasklist.tasklist_id,'-')+1) as document_id,
	lookupkey('agent_id',tasklist.primary_agent_id) as agent_id_key,
	tasklist.vehicle_number as vehicle_id,
	tasklist.tasklist_type as document_type,	
	RANK() OVER (PARTITION BY vendor_tracking_id order by lookup_date(coalesce(tasklist.tasklist_end_date_time, tasklist.tasklist_closed_date_time)) DESC, tasklist.tasklist_id DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as tasklistid,	
	count(tasklist.tasklist_id) OVER (PARTITION BY vendor_tracking_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as task_type,
	coalesce(tasklist.tasklist_end_date_time, tasklist.tasklist_closed_date_time) as tasklist_completion_datetime
FROM bigfoot_external_neo.scp_ekl__runsheet_shipment_map_l1_fact tasklist
LEFT JOIN bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim facility 
on tasklist.facility_id = facility.facility_id 
where lower(name) not like '%mphub%';
