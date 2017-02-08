INSERT OVERWRITE TABLE consignment_l1_90_fact
-- changed the actial eta to consignment receive time for calculating consignment eta breach line#17consignment_breach_flag
-- changed the eta breach definition to eta < consignment receive time line#18consignment_breach_flag
-- Added ekl_zone for movement flag
-- changed conecction_1_11 to connection_1
-- Changed eta datetime from max to min
SELECT CAST(SPLIT(Cons.entityid, "-")[1] AS INT) AS consignment_id,
       Cons.`data`.vendor_tracking_id AS consignment_awb_number,
       Cons.`data`.connection_id AS consignment_connection_id,
       IF(size(Cons.`data`.shipment_groups)>0,'BAG','SHIPMENT') AS consignment_type,
       upper(Cons.`data`.connection_mode) AS consignment_mode,
       Conn.`data`.type AS connection_type,
       Conn.`data`.transit_type AS connection_transit_type,
       Conn.`data`.code AS connection_code,
       Conn.`data`.frequency_type AS connection_frequency_type,
       Conn.`data`.vehicle_no AS connection_vehicle_no,
       Cons.`data`.status AS consignment_status,
       Cons.`data`.source_location.type AS consignment_source_hub_type,
       Cons.`data`.destination_location.type AS consignment_destination_hub_type,
       Conn.`data`.coloader AS consignment_co_loader,
	   if(consign.consignment_received_datetime is not null,if(unix_timestamp(consign.consignment_received_datetime)> unix_timestamp(consign.eta),1,0),If(unix_timestamp()>(unix_timestamp(consign.eta)),1,0)) as consignment_breach_flag,
	   
	   IF((upper(src_hub.lt) = upper(dest_hub.lt) OR upper(src_hub.city)=upper(dest_hub.city)), "INTRACITY"
			,IF(upper(src_hub.geo_zone) = upper(dest_hub.geo_zone), "INTRAZONE"
				, IF(upper(src_hub.geo_zone) <> upper(dest_hub.geo_zone), "INTERZONE", "Missing")
				)
		) as consignment_movement_flag,
       size(Cons.`data`.shipment_groups) AS consignment_no_of_bags,
       size(Cons.`data`.shipments) AS consignment_no_of_shipments,
       CAST(IF(Cons.`data`.connection_id=0,0,1) AS INT) AS tms_compliant_consignments,
       CAST(IF(Cons.`data`.connection_id=0,0,size(Cons.`data`.shipment_groups)) AS INT) AS tms_compliant_bags,
       CAST(IF(Cons.`data`.connection_id=0,0,size(Cons.`data`.shipments)) AS INT) AS tms_compliant_shipments,
       Cons.`data`.source_location.id AS consignment_source_hub_id,
       Cons.`data`.destination_location.id AS consignment_destination_hub_id,
       0.0 AS consignment_lane_cost_per_kg,
       0.0 AS consignment_n_form_charges,
       Cons.`data`.system_weight.physical AS consignment_system_weight,
       Cons.`data`.sender_weight.physical AS consignment_sender_measured_weight,
       Cons.`data`.receiver_weight.physical AS consignment_receiver_measured_weight,
       CAST(IF(Cons.`data`.system_weight.physical > Cons.`data`.system_weight.volumetric, Cons.`data`.system_weight.physical, Cons.`data`.system_weight.volumetric) AS DOUBLE) AS consignment_chargeable_weight,
       Cons.`data`.created_at AS consignment_create_datetime,
       consign.consignment_received_datetime as consignment_received_datetime,
       CAST((Cons.updatedat/1000) AS TIMESTAMP) AS consignment_status_date_time,
       CAST(If((Hour(Cons.`data`.created_at)*60*60+Minute(Cons.`data`.created_at)*60+Second(Cons.`data`.created_at))<Conn.`data`.cutoff-60*60,
              (Conn.`data`.cutoff+round(unix_timestamp(Cons.`data`.created_at)/86400)*86400-19800-86400),(Conn.`data`.cutoff+round(unix_timestamp(Cons.`data`.created_at)/86400)*86400-19800)) AS TIMESTAMP) AS connection_cut_off_datetime,
       (Unix_timestamp(Cons.`data`.created_at)-If((Hour(Cons.`data`.created_at)*60*60+Minute(Cons.`data`.created_at)*60+Second(Cons.`data`.created_at))<Conn.`data`.cutoff-60*60,
              (Conn.`data`.cutoff+round(unix_timestamp(Cons.`data`.created_at)/86400)*86400-(19800+86400)),(Conn.`data`.cutoff+round(unix_timestamp(Cons.`data`.created_at)/86400)*86400-19800)))/3600 AS consignment_creation_delay_in_hours,
       CAST((unix_timestamp() - unix_timestamp(Cons.`data`.created_at))/(3600) AS INT) AS consignment_total_age_in_hours,
       COALESCE (cast(consign.eta as string),from_unixtime(unix_timestamp(Cons.`data`.created_at)+Cons.`data`.connection_estimated_tat)) AS consignment_eta_datetime,
       (Cons.`data`.connection_estimated_tat-If(Cons.`data`.connection_actual_tat IS NULL OR Cons.`data`.connection_actual_tat = 0,
              Hour(from_unixtime(unix_timestamp()))*60*60+Minute(from_unixtime(unix_timestamp()))*60+Second(from_unixtime(unix_timestamp())),
              Cons.`data`.connection_actual_tat))/3600 AS consignment_receive_delay_in_hours,
       CAST(IF(Cons.`data`.connection_id=0,size(Cons.`data`.shipment_groups),0) AS INT) AS tms_non_compliant_bags,
       CAST(IF(Cons.`data`.connection_id=0,size(Cons.`data`.shipments),0) AS INT) AS tms_non_compliant_shipments,bagcons.`data`.bag_based_reason as bag_based_consignment_reason,
	   consign.eta as eta
FROM bigfoot_snapshot.dart_wsr_scp_ekl_shipmentgroup_3_view Cons
inner join 
(select 
    CAST(SPLIT(entityid, "-")[1] AS INT) AS consignment_id,
	min(struct(updatedat, data.eta)).col2 as eta,
    min(IF(`data`.status='Received', CAST((updatedat/1000) AS TIMESTAMP), NULL)) as consignment_received_datetime
    from bigfoot_journal.dart_wsr_scp_ekl_shipmentgroup_3 where day > date_format(date_sub(current_date,100),'yyyyMMdd')
    group by CAST(SPLIT(entityid, "-")[1] AS INT)
) consign
on CAST(SPLIT(Cons.entityid, "-")[1] AS INT)=consign.consignment_id
LEFT OUTER JOIN  
(	SELECT facility.facility_id as facility_id,
	ekl_hive_facility_dim_key,
	`data`.local_territory as lt, 
	`data`.city   AS city,
	`data`.zone   AS geo_zone,
	`data`.state  AS geo_state,
	facility.postal_code				
	FROM bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim  as facility 
	JOIN bigfoot_snapshot.dart_fki_scp_ekl_geo_1_view_total AS geo 
	ON   geo.`data`.pincode=facility.postal_code
) as src_hub
on lookupkey('facility_id', Cons.`data`.source_location.id)=src_hub.ekl_hive_facility_dim_key
LEFT OUTER JOIN  
(	SELECT facility.facility_id as facility_id, 
	ekl_hive_facility_dim_key,
	`data`.local_territory as lt, 
	`data`.city   AS city,
	`data`.zone   AS geo_zone,
	`data`.state  AS geo_state,
	facility.postal_code				
	FROM bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim  as facility 
	JOIN bigfoot_snapshot.dart_fki_scp_ekl_geo_1_view_total AS geo 
	ON   geo.`data`.pincode=facility.postal_code
) as dest_hub 
on lookupkey('facility_id', Cons.`data`.destination_location.id)=dest_hub.ekl_hive_facility_dim_key
LEFT OUTER JOIN bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_view_total Conn ON Cons.`data`.connection_id = cast(split(Conn.entityid, "-")[1] AS INT)
LEFT OUTER JOIN bigfoot_journal.dart_wsr_scp_ekl_bagbasedconsignmentdata_1 bagcons
on bagcons.`data`.consignment_id=consign.consignment_id
WHERE Cons.`data`.type = 'consignment';
