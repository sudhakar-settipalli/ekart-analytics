INSERT OVERWRITE TABLE ekl_facility_hive_dim 
SELECT 
lookupkey('facility_id',facility.facility_id) AS ekl_facility_dim_key, 
facility.cutoff_time AS cutoff_time, 
facility.cst_number AS cst_number, 
facility.code AS code, 
facility.facility_id AS facility_id,
facility.coc_code AS coc_code, 
facility.type AS type, 
facility_address.`data`.city AS city, 
facility.display_name AS display_name, 
facility.vat_number AS vat_number, 
facility.name AS name, 
facility.is_in_lbt AS is_in_lbt, 
facility.zone AS zone, 
facility.active_flag AS active_flag, 
facility.tin_number AS tin_number,
facility.address_id AS facility_address_id,
facility_address.data.postal_code
FROM (SELECT `data`.cutoff_times[0] AS cutoff_time, `data`.cst_number AS cst_number, `data`.code AS code, entityId AS facility_id, `data`.coc_code AS coc_code,
 `data`.type AS type, `data`.display_name AS display_name, `data`.vat_number AS vat_number, `data`.name AS name, `data`.is_in_lbt AS is_in_lbt, 
 `data`.active_flag AS active_flag, `data`.zone AS zone, `data`.tin_number AS tin_number, 
 `data`.address_id AS address_id from bigfoot_snapshot.dart_wsr_scp_ekl_facility_0_11_view_total) 
facility left JOIN bigfoot_snapshot.dart_wsr_scp_ekl_facilityaddress_0_3_view_total facility_address ON facility.address_id=facility_address.entityId;
