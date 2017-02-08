INSERT OVERWRITE TABLE agent_hive_dim 
SELECT lookupkey('agent_id',entityId)  AS agent_dim_key, 
entityId AS agent_id, 
`data`.type AS type, 
`data`.employee_id AS employee_id, 
`data`.employee_payroll_system AS employee_payroll_system, 
`data`.company_code AS company_id, 
`data`.name AS name, 
`data`.display_name AS display_name, 
`data`.ldap_username AS ldap_user_name, 
`data`.role AS role, 
`data`.ekl_facility.id AS location_id, 
`data`.mobile_number AS mobile_no, 
`data`.status_of_employment AS employment_status 
from bigfoot_snapshot.dart_wsr_scp_ekl_agent_1_4_view_total;
