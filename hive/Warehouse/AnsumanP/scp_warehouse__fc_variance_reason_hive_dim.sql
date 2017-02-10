INSERT OVERWRITE TABLE fc_variance_reason_hive_dim 
SELECT variance_reason_dim_key,variance_reason_id,variance_reason_context,variance_reason_created_at,variance_reason_created_by,
variance_reason_description,variance_reason_type,variance_system_reason,variance_reason_updated_at,variance_reason_updated_by,variance_type,
variance_warehouse_company
FROM
(
SELECT lookupkey('variance_reason_id', CONCAT(vr.entityid,'fki')) AS variance_reason_dim_key ,
vr.entityid as variance_reason_id,
vr.`data`.context as variance_reason_context,
from_unixtime(unix_timestamp(vr.`data`.created_at)) as variance_reason_created_at,
vr.`data`.created_by as variance_reason_created_by,
vr.`data`.description as variance_reason_description,
vr.`data`.reason_type as variance_reason_type,
vr.`data`.system_reason as variance_system_reason,
from_unixtime(unix_timestamp(vr.`data`.updated_at)) as variance_reason_updated_at,
vr.`data`.updated_by as variance_reason_updated_by,
vr.`data`.variance_type as variance_type,
'fki' as variance_warehouse_company
FROM bigfoot_snapshot.dart_fki_scp_warehouse_inventory_variance_reason_1_view_total vr
union all
SELECT lookupkey('variance_reason_id', CONCAT(vr.entityid,'wsr')) AS variance_reason_dim_key ,
vr.entityid as variance_reason_id,
vr.`data`.context as variance_reason_context,
from_unixtime(unix_timestamp(vr.`data`.created_at)) as variance_reason_created_at,
vr.`data`.created_by as variance_reason_created_by,
vr.`data`.description as variance_reason_description,
vr.`data`.reason_type as variance_reason_type,
vr.`data`.system_reason as variance_system_reason,
from_unixtime(unix_timestamp(vr.`data`.updated_at)) as variance_reason_updated_at,
vr.`data`.updated_by as variance_reason_updated_by,
vr.`data`.variance_type as variance_type,
'wsr' as variance_warehouse_company
FROM bigfoot_snapshot.dart_wsr_scp_warehouse_inventory_variance_reason_1_view_total vr) T1
;
