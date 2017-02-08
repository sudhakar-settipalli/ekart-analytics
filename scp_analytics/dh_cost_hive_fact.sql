INSERT OVERWRITE TABLE dh_cost_hive_fact
SELECT distinct
X.vendor_tracking_id ,
lookup_date(X.fsd_first_dh_received_datetime) AS dh_received_date_key,
if(X.ekl_shipment_type = 'rvp',X.shipment_first_received_hub_id,X.fsd_assigned_hub_id) as   hub_id,
IF(
lookup_date(X.fsd_first_dh_received_datetime) >= RC.start_date AND (
lookup_date(X.fsd_first_dh_received_datetime) <= RC.end_date OR RC.end_date IS NULL),RC.offroll_salary,0) as offroll_salary,
IF(
lookup_date(X.fsd_first_dh_received_datetime) >= RC.start_date AND (
lookup_date(X.fsd_first_dh_received_datetime) <= RC.end_date OR RC.end_date IS NULL),RC.pan_india_other,0) AS pan_india_other,
IF(
lookup_date(X.fsd_first_dh_received_datetime) >= RC.start_date AND (
lookup_date(X.fsd_first_dh_received_datetime) <= RC.end_date OR RC.end_date IS NULL),if(X.ekl_shipment_type = 'forward',RC.delivery_conveyance,0),0) AS delivery_conveyance,
IF(
lookup_date(X.fsd_first_dh_received_datetime) >= RC.start_date AND (
lookup_date(X.fsd_first_dh_received_datetime) <= RC.end_date OR RC.end_date IS NULL),if(X.ekl_shipment_type = 'rvp',RC.rvp_conveyance,0),0) AS rvp_conveyance ,
IF(
lookup_date(X.fsd_first_dh_received_datetime) >= RC.start_date AND (
lookup_date(X.fsd_first_dh_received_datetime) <= RC.end_date OR RC.end_date IS NULL),if(X.shipment_fa_flag IN (false),RC.mp_conveyance,0),0) AS mp_conveyance,
IF(
lookup_date(X.fsd_first_dh_received_datetime) >= RC.start_date AND (
lookup_date(X.fsd_first_dh_received_datetime) <= RC.end_date OR RC.end_date IS NULL),RC.area_manager_sal,0) AS area_manager_sal,
IF(
lookup_date(X.fsd_first_dh_received_datetime) >= RC.start_date AND (
lookup_date(X.fsd_first_dh_received_datetime) <= RC.end_date OR RC.end_date IS NULL),RC.dh_security,0) AS dh_security,
IF(
lookup_date(X.fsd_first_dh_received_datetime) >= RC.start_date AND (
lookup_date(X.fsd_first_dh_received_datetime) <= RC.end_date OR RC.end_date IS NULL),RC.van,0) AS van
FROM
bigfoot_external_neo.scp_ekl__shipment_l1_90_fact X
LEFT OUTER JOIN bigfoot_common.mastercard_dh_cps RC ON (if(X.ekl_shipment_type = 'rvp',X.shipment_first_received_hub_id,X.fsd_assigned_hub_id) = RC.hubid) ;
