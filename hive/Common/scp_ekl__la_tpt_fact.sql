INSERT overwrite TABLE la_tpt_fact
SELECT 
  A.vendor_tracking_id AS vendor_tracking_id,
  'dummy' AS con_tracking_id,
  A.origin_facility_id AS origin_facility_id,
  A.dest_facility_id AS dest_facility_id,
  A.fsd_assigned_hub_id_key AS fsd_assigned_hub_id_key,
  A.shipment_origin_facility_id_key AS shipment_origin_facility_id_key,
  A.actual_conn_id AS actual_conn_id,
  A.actual_cons_id AS actual_cons_id,
  A.Actual_cons_created_at AS actual_cons_created_datetime,
  lookup_date(to_date(A.Actual_cons_created_at)) AS actual_cons_created_date_key,
  A.shipment_first_consignment_id AS shipment_first_consignment_id,
  'Null' AS first_conn,
  A.tpt_compliance_flag AS tpt_compliance_flag,
  if(A.actual_dh_recieve_date_time IS NOT NULL, if(A.actual_dh_recieve_date_time<=A.shipment_tpt_dh_eta_datetime,0,1),if(from_unixtime(unix_timestamp())>A.shipment_tpt_dh_eta_datetime,1,0)) AS tpt_intransit_breach_flag,
  A.Conn_eta_in_sec AS conn_eta_in_sec,
  A.shipment_tpt_dh_eta_datetime AS shipment_tpt_dh_eta_datetime,
  lookup_date(to_date(A.shipment_tpt_dh_eta_datetime)) AS shipment_tpt_dh_eta_date_key,
  A.fulfill_item_unit_dispatch_expected_time AS fulfill_item_unit_dispatch_expected_time,
  A.fulfill_item_unit_dispatch_actual_time AS fulfill_item_unit_dispatch_actual_time,
  A.shipment_first_consignment_create_datetime AS shipment_first_consignment_create_datetime,
  lookup_date(to_date(A.shipment_first_consignment_create_datetime)) AS shipment_first_consignment_create_date_key,
  A.actual_dh_recieve_date_time AS dh_recieve_datetime,
  lookup_date(to_date(A.actual_dh_recieve_date_time)) AS dh_recieve_date_key,
  A.ekl_shipment_type,
  A.shipment_mh_etd_datetime,
  lookup_date(to_date(A.shipment_mh_etd_datetime)) AS shipment_mh_etd_date_key,
  if(A.shipment_first_consignment_create_datetime IS NOT NULL, if(A.shipment_first_consignment_create_datetime <= A.shipment_mh_etd_datetime,0,1), if(from_unixtime(unix_timestamp())>A.shipment_mh_etd_datetime,1,0)) AS shipment_mh_etd_breach_flag
FROM
(
  SELECT 
    Sh.fsd_assigned_hub_id_key AS fsd_assigned_hub_id_key,
    Sh.shipment_origin_facility_id_key AS shipment_origin_facility_id_key,
    Sh.vendor_tracking_id AS vendor_tracking_id,
    Sh.shipment_first_consignment_create_datetime AS shipment_first_consignment_create_datetime,
    Sh.shipment_first_consignment_id AS shipment_first_consignment_id,
    Sh.shipment_last_consignment_conn_id AS actual_conn_id,
    Sh.shipment_last_consignment_id AS actual_cons_id,
    if(isnull(Sh.shipment_last_consignment_conn_id) AND not(isnull(Sh.shipment_last_consignment_id)) ,1,0) AS tpt_compliance_flag,
    unix_timestamp(Sh.shipment_last_consignment_eta_datetime) - unix_timestamp(Sh.shipment_first_consignment_create_datetime) AS Conn_eta_in_sec,
		CASE 
		  WHEN ff.fulfill_item_unit_is_for_slotted_delivery='NotSlotted' THEN to_utc_timestamp(concat(substr(ff.fulfill_item_unit_delivered_status_expected_date_key,1,4),'-',substr(ff.fulfill_item_unit_delivered_status_expected_date_key,5,2),'-',substr(ff.fulfill_item_unit_delivered_status_expected_date_key,7,2),' 06:00:00'),'')
		  WHEN ff.fulfill_item_unit_is_for_slotted_delivery='Slotted' 
  		  THEN 
  		  to_utc_timestamp(from_unixtime(IF(unix_timestamp(Sh.shipment_last_consignment_eta_datetime) >=
  		  unix_timestamp(ff.dispatch_max_time) + 7200 + unix_timestamp(Sh.shipment_last_consignment_eta_datetime) - unix_timestamp(Sh.shipment_first_consignment_create_datetime),
  		  unix_timestamp(Sh.shipment_last_consignment_eta_datetime),
  		  unix_timestamp(ff.dispatch_max_time) + 7200 + unix_timestamp(Sh.shipment_last_consignment_eta_datetime) - unix_timestamp(Sh.shipment_first_consignment_create_datetime)
  		  )),'')
		END AS shipment_tpt_dh_eta_datetime,
		Sh.shipment_last_consignment_create_datetime AS Actual_cons_created_at,
    ff.fulfill_item_unit_dispatch_expected_time AS fulfill_item_unit_dispatch_expected_time,
    ff.fulfill_item_unit_dispatch_actual_time AS fulfill_item_unit_dispatch_actual_time,
    hour(ff.fulfill_item_unit_dispatch_actual_time)*3600+minute(ff.fulfill_item_unit_dispatch_actual_time)*60+second(ff.fulfill_item_unit_dispatch_actual_time) AS D_act_sec,
    hour(ff.fulfill_item_unit_dispatch_expected_time)*3600+minute(ff.fulfill_item_unit_dispatch_expected_time)*60+second(ff.fulfill_item_unit_dispatch_expected_time) AS D_exp_sec,
    Sh.fsd_first_dh_received_datetime AS actual_dh_recieve_date_time,
    Origin.origin_facility_id AS origin_facility_id,
    Dest.dest_facility_id AS dest_facility_id,
    Sh.ekl_shipment_type,
    from_unixtime(unix_timestamp(ff.dispatch_max_time) + 7200) AS shipment_mh_etd_datetime
   FROM
   (
      SELECT 
        fulfill_item_unit_dispatch_expected_time,
        fulfill_item_unit_dispatch_actual_time,
        if(fulfill_item_unit_dispatch_expected_time>fulfill_item_unit_dispatch_actual_time,fulfill_item_unit_dispatch_expected_time, fulfill_item_unit_dispatch_actual_time) AS dispatch_max_time,
        fulfill_item_unit_is_for_slotted_delivery,
        fulfill_item_unit_delivered_status_expected_date_key,
        shipment_merchant_reference_id
      FROM 
        bigfoot_external_neo.scp_fulfillment__la_fulfilment_fact
      WHERE 
        fulfill_item_unit_size='Large'
    ) ff
   INNER JOIN
   (
      SELECT 
        merchant_reference_id,
        fsd_assigned_hub_id_key,
        shipment_origin_facility_id_key,
        vendor_tracking_id,
        fsd_first_dh_received_datetime,
        lookup_date(to_date(fsd_first_dh_received_datetime)) AS fsd_first_dh_received_date_key,
        shipment_last_consignment_create_datetime,
        shipment_first_consignment_create_datetime,
        shipment_last_consignment_id,
        shipment_first_consignment_id,
        shipment_last_consignment_conn_id,
        shipment_last_consignment_eta_in_sec,
        shipment_last_consignment_eta_datetime,
        ekl_shipment_type
      FROM 
        bigfoot_external_neo.scp_ekl__la_shipment_l0_fact
      WHERE 
        shipment_last_consignment_id IS NOT NULL
    ) Sh 
    ON 
      ff.shipment_merchant_reference_id = Sh.merchant_reference_id
    LEFT JOIN
    (
      SELECT 
        ekl_facility_hive_dim_key,
        facility_id AS origin_facility_id
      FROM 
        bigfoot_external_neo.scp_ekl__ekl_facility_hive_dim
    ) Origin 
    ON 
      Sh.shipment_origin_facility_id_key = NVL(Origin.ekl_facility_hive_dim_key, 0)
    LEFT JOIN
    (
      SELECT 
        ekl_facility_hive_dim_key AS dest_fid_dim_key,
        facility_id AS dest_facility_id
      FROM 
        bigfoot_external_neo.scp_ekl__ekl_facility_hive_dim
    ) Dest 
    ON 
      Sh.fsd_assigned_hub_id_key = NVL(Dest.dest_fid_dim_key, 0)
    LEFT JOIN
    (
      SELECT 
        fkl_facility_id,
        mh_facility_id
      FROM 
        bigfoot_common.ekl_fkl_facility_mother_hub_mapping
    ) B 
    ON 
      B.fkl_facility_id = Origin.origin_facility_id
    WHERE 
      not(isnull(Sh.vendor_tracking_id))
) A;
