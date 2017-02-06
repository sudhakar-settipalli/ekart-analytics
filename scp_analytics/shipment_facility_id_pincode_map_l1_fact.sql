INSERT OVERWRITE TABLE shipment_facility_id_pincode_map_l1_fact
SELECT
distinct entityid as facility_id,
pincode,
`data`.type as hub_type
FROM
bigfoot_snapshot.dart_wsr_scp_ekl_facility_0_11_view_total lateral view explode(`data`.facility_routes.pin_codes) exploded_table as pincode where `data`.type in ('BULK_HUB','DELIVERY_HUB') and `data`.active_flag = 1;
