INSERT OVERWRITE TABLE first_mile_bag_shipmment_map_hive_fact
SELECT
`data`.bagid as bag_tracking_id,
exp as shipment_id,
min(updatedat) as first_time
FROM
bigfoot_snapshot.dart_wsr_scp_ekl_firtmilebag_2_0_view lateral view explode(`data`.shipmentids) exploded_table as exp group by `data`.Bagid, exp ;
