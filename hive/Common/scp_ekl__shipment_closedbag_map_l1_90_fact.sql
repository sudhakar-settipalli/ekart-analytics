INSERT OVERWRITE TABLE shipment_closedbag_map_l1_90_fact
select bag,
shipment_id,
first_time,
hub_type,
hub_id,
bag_in_bag_flag
from
(
SELECT entityid AS bag,
exp AS shipment_id,
min(updatedat) AS first_time,
min(data.current_location.type) AS hub_type,
min(data.current_location.id) AS hub_id,
0 as bag_in_bag_flag
FROM bigfoot_journal.dart_wsr_scp_ekl_shipmentgroup_3 LATERAL VIEW explode(`data`.shipments) exploded_table AS exp
WHERE `data`.type = 'bag' and `data`.status = 'CLOSED' and day > date_format(date_sub(current_date,90),'yyyyMMdd')
GROUP BY entityid,exp

UNION 

select PCB.bag as bag,
-- PCB.child_bag as child_bag,
C.shipment_id as shipment_id,
PCB.first_time as first_time,
hub_type as hub_type,
hub_id as hub_id,
-- child_bag__hub_type,
-- child_bag_hub_id,
1 as bag_in_bag_flag
from
(SELECT entityid AS bag,
exp AS child_bag,
min(updatedat) AS first_time,
min(data.current_location.type) AS hub_type,
min(data.current_location.id) AS hub_id,
1 as bag_in_bag_flag
FROM bigfoot_snapshot.dart_wsr_scp_ekl_shipmentgroup_3_view_total LATERAL VIEW explode(`data`.shipment_groups) exploded_table AS exp
WHERE `data`.type = 'bag' and size(`data`.shipment_groups) >= 2 and from_unixtime(cast(updatedat/1000 as int)) >date_format(date_sub(current_date,90),'yyyy-MM-dd')
GROUP BY entityid,exp) as PCB
INNER JOIN
(SELECT entityid AS bag,
exp AS shipment_id,
min(updatedat) AS first_time,
min(data.current_location.type) AS child_bag__hub_type,
min(data.current_location.id) AS child_bag_hub_id
FROM bigfoot_snapshot.dart_wsr_scp_ekl_shipmentgroup_3_view_total LATERAL VIEW explode(`data`.shipments) exploded_table AS exp
WHERE `data`.type = 'bag' 
GROUP BY entityid,exp
) as C
on PCB.child_bag=C.bag
) as final;
