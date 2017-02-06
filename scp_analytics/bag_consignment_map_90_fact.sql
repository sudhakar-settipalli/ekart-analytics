INSERT OVERWRITE TABLE bag_consignment_map_90_fact
select 
bag_id,
consignment_id,
mapping_datetime
from
(
SELECT
if(a.bag_id like 'Bag-%', CAST(SPLIT(a.bag_id, "-")[1] AS INT), a.bag_id) AS bag_id, 
CAST(SPLIT(a.consignment_id, "-")[1] AS INT) AS consignment_id,
CAST(a.updatedat AS TIMESTAMP) as mapping_datetime
FROM
(SELECT 
entityid as consignment_id,
EXPLODE_0 as bag_id,
MIN(updatedat) as updatedat
FROM bigfoot_snapshot.dart_wsr_scp_ekl_shipmentgroup_3_view LATERAL VIEW explode(`data`.shipment_groups) exploded_table AS EXPLODE_0 WHERE `data`.type = 'consignment' 
GROUP BY entityid,EXPLODE_0) a

union all

select 
if(EXPLODE_1  like 'Bag-%', CAST(SPLIT(EXPLODE_1 , "-")[1] AS INT), EXPLODE_1) as bag_id,
consignment_id,
mapping_datetime
FROM
(select 
a.consignment_id,
CAST(a.updatedat AS TIMESTAMP) as mapping_datetime,
bag_in_bag_check.shipgroups,
bag_in_bag_check.entityid as entityid
from
(SELECT 
entityid as consignment_id,
EXPLODE_0 as bag_id,
MIN(updatedat) as updatedat
FROM bigfoot_snapshot.dart_wsr_scp_ekl_shipmentgroup_3_view LATERAL VIEW explode(`data`.shipment_groups) exploded_table AS EXPLODE_0 WHERE `data`.type = 'consignment' 
GROUP BY entityid,EXPLODE_0) a
inner join 
(select
entityid,
`data`.shipment_groups as shipgroups
from
bigfoot_snapshot.dart_wsr_scp_ekl_shipmentgroup_3_view
where `data`.type = 'bag' 
and  
size(`data`.shipment_groups) > 1
) bag_in_bag_check
on bag_in_bag_check.entityid = a.bag_id
)c lateral view explode(shipgroups) exploded_table2 as EXPLODE_1 
where EXPLODE_1 <> entityid
) as final;

