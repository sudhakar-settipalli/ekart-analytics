INSERT overwrite TABLE shipment_first_last_bag_map_l1_90_fact 
SELECT          
d.shipment_id                 AS vendor_tracking_id, 
b.first_bag                   AS shipment_first_bag_id, 
c.last_bag                    AS shipment_last_bag_id, 
e.bag_current_status          AS shipment_last_bag_status, 
e.bag_current_hub_type        AS shipment_last_bag_hub_type, 
e.bag_current_hub_id          AS shipment_last_bag_hub_id, 
e.bag_created_at_datetime         AS shipment_last_bag_create_datetime, 
e.bag_receive_datetime        AS shipment_last_bag_receive_datetime, 
e.bag_destination_hub_inscan_datetime  AS shipment_last_bag_reach_datetime, 
e.bag_first_closed_datetime         AS shipment_last_bag_close_datetime, 
e.bag_source_hub_id           AS shipment_last_bag_created_hub_id, 
e.bag_tracking_id             AS shipment_last_bag_tracking_id, 
f.bag_created_at_datetime         AS shipment_first_bag_create_datetime,  
f.bag_receive_datetime        AS shipment_first_bag_receive_datetime, 
f.bag_first_closed_datetime   AS shipment_first_bag_closed_datetime, 
f.bag_assigned_hub_id         AS shipment_first_bag_final_hub_id, 
f.bag_source_hub_id           AS shipment_first_bag_created_hub_id, 
f.bag_tracking_id             AS shipment_first_bag_tracking_id,
e.bag_created_hub_type        AS shipment_last_bag_created_hub_type,
e.bag_destination_hub_type    AS shipment_last_bag_destination_hub_type,
f.bag_destination_hub_inscan_datetime AS shipment_first_bag_reach_datetime,
f1.bag_destination_hub_inscan_datetime AS shipment_first_dh_bag_reach_datetime,
e1.bag_destination_hub_inscan_datetime AS shipment_last_dh_bag_reach_datetime,
e1.bag_current_hub_type        AS shipment_last_bag_dh_hub_type, 
e1.bag_current_hub_id          AS shipment_last_bag_dh_hub_id,
e1.bag_source_hub_id           AS shipment_last_bag_created_dh_hub_id,
f1.bag_assigned_hub_id         AS shipment_first_bag_final_dh_hub_id, 
f1.bag_source_hub_id           AS shipment_first_bag_created_dh_hub_id, 
e1.bag_created_hub_type        AS shipment_last_bag_created_dh_hub_type
FROM( 
SELECT   shipment_id, 
min(first_time) AS first_bag_create_time, 
max(first_time) AS last_bag_create_time 
FROM     bigfoot_external_neo.scp_ekl__shipment_closedbag_map_l1_90_fact 
GROUP BY shipment_id)d 
INNER JOIN 
(SELECT bag AS first_bag, 
shipment_id, 
first_time AS first_bag_create_time 
FROM   bigfoot_external_neo.scp_ekl__shipment_closedbag_map_l1_90_fact)b 
ON ( 
d.shipment_id = b.shipment_id 
and b.first_bag_create_time = d.first_bag_create_time) 
INNER JOIN 
( 
SELECT bag AS last_bag, 
shipment_id, 
first_time AS last_bag_create_time 
FROM   bigfoot_external_neo.scp_ekl__shipment_closedbag_map_l1_90_fact)c 
ON ( 
d.shipment_id = c.shipment_id 
and c.last_bag_create_time = d.last_bag_create_time) 
LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__bag_l1_90_fact e 
ON ( 
e.bag_id = cast( split(c.last_bag, "-")[1] AS int)) 
LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__bag_l1_90_fact f 
ON (f.bag_id = cast( split(b.first_bag, "-")[1] AS int))
LEFT OUTER JOIN (
SELECT shipment_id, 
min(first_time) AS first_bag_create_time,
max(first_time) AS last_bag_create_time
from  
(select 
 fi.shipment_id,
 fi.bag, 
 fi.first_time
FROM bigfoot_external_neo.scp_ekl__shipment_closedbag_map_l1_90_fact fi
left join bigfoot_external_neo.scp_ekl__bag_l1_90_fact se
on split(fi.bag,"-")[1] = se.bag_id 
where bag_destination_hub_type = 'DELIVERY_HUB') a
group by shipment_id)d1 ON d1.shipment_id = d.shipment_id
LEFT OUTER JOIN
(SELECT bag AS first_bag,shipment_id,first_time AS first_bag_create_time 
FROM bigfoot_external_neo.scp_ekl__shipment_closedbag_map_l1_90_fact)b1 
ON (d1.shipment_id = b1.shipment_id and b1.first_bag_create_time = d1.first_bag_create_time) 
LEFT OUTER JOIN 
(SELECT bag AS last_bag, 
shipment_id, 
first_time AS last_bag_create_time 
FROM   bigfoot_external_neo.scp_ekl__shipment_closedbag_map_l1_90_fact)c1 
ON (d1.shipment_id = c1.shipment_id and c1.last_bag_create_time = d1.last_bag_create_time)
LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__bag_l1_90_fact e1 
ON (e1.bag_id = cast( split(c1.last_bag, "-")[1] AS int))
LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__bag_l1_90_fact f1 
ON (f1.bag_id = cast( split(b1.first_bag, "-")[1] AS int));
