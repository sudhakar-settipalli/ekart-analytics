INSERT OVERWRITE TABLE bag_l1_90_fact
SELECT
CAST( SPLIT(a.entityid, "-")[1] AS INT) AS bag_id,
b.`data`.vendor_tracking_id AS bag_tracking_id,
b.`data`.seal_id AS bag_seal_id,
b.`data`.status AS bag_current_status,
concat_ws('-', b.`data`.attributes) AS bag_category,
size(b.`data`.shipments) AS bag_no_of_shipments,
a.bag_number_of_offloads AS bag_number_of_offloads,
a.bag_number_of_hops AS bag_number_of_hops,
c.status AS bag_first_offload_reason,
If(size(b.`data`.notes) = 0, NULL, If(b.`data`.notes[0].type = 'EtaUpdate Notes',b.`data`.notes[0].flag,NULL)) as bag_eta_update_reason,
b.`data`.system_weight.physical AS bag_system_weight,
b.`data`.sender_weight.physical AS bag_sender_weight,
b.`data`.sender_weight.volumetric AS bag_sender_volumetric_weight,
IF(b.`data`.system_weight.physical > b.`data`.system_weight.volumetric, b.`data`.system_weight.physical, b.`data`.system_weight.volumetric ) AS bag_chargeable_weight,
b.`data`.receiver_weight.physical AS bag_receiver_weight,
b.`data`.transportation_cost.total_charge.value AS bag_transport_cost,
b.`data`.source_location.id AS bag_source_hub_id,
b.`data`.current_location.id AS bag_current_hub_id,
b.`data`.source_location.type as bag_created_hub_type,
b.`data`.current_location.type as bag_current_hub_type,
b.`data`.destination_location.type as bag_destination_hub_type,
b.`data`.destination_location.id AS bag_assigned_hub_id,
b.`data`.created_at as bag_created_at_datetime,
CAST(a.first_close_time AS TIMESTAMP) as bag_first_closed_datetime,
CAST(a.bag_first_connected_time AS TIMESTAMP) as bag_first_connected_datetime,
CAST(a.bag_last_connected_time AS TIMESTAMP) as bag_last_connected_datetime,
CAST(a.last_intermediate_hub_inscan_time AS TIMESTAMP) as bag_last_intermediate_hub_inscan_datetime,
CAST(a.bag_destination_hub_inscan_time AS TIMESTAMP) as bag_destination_hub_inscan_datetime,
CAST(a.bag_receive_time AS TIMESTAMP) AS bag_receive_datetime,
CAST(b.updatedat AS TIMESTAMP) AS bag_last_update_datetime,
CAST(a.bag_first_offload_time AS TIMESTAMP) AS bag_first_offload_datetime,
x.hub_id as source_hub_id,x.scanned_date_time,x.bag_weight,x.machine_id,x.hub_type,x.machine_status,x.weight_entry_type,
y.hub_id as first_hop_id,
y.scanned_date_time as first_inscan,
y.bag_weight as first_hop_weight,
y.machine_id as first_hop_machine_id,
y.hub_type as first_hop_type,
y.machine_status as first_hop_machine_status,
y.weight_entry_type as first_hop_weight_entry_type,
z.hub_id as second_hop_id,
z.scanned_date_time as second_inscan,
z.bag_weight as second_hop_weight,
z.machine_id as second_hop_machine_id,
z.hub_type as second_hop_type,
z.machine_status as second_hop_machine_status,
z.weight_entry_type as second_hop_weight_entry_type,
d.hub_id as third_hop_id,
d.scanned_date_time as third_inscan,
d.bag_weight as third_hop_weight,
d.machine_id as third_hop_machine_id,
d.hub_type as third_hop_type,
d.machine_status as third_hop_machine_status,
d.weight_entry_type as third_hop_weight_entry_type,
y.is_within_tolerance as first_is_within_tolerance,
z.is_within_tolerance as second_is_within_tolerance,
d.is_within_tolerance as third_is_within_tolerance,
dest.machine_id as last_hub_machine_id, 
dest.bag_weight as last_hub_weight,
dest.is_within_tolerance as last_hub_is_within_tolerance,
dest.hub_id as last_hop_id,
dest.scanned_date_time as last_inscan,
dest.hub_type as last_hub_type,
bs.no_of_shipments_inscanned,
b.data.eta as eta_datetime
FROM
(SELECT entityid,
       MIN(updatedat) AS first_time,
       MIN(IF(`data`.status='CLOSED', updatedat, NULL)) AS first_close_time,
       MAX(IF(`data`.status='CLOSED' AND `data`.source_location.id <> `data`.current_location.id, updatedat, NULL)) AS last_intermediate_hub_inscan_time,
       MIN(IF(`data`.status='INTRANSIT', updatedat, NULL)) AS bag_first_connected_time,
       MIN(IF(`data`.status='INTRANSIT', updatedat, NULL)) AS bag_last_connected_time,
       MIN(IF(`data`.status='REACHED', updatedat, NULL)) AS bag_destination_hub_inscan_time,
       MIN(IF(`data`.status='RECEIVED', updatedat, NULL)) AS bag_receive_time,
       SUM(IF(`data`.status LIKE '%Offloaded%', 1, 0)) AS bag_number_of_offloads,
       SUM(IF(`data`.status='CLOSED', 1, 0)) AS bag_number_of_hops,
       MIN(IF(`data`.status LIKE '%Offloaded%', updatedat, NULL)) AS bag_first_offload_time
FROM bigfoot_journal.dart_wsr_scp_ekl_shipmentgroup_3
WHERE lower(`data`.type) = 'bag' and   day > date_format(date_sub(current_date,100),'yyyyMMdd')
GROUP BY entityid) a
LEFT OUTER JOIN 
( select entityid,`data`.status as status,updatedat from bigfoot_journal.dart_wsr_scp_ekl_shipmentgroup_3 where day > date_format(date_sub(current_date,100),'yyyyMMdd'))  c
ON (c.entityid = a.entityid AND c.status LIKE '%Offloaded%' AND c.updatedat = a.bag_first_offload_time)
INNER JOIN bigfoot_snapshot.dart_wsr_scp_ekl_shipmentgroup_3_view b ON (b.entityid = a.entityid)
LEFT JOIN
(
select hub_id,tracking_id,scanned_date_time,bag_weight,rank ,
machine_id,
hub_type,
machine_status,
weight_entry_type,
is_within_tolerance
from
(select data.hub_id,
data.tracking_id,
data.scanned_date_time,
data.bag_weight,
data.machine_id,
data.hub_type,
data.machine_status,
data.weight_entry_type,
data.is_within_tolerance,
ROW_NUMBER() OVER (PARTITION BY data.tracking_id ORDER BY data.scanned_date_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as rank
from bigfoot_journal.dart_wsr_scp_ekl_weighthistory_1  ) as a1 where a1.rank=1) as x

ON (x.tracking_id=b.data.vendor_tracking_id)
LEFT JOIN
(
select hub_id,tracking_id,scanned_date_time,bag_weight,rank,
machine_id,
hub_type,
machine_status,
weight_entry_type,
is_within_tolerance
from
(select data.hub_id,
data.tracking_id,
data.scanned_date_time,
data.bag_weight,
data.machine_id,
data.hub_type,
data.machine_status,
data.weight_entry_type,
data.is_within_tolerance,
ROW_NUMBER() OVER (PARTITION BY data.tracking_id ORDER BY data.scanned_date_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as rank
from bigfoot_journal.dart_wsr_scp_ekl_weighthistory_1 ) as b1 where b1.rank=2) as y
on (y.tracking_id=x.tracking_id )
LEFT JOIN
(
select hub_id,tracking_id,scanned_date_time,bag_weight,rank, 
machine_id,
hub_type,
machine_status,
weight_entry_type,
is_within_tolerance
from
(select data.hub_id,
data.tracking_id,
data.scanned_date_time,
data.bag_weight,
data.machine_id,
data.hub_type,
data.machine_status,
data.weight_entry_type,
data.is_within_tolerance,
ROW_NUMBER() OVER (PARTITION BY data.tracking_id ORDER BY data.scanned_date_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as rank
from bigfoot_journal.dart_wsr_scp_ekl_weighthistory_1  ) as c1 where c1.rank=3) as z
ON (z.tracking_id=x.tracking_id )
LEFT JOIN
(
select hub_id,tracking_id,scanned_date_time,bag_weight,rank, 
machine_id,
hub_type,
machine_status,
weight_entry_type,
is_within_tolerance
from
(select data.hub_id,
data.tracking_id,
data.scanned_date_time,
data.bag_weight,
data.machine_id,
data.hub_type,
data.machine_status,
data.weight_entry_type,
data.is_within_tolerance,
ROW_NUMBER() OVER (PARTITION BY data.tracking_id ORDER BY data.scanned_date_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as rank
from bigfoot_journal.dart_wsr_scp_ekl_weighthistory_1 ) as d1 where d1.rank=4) as d
on (d.tracking_id=x.tracking_id )

LEFT JOIN
(
select hub_id,tracking_id,scanned_date_time,bag_weight,rank ,
machine_id,
hub_type,
machine_status,
weight_entry_type,
is_within_tolerance
from
(select data.hub_id,
data.tracking_id,
data.scanned_date_time,
data.bag_weight,
data.machine_id,
data.hub_type,
data.machine_status,
data.weight_entry_type,
data.is_within_tolerance,
ROW_NUMBER() OVER (PARTITION BY data.tracking_id ORDER BY data.scanned_date_time DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as rank
from bigfoot_journal.dart_wsr_scp_ekl_weighthistory_1  ) as a1 where a1.rank=1) as dest
on dest.tracking_id=b.data.vendor_tracking_id
left join
(
select count(distinct vendor_tracking_id) as no_of_shipments_inscanned,incoming_bag_tracking_id,incoming_bag_destination_hub_id from bigfoot_external_neo.scp_ekl__shipment_facility_l2_90_fact
where shipment_facility_inscan_datetime is not null
group by incoming_bag_tracking_id,incoming_bag_destination_hub_id
) as bs
on bs.incoming_bag_tracking_id=dest.tracking_id and bs.incoming_bag_destination_hub_id=dest.hub_id
INSERT OVERWRITE TABLE bag_l1_90_fact
SELECT
CAST( SPLIT(a.entityid, "-")[1] AS INT) AS bag_id,
b.`data`.vendor_tracking_id AS bag_tracking_id,
b.`data`.seal_id AS bag_seal_id,
b.`data`.status AS bag_current_status,
concat_ws('-', b.`data`.attributes) AS bag_category,
size(b.`data`.shipments) AS bag_no_of_shipments,
a.bag_number_of_offloads AS bag_number_of_offloads,
a.bag_number_of_hops AS bag_number_of_hops,
c.status AS bag_first_offload_reason,
If(size(b.`data`.notes) = 0, NULL, If(b.`data`.notes[0].type = 'EtaUpdate Notes',b.`data`.notes[0].flag,NULL)) as bag_eta_update_reason,
b.`data`.system_weight.physical AS bag_system_weight,
b.`data`.sender_weight.physical AS bag_sender_weight,
b.`data`.sender_weight.volumetric AS bag_sender_volumetric_weight,
IF(b.`data`.system_weight.physical > b.`data`.system_weight.volumetric, b.`data`.system_weight.physical, b.`data`.system_weight.volumetric ) AS bag_chargeable_weight,
b.`data`.receiver_weight.physical AS bag_receiver_weight,
b.`data`.transportation_cost.total_charge.value AS bag_transport_cost,
b.`data`.source_location.id AS bag_source_hub_id,
b.`data`.current_location.id AS bag_current_hub_id,
b.`data`.source_location.type as bag_created_hub_type,
b.`data`.current_location.type as bag_current_hub_type,
b.`data`.destination_location.type as bag_destination_hub_type,
b.`data`.destination_location.id AS bag_assigned_hub_id,
b.`data`.created_at as bag_created_at_datetime,
CAST(a.first_close_time AS TIMESTAMP) as bag_first_closed_datetime,
CAST(a.bag_first_connected_time AS TIMESTAMP) as bag_first_connected_datetime,
CAST(a.bag_last_connected_time AS TIMESTAMP) as bag_last_connected_datetime,
CAST(a.last_intermediate_hub_inscan_time AS TIMESTAMP) as bag_last_intermediate_hub_inscan_datetime,
CAST(a.bag_destination_hub_inscan_time AS TIMESTAMP) as bag_destination_hub_inscan_datetime,
CAST(a.bag_receive_time AS TIMESTAMP) AS bag_receive_datetime,
CAST(b.updatedat AS TIMESTAMP) AS bag_last_update_datetime,
CAST(a.bag_first_offload_time AS TIMESTAMP) AS bag_first_offload_datetime,
x.hub_id as source_hub_id,x.scanned_date_time,x.bag_weight,x.machine_id,x.hub_type,x.machine_status,x.weight_entry_type,
y.hub_id as first_hop_id,
y.scanned_date_time as first_inscan,
y.bag_weight as first_hop_weight,
y.machine_id as first_hop_machine_id,
y.hub_type as first_hop_type,
y.machine_status as first_hop_machine_status,
y.weight_entry_type as first_hop_weight_entry_type,
z.hub_id as second_hop_id,
z.scanned_date_time as second_inscan,
z.bag_weight as second_hop_weight,
z.machine_id as second_hop_machine_id,
z.hub_type as second_hop_type,
z.machine_status as second_hop_machine_status,
z.weight_entry_type as second_hop_weight_entry_type,
d.hub_id as third_hop_id,
d.scanned_date_time as third_inscan,
d.bag_weight as third_hop_weight,
d.machine_id as third_hop_machine_id,
d.hub_type as third_hop_type,
d.machine_status as third_hop_machine_status,
d.weight_entry_type as third_hop_weight_entry_type,
y.is_within_tolerance as first_is_within_tolerance,
z.is_within_tolerance as second_is_within_tolerance,
d.is_within_tolerance as third_is_within_tolerance,
dest.machine_id as last_hub_machine_id, 
dest.bag_weight as last_hub_weight,
dest.is_within_tolerance as last_hub_is_within_tolerance,
dest.hub_id as last_hop_id,
dest.scanned_date_time as last_inscan,
dest.hub_type as last_hub_type,
bs.no_of_shipments_inscanned,
b.data.eta as eta_datetime
FROM
(SELECT entityid,
       MIN(updatedat) AS first_time,
       MIN(IF(`data`.status='CLOSED', updatedat, NULL)) AS first_close_time,
       MAX(IF(`data`.status='CLOSED' AND `data`.source_location.id <> `data`.current_location.id, updatedat, NULL)) AS last_intermediate_hub_inscan_time,
       MIN(IF(`data`.status='INTRANSIT', updatedat, NULL)) AS bag_first_connected_time,
       MIN(IF(`data`.status='INTRANSIT', updatedat, NULL)) AS bag_last_connected_time,
       MIN(IF(`data`.status='REACHED', updatedat, NULL)) AS bag_destination_hub_inscan_time,
       MIN(IF(`data`.status='RECEIVED', updatedat, NULL)) AS bag_receive_time,
       SUM(IF(`data`.status LIKE '%Offloaded%', 1, 0)) AS bag_number_of_offloads,
       SUM(IF(`data`.status='CLOSED', 1, 0)) AS bag_number_of_hops,
       MIN(IF(`data`.status LIKE '%Offloaded%', updatedat, NULL)) AS bag_first_offload_time
FROM bigfoot_journal.dart_wsr_scp_ekl_shipmentgroup_3
WHERE lower(`data`.type) = 'bag' and   day > date_format(date_sub(current_date,100),'yyyyMMdd')
GROUP BY entityid) a
LEFT OUTER JOIN 
( select entityid,`data`.status as status,updatedat from bigfoot_journal.dart_wsr_scp_ekl_shipmentgroup_3 where day > date_format(date_sub(current_date,100),'yyyyMMdd'))  c
ON (c.entityid = a.entityid AND c.status LIKE '%Offloaded%' AND c.updatedat = a.bag_first_offload_time)
LEFT JOIN bigfoot_snapshot.dart_wsr_scp_ekl_shipmentgroup_3_view b ON (b.entityid = a.entityid)
LEFT JOIN
(
select hub_id,tracking_id,scanned_date_time,bag_weight,rank ,
machine_id,
hub_type,
machine_status,
weight_entry_type,
is_within_tolerance
from
(select data.hub_id,
data.tracking_id,
data.scanned_date_time,
data.bag_weight,
data.machine_id,
data.hub_type,
data.machine_status,
data.weight_entry_type,
data.is_within_tolerance,
ROW_NUMBER() OVER (PARTITION BY data.tracking_id ORDER BY data.scanned_date_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as rank
from bigfoot_journal.dart_wsr_scp_ekl_weighthistory_1  ) as a1 where a1.rank=1) as x

ON (x.tracking_id=b.data.vendor_tracking_id)
LEFT JOIN
(
select hub_id,tracking_id,scanned_date_time,bag_weight,rank,
machine_id,
hub_type,
machine_status,
weight_entry_type,
is_within_tolerance
from
(select data.hub_id,
data.tracking_id,
data.scanned_date_time,
data.bag_weight,
data.machine_id,
data.hub_type,
data.machine_status,
data.weight_entry_type,
data.is_within_tolerance,
ROW_NUMBER() OVER (PARTITION BY data.tracking_id ORDER BY data.scanned_date_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as rank
from bigfoot_journal.dart_wsr_scp_ekl_weighthistory_1 ) as b1 where b1.rank=2) as y
on (y.tracking_id=x.tracking_id )
LEFT JOIN
(
select hub_id,tracking_id,scanned_date_time,bag_weight,rank, 
machine_id,
hub_type,
machine_status,
weight_entry_type,
is_within_tolerance
from
(select data.hub_id,
data.tracking_id,
data.scanned_date_time,
data.bag_weight,
data.machine_id,
data.hub_type,
data.machine_status,
data.weight_entry_type,
data.is_within_tolerance,
ROW_NUMBER() OVER (PARTITION BY data.tracking_id ORDER BY data.scanned_date_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as rank
from bigfoot_journal.dart_wsr_scp_ekl_weighthistory_1  ) as c1 where c1.rank=3) as z
ON (z.tracking_id=x.tracking_id )
LEFT JOIN
(
select hub_id,tracking_id,scanned_date_time,bag_weight,rank, 
machine_id,
hub_type,
machine_status,
weight_entry_type,
is_within_tolerance
from
(select data.hub_id,
data.tracking_id,
data.scanned_date_time,
data.bag_weight,
data.machine_id,
data.hub_type,
data.machine_status,
data.weight_entry_type,
data.is_within_tolerance,
ROW_NUMBER() OVER (PARTITION BY data.tracking_id ORDER BY data.scanned_date_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as rank
from bigfoot_journal.dart_wsr_scp_ekl_weighthistory_1 ) as d1 where d1.rank=4) as d
on (d.tracking_id=x.tracking_id )

LEFT JOIN
(
select hub_id,tracking_id,scanned_date_time,bag_weight,rank ,
machine_id,
hub_type,
machine_status,
weight_entry_type,
is_within_tolerance
from
(select data.hub_id,
data.tracking_id,
data.scanned_date_time,
data.bag_weight,
data.machine_id,
data.hub_type,
data.machine_status,
data.weight_entry_type,
data.is_within_tolerance,
ROW_NUMBER() OVER (PARTITION BY data.tracking_id ORDER BY data.scanned_date_time DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as rank
from bigfoot_journal.dart_wsr_scp_ekl_weighthistory_1  ) as a1 where a1.rank=1) as dest
on dest.tracking_id=b.data.vendor_tracking_id
left join
(
select count(distinct vendor_tracking_id) as no_of_shipments_inscanned,incoming_bag_tracking_id,incoming_bag_destination_hub_id from bigfoot_external_neo.scp_ekl__shipment_facility_l2_90_fact
where shipment_facility_inscan_datetime is not null
group by incoming_bag_tracking_id,incoming_bag_destination_hub_id
) as bs
on bs.incoming_bag_tracking_id=dest.tracking_id and bs.incoming_bag_destination_hub_id=dest.hub_id
