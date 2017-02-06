INSERT OVERWRITE TABLE service_based_network_and_connection_map_fact
select 
distinct
N.source_id as source_id,
N.destination_id as destination_id,
C.connection_id as conn_id,
C.cutoff as conn_cutoff,
N.ship_type as shipment_type,
1 as dg_mp_priority_flag
from 
bigfoot_external_neo.scp_ekl__service_based_routing_network_map_fact N 
inner join 
bigfoot_external_neo.scp_ekl__service_based_connections_fact C 
on N.source_id = C.source_id
and 
N.first_dest_id =C.destination_id
and 
N.ship_type = C.service_type
union all 

select 
source_id,
destination_id,
connection_id as conn_id,
cutoff as conn_cutoff,
service_type as shipment_type,
1 as dg_mp_priority_flag
from 
bigfoot_external_neo.scp_ekl__service_based_connections_fact
;
INSERT OVERWRITE TABLE service_based_network_and_connection_map_fact
select 
distinct
N.source_id as source_id,
N.destination_id as destination_id,
C.connection_id as conn_id,
C.cutoff as conn_cutoff,
N.ship_type as shipment_type,
1 as dg_mp_priority_flag
from 
bigfoot_external_neo.scp_ekl__service_based_routing_network_map_fact N 
inner join 
bigfoot_external_neo.scp_ekl__service_based_connections_fact C 
on N.source_id = C.source_id
and 
N.first_dest_id =C.destination_id
and 
N.ship_type = C.service_type

union all 

select 
source_id,
destination_id,
connection_id as conn_id,
cutoff as conn_cutoff,
service_type as shipment_type,
1 as dg_mp_priority_flag
from 
bigfoot_external_neo.scp_ekl__service_based_connections_fact a 
inner join 
(select 
conn_id
from 
(select 
`data`. source_address.id  as source_id,
`data`.destination_address.id  as destination_id,
`data`.group_id as conn_id
from  bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total
where size(`data`.associated_connections) <=0
)b
)c
on a.connection_id = c.conn_id;
