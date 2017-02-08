INSERT OVERWRITE TABLE sbr_mh_breach_60_fact
select
B.vendor_tracking_id as vendor_tracking_id,
If(B.processing_cue_flag = 1,
  If (B.incoming_consignment_create_datetime is  null,
    0,
      If( lead_time > 5400 and (unix_timestamp (bag_history.bag_closed_datetime) - unix_timestamp(B.mh_received_at_datetime) >= lead_time-1800),
        1,
        IF(lead_time < 5400 and (unix_timestamp (bag_history.bag_closed_datetime) - unix_timestamp(B.mh_received_at_datetime) >=lead_time + 86400-1800),1,0)
      )
    ),
  If (bag_history.bag_closed_datetime is not null,
    If(unix_timestamp(bag_history.bag_closed_datetime) <= unix_timestamp(B.mh_received_at_datetime)+ B.lead_time -1800,0,
      1),
    0)
    ) as mh_breach_flag,
  If (B.processing_cue_flag = 1,
    IF(B.incoming_consignment_create_unixseconds is null,
      0,
      --IF(B.ideal_cutoff = actual_conn.`data`.cutoff,0,
        If( lead_time > 5400 and (B.incoming_consignment_create_unixseconds - unix_timestamp(B.mh_received_at_datetime) >= lead_time + 10800) ,
        1,
        IF(lead_time < 5400 and (B.incoming_consignment_create_unixseconds - unix_timestamp(B.mh_received_at_datetime) >=lead_time + 86400+10800),1,0)
      )
    ),
  IF(B.incoming_consignment_create_unixseconds is null,
    0,
    IF(B.incoming_consignment_create_unixseconds<= unix_timestamp(B.mh_received_at_datetime)+B.lead_time+10800,0,1)
      )
    ) as connection_cutoff_breach_flag,
  B.incoming_consignment_connection_id as actual_connection_id,
  B.incoming_consignment_destination_hub_id as actual__first_hop_id,
  null as current_timestamp_brach_flag,
  ideal_conn_table.ideal_first_hop_id as ideal_first_hop_id,
  ideal_conn_table.ideal_conn_id as ideal_conn_id,
  -- 0 as ideal_first_hop_id,
  -- 0 as ideal_conn_id,
unix_timestamp(bag_history.bag_closed_datetime) as shipment_first_bag_close_unixsec,
bag_history.bag_closed_datetime as first_bag_closed_datetime,
B.start_time,
B.lead_time,
B.incoming_consignment_create_unixseconds,
B.date_min_unixseconds,
B.date_max_unixseconds,
B.ideal_time_int_min,
B.ideal_time_int_max,
B.ideal_cutoff,
actual_conn.`data`.cutoff as actual_cutoff,
unix_timestamp(B.mh_received_at_datetime) as mh_received_at_unixsec,
B.mh_received_at_datetime,
B.shipment_dg_flag,
B.shipment_priority_flag,
B.shipment_fa_flag,
B.ekl_shipment_type,
B.shipment_current_status,
B.source_hub_id,
B.destination_hub_id,
B.shipment_type,
B.shipment_delivered_at_datetime,
B.customer_promise_datetime,
B.incoming_consignment_id,
B.shipment_direction_flag
FROM(
select
vendor_tracking_id,
shipment_type,
source_hub_id,
destination_hub_id,
mh_received_at_datetime,
start_time,
cutoff_range_filter,
date_range_filter,
ideal_cutoff,
lead_time,
date_min_unixseconds,
date_max_unixseconds,
ideal_time_int_min,
ideal_time_int_max,
shipment_dg_flag,
shipment_priority_flag,
shipment_fa_flag,
ekl_shipment_type,
shipment_current_status,
shipment_delivered_at_datetime,
customer_promise_datetime,
processing_cue_flag,
incoming_bag_id,
incoming_consignment_source_hub_id,
incoming_consignment_connection_id,
incoming_consignment_destination_hub_id,
incoming_consignment_create_unixseconds,
incoming_consignment_create_datetime,
incoming_consignment_id,
shipment_direction_flag
FROM(
  select
  A.vendor_tracking_id,
  A.shipment_type,
   A.shipment_dg_flag,
    A.shipment_priority_flag,
    A.shipment_fa_flag,
    A.ekl_shipment_type,
    A.shipment_current_status,
  A.source_hub_id,
  A.destination_hub_id,
  A.mh_received_at_datetime,
  A.shipment_delivered_at_datetime,
  A.customer_promise_datetime,
  A.start_time,
  if(A.start_time >=ICF.ideal_time_int_min and A.start_time <=ICF.ideal_time_int_max,1,0) as cutoff_range_filter,
  if(unix_timestamp(A.mh_received_at_datetime) >=ICF.date_min_unixseconds
    and unix_timestamp(A.mh_received_at_datetime) <=ICF.date_max_unixseconds,1,0) as date_range_filter,
  A.incoming_bag_id,
  A.incoming_consignment_source_hub_id,
  A.incoming_consignment_connection_id,
  A.incoming_consignment_destination_hub_id,
  A.incoming_consignment_create_unixseconds,
  A.incoming_consignment_create_datetime,
  A.incoming_consignment_id,
  A.shipment_direction_flag,
  ICF.cutoff as ideal_cutoff,
  pmod(cast(ICF.cutoff as int) -A.start_time + 86400,86400) as lead_time,
  ICF.date_min_unixseconds,
  ICF.date_max_unixseconds,
  ICF.ideal_time_int_min,
  ICF.ideal_time_int_max,
  ICF.processing_cue_flag

  FROM(
    SELECT
    S.vendor_tracking_id,
    S.shipment_dg_flag,
    S.shipment_fa_flag,
    S.shipment_priority_flag,
    S.ekl_shipment_type,
    S.shipment_current_status,
    S.shipment_delivered_at_datetime,
    S.customer_promise_datetime,
    If(S.service_tier = 'Economy Delivery' and S.shipment_direction_flag = 'forward','ECONOMY',
      if(S.shipment_direction_flag = 'forward',
      if(S.shipment_dg_flag = TRUE,'DG',
        if(S.service_tier  = 'Next Day Delivery' or S.service_tier = 'Same Day Delivery','PRIORITY',
          if(S.service_tier = 'Standard Delivery' or S.service_tier is null ,'REGULAR',null
            )
          )
        ),
      'DG'
     )
    ) as shipment_type,
    S.incoming_consignment_source_hub_id AS source_hub_id,
    S.incoming_consignment_destination_hub_id AS destination_hub_id,
  --modified 120 min resort
    if (S.incoming_consignment_source_hub_id = S.shipment_origin_mh_facility_id ,
        if(S.shipment_direction_flag = 'forward',
        if(upper(S.seller_type) not in ('FA','WSR'),
          S.shipment_first_received_at_datetime,
          S.shipment_first_received_at_datetime
          ),
      coalesce(S.shipment_facility_inscan_datetime,S.incoming_bag_source_hub_receive_datetime)),
      from_unixtime(unix_timestamp(coalesce(S.incoming_bag_source_hub_receive_datetime,S.shipment_facility_inscan_datetime))+3600)
      ) as  mh_received_at_datetime,
    (cast(hour(if (S.incoming_consignment_source_hub_id = S.shipment_origin_mh_facility_id ,
        if(S.shipment_direction_flag = 'forward',
        if(upper(S.seller_type) not in ('FA','WSR'),
          S.shipment_first_received_at_datetime,
          S.shipment_first_received_at_datetime
          ),
      coalesce(S.shipment_facility_inscan_datetime,S.incoming_bag_source_hub_receive_datetime)),
      from_unixtime(unix_timestamp(coalesce(S.incoming_bag_source_hub_receive_datetime,S.shipment_facility_inscan_datetime))+3600)
      )) as int)*3600 + cast(minute(if (S.incoming_consignment_source_hub_id = S.shipment_origin_mh_facility_id ,
        if(S.shipment_direction_flag = 'forward',
        if(upper(S.seller_type) not in ('FA','WSR'),
          S.shipment_first_received_at_datetime,
          S.shipment_first_received_at_datetime
          ),
      coalesce(S.shipment_facility_inscan_datetime,S.incoming_bag_source_hub_receive_datetime)),
      from_unixtime(unix_timestamp(coalesce(S.incoming_bag_source_hub_receive_datetime,S.shipment_facility_inscan_datetime))+3600)
      )) as int)*60) as start_time,
    S.incoming_bag_id,
    S.incoming_consignment_source_hub_id,
    S.incoming_consignment_connection_id,
    S.incoming_consignment_destination_hub_id,
    S.incoming_consignment_create_unixseconds,
    S.incoming_consignment_create_datetime,
    S.incoming_consignment_id,
    S.shipment_direction_flag
    FROM
    bigfoot_external_neo.scp_ekl__shipment_facility_l2_90_fact S
    WHERE 
     S.shipment_carrier IN ('FSD') 
    AND S.shipment_current_status NOT IN ('pickup_leg_completed','pickup_leg_complete') 
    AND S.shipment_facility_inscan_datetime is not null
    AND S.incoming_consignment_source_hub_id is not null
    AND 
    S.incoming_consignment_source_hub_id = S.incoming_bag_source_hub_id 
    )A
  inner join 
  (select source_id,
    destination_id,
    shipment_type,
    ideal_time_int_min,
    cutoff,
    ideal_time_int_max,
    date_min_unixseconds,
    date_max_unixseconds,
    processing_cue_flag
  FROM
  bigfoot_external_neo.scp_ekl__service_based_routing_ideal_connection_90_fact) ICF
  on 
  concat_ws('-',cast(A.source_hub_id as string),
    cast(A.destination_hub_id as string),cast(A.shipment_type as string)) = 
  concat_ws('-',cast(ICF.source_id as string),cast(ICF.destination_id as string),cast(ICF.shipment_type as string)
  ) )C 
  WHERE
  cutoff_range_filter = 1 
  and 
  date_range_filter = 1
  )B
LEFT OUTER JOIN 
(SELECT
CAST( SPLIT(inner_bag.entityid, "-")[1] AS INT) AS bag_id,
inner_bag.location_id,
CAST(inner_bag.first_open_time AS TIMESTAMP) AS bag_open_datetime,
CAST(inner_bag.first_close_time AS TIMESTAMP) as bag_closed_datetime,
CAST(inner_bag.bag_first_connected_time AS TIMESTAMP) as bag_connected_datetime,
CAST(inner_bag.bag_destination_hub_inscan_time AS TIMESTAMP) as bag_destination_hub_inscan_datetime,
CAST(inner_bag.bag_receive_time AS TIMESTAMP) AS bag_receive_datetime
FROM
(SELECT entityid,
       `data`.current_location.id as location_id,
       MIN(IF(`data`.status='OPEN', updatedat, NULL)) AS first_open_time,
       MIN(IF(`data`.status='CLOSED', updatedat, NULL)) AS first_close_time,
       MIN(IF(`data`.status='INTRANSIT', updatedat, NULL)) AS bag_first_connected_time,
       MIN(IF(`data`.status='REACHED', updatedat, NULL)) AS bag_destination_hub_inscan_time,
       MIN(IF(`data`.status='RECEIVED', updatedat, NULL)) AS bag_receive_time
FROM bigfoot_journal.dart_wsr_scp_ekl_shipmentgroup_3 WHERE lower(`data`.type) = 'bag'
and  day  > date_format(date_sub(current_date,100),'yyyyMMdd')
GROUP BY entityid,
`data`.current_location.id
) inner_bag 
)bag_history
on 
B.incoming_bag_id = bag_history.bag_id 
and 
B.incoming_consignment_source_hub_id = bag_history.location_id 

LEFT OUTER JOIN 
  bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total actual_conn
  ON 
    B.incoming_consignment_connection_id = actual_conn.`data`.group_id

LEFT OUTER JOIN
(
select asdf.source_id,
asdf.destination_id,
asdf.shipment_type,
asdf.conn_cutoff,
asdf.conn_id as ideal_conn_id,
ideal_conn.`data`.destination_address.id as ideal_first_hop_id
FROM
(select source_id,destination_id,shipment_type,conn_cutoff,max(conn_id) as conn_id
from bigfoot_external_neo.scp_ekl__service_based_network_and_connection_map_fact
group by source_id,destination_id,shipment_type,conn_cutoff )asdf
inner join 
bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total ideal_conn
  ON 
    asdf.conn_id = ideal_conn.`data`.group_id
) ideal_conn_table
ON 
B.source_hub_id = ideal_conn_table.source_id
and
B.destination_hub_id = ideal_conn_table.destination_id
and
B.shipment_type = ideal_conn_table.shipment_type
and
B.ideal_cutoff = ideal_conn_table.conn_cutoff;
