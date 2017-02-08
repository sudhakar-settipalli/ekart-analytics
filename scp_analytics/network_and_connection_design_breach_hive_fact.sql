insert overwrite table network_and_connection_design_breach_hive_fact
select
SS.vendor_tracking_id,
if(nw_map_final.shipment_type is null and SS.source_hub_id is not null and SS.destination_hub_id is not null and FLC.shipment_first_consignment_destination_hub_id is not null,1,0) as network_design_breach_flag,
if(nw_map_final.shipment_type is not null and FLC.shipment_first_consignment_source_hub_id = SS.source_hub_id and FLC.shipment_first_consignment_destination_hub_id is not null,if(lead_time.shipment_type is null,1,0),0) as connection_design_breach_flag,
SS.source_hub_id,
SS.destination_hub_id,
lookupkey('facility_id',SS.source_hub_id) as source_hub_id_key,
lookupkey('facility_id',SS.destination_hub_id) as destination_hub_id_key,
lookup_date(SS.shipment_first_received_at_datetime) as mh_inscan_date_key,
lookup_time(SS.shipment_first_received_at_datetime) as mh_inscan_time_key,
SS.shipment_first_received_at_datetime as mh_inscan_datetime,
lookup_date(SS.customer_promise_datetime) as customer_promise_date_key,
lookup_time(SS.customer_promise_datetime) as customer_promise_time_key,
SS.customer_promise_datetime,
SS.shipment_delivered_at_datetime,
lookup_date(SS.shipment_delivered_at_datetime) as shipment_delivered_at_date_key,
if(SS.shipment_delivered_at_datetime is not null,
    if(lookup_date(SS.shipment_delivered_at_datetime) > lookup_date(SS.customer_promise_datetime),1,0)
    ,0) as cpd_breach_flag,
lookupkey('facility_id', FLC.shipment_first_consignment_destination_hub_id) as first_hop_hub_id_key,
FLC.shipment_first_consignment_destination_hub_id,
FLC.shipment_first_consignment_source_hub_id
FROM
(select
S.source_hub_id,
S.destination_hub_id,
S.shipment_type,
S.vendor_tracking_id,
S.customer_promise_datetime,
S.shipment_first_received_at_datetime,
S.shipment_delivered_at_datetime
FROM
(SELECT    
vendor_tracking_id,
If(service_tier = 'Economy Delivery' and ekl_shipment_type = 'forward','ECONOMY',
      if(ekl_shipment_type  = 'forward',
      if(shipment_dg_flag = TRUE,'DG',
        if(service_tier  = 'Next Day Delivery' or service_tier = 'Same Day Delivery','PRIORITY',
          if(service_tier = 'Standard Delivery' or service_tier is null ,'REGULAR',null
            )
          )
        ),
      'DG'
     )
    ) as shipment_type,
shipment_origin_mh_facility_id AS source_hub_id,
fsd_assigned_hub_id AS destination_hub_id,
customer_promise_datetime,
shipment_first_received_at_datetime,
shipment_delivered_at_datetime
FROM
bigfoot_external_neo.scp_ekl__shipment_l1_90_fact a 
left join 
(select facility_id,type from bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim) fac 
 on 
fac.facility_id = a.fsd_assigned_hub_id 
WHERE 
ekl_shipment_type IN ('forward') 
AND shipment_carrier IN ('FSD') 
AND shipment_current_status NOT IN ('pickup_leg_completed','pickup_leg_complete') 
AND shipment_first_received_at_datetime is not null 
AND fac.type <> 'PICKUP_CENTER' 
)S
)SS
left outer join
(
select distinct 
source_id,
destination_id,
shipment_type
from 
bigfoot_external_neo.scp_ekl__network_route_map_l1_fact
)nw_map_final
    on 
    SS.source_hub_id = nw_map_final.source_id
    and
    SS.destination_hub_id = nw_map_final.destination_id
    and
    SS.shipment_type = nw_map_final.shipment_type

LEFT OUTER JOIN 
    (SELECT
      unix_timestamp(datedim.full_date,'yyyy-MM-dd')+(floor(CAST(fl.shipment_first_consignment_create_time_key as int)/100)*3600
        +cast(substr(lpad(fl.shipment_first_consignment_create_time_key,4,'0'),3) as int)*60) as shipment_first_consignment_create_unix_seconds,
      fl.shipment_first_consignment_connection_id,
      fl.shipment_first_consignment_destination_hub_id,
      fl.vendor_tracking_id,
      fl.shipment_first_consignment_create_date_key,
      fl.shipment_first_consignment_create_time_key,
      actual_conn.`data`.source_address.id as shipment_first_consignment_source_hub_id
      FROM

       bigfoot_external_neo.scp_ekl__shipment_first_last_consignment_map_l1_90_fact fl

       LEFT OUTER JOIN 

       bigfoot_external_neo.scp_oms__date_dim_fact datedim
       ON 
          fl.shipment_first_consignment_create_date_key = datedim.date_dim_key

        LEFT OUTER JOIN 
  bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total actual_conn
  ON 
    fl.shipment_first_consignment_connection_id = actual_conn.`data`.group_id
      )FLC
    ON
   FLC.vendor_tracking_id = SS.vendor_tracking_id

   left outer join 
    (select distinct 
      `data`.source_address.id as source_id,
      `data`.destination_address.id as destination_id,
      'asdf' as shipment_type
from bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total
where 
`data`.state = "active")lead_time
    on 
    FLC.shipment_first_consignment_source_hub_id = lead_time.source_id
    and
    FLC.shipment_first_consignment_destination_hub_id = lead_time.destination_id
  ;  
