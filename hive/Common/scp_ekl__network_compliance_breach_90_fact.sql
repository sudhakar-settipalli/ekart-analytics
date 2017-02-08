INSERT OVERWRITE TABLE network_compliance_breach_90_fact
select vendor_tracking_id,
min(network_compliance_breach_flag) as network_compliance_breach_flag,
used_first_hop,
facility_name_type_string,
lookup_date(mh_received_at_datetime) as mh_received_at_date_key,
lookup_time(mh_received_at_datetime) as mh_received_at_time_key,
mh_received_at_datetime,
source_hub_id,
destination_hub_id,
lookupkey('facility_id',source_hub_id) as source_hub_id_key,
lookupkey('facility_id',destination_hub_id) as destination_hub_id_key,
shipment_delivered_at_datetime,
customer_promise_datetime,
lookup_date(shipment_delivered_at_datetime) as shipment_delivered_at_date_key,
lookup_date(customer_promise_datetime) as customer_promise_date_key,
if(shipment_delivered_at_datetime is not null,
    if(lookup_date(shipment_delivered_at_datetime) > lookup_date(customer_promise_datetime),1,0)
    ,0) as cpd_breach_flag
from 
(
    select
    breach_wtf.vendor_tracking_id,
    if(breach_wtf.actual_connection_id <> 0 or breach_wtf.actual_connection_id is not null,
    if(find_in_set(cast(breach_wtf.actual__first_hop_id as string),lead_time.first_hop_string) = 0,1,0),0) as
    network_compliance_breach_flag,
    concat(breach_wtf.display_name,' ( ',breach_wtf.type,' ) ') as used_first_hop,
    lead_time.facility_name_type_string,
    breach_wtf.mh_received_at_datetime,
    breach_wtf.source_hub_id,
    breach_wtf.destination_hub_id,
    breach_wtf.shipment_delivered_at_datetime,
    breach_wtf.customer_promise_datetime
from 
(select 
vendor_tracking_id,
actual_connection_id,
actual__first_hop_id,
display_name,
type,
mh_received_at_datetime,
source_hub_id,
destination_hub_id,
shipment_type,
shipment_delivered_at_datetime,
customer_promise_datetime
from
(select vendor_tracking_id,
actual_connection_id,
actual__first_hop_id,
display_name,
type,
mh_received_at_datetime,
source_hub_id,
destination_hub_id,
shipment_type,
shipment_delivered_at_datetime,
customer_promise_datetime,
ROW_NUMBER() OVER (PARTITION BY vendor_tracking_id order by mh_received_at_datetime ASC
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as rank
    from 
    (select mh_breach.vendor_tracking_id,
    mh_breach.actual_connection_id,
    mh_breach.actual__first_hop_id,
    fac2.display_name,
    fac2.type,
    mh_breach.mh_received_at_datetime,
    mh_breach.source_hub_id,
    mh_breach.destination_hub_id,
    mh_breach.shipment_type,
    mh_breach.shipment_delivered_at_datetime,
    mh_breach.customer_promise_datetime
    from 
    bigfoot_external_neo.scp_ekl__sbr_mh_breach_60_fact mh_breach
    left outer join  bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim fac2
    on fac2.facility_id = mh_breach.actual__first_hop_id
    ) filter_table
)ranker
where rank =1 
)breach_wtf

    left outer join 

    (select 
        A.source_id,
        A.destination_id,
        A.shipment_type,
        A.first_hop_string,
        B.facility_name_type_string
        FROM
        (select 
        source_id,
        destination_id,
        shipment_type,
        concat_ws(',',collect_set(cast(first_hop_id as string))) as first_hop_string 
        from 
        (select 

        L.source_id,
        L.destination_id,
        L.shipment_type,
        conn.`data`.destination_address.id as first_hop_id
        FROM
        bigfoot_external_neo.scp_ekl__service_based_network_and_connection_map_fact L 
        inner join bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total conn 
        on L.conn_id = conn.`data`.group_id
        )C
        group by source_id,
            destination_id,
            shipment_type
        )A
        LEFT OUTER JOIN 
        (select 
        source_id,
        destination_id,
        shipment_type,
        concat_ws(' || ',collect_set(cast(facility_name_type as string))) as facility_name_type_string
        from 
        (select 
        L.source_id,
        L.destination_id,
        L.shipment_type,
        concat(fac.display_name,' ( ',fac.type,' ) ') as facility_name_type     
        FROM

        bigfoot_external_neo.scp_ekl__service_based_network_and_connection_map_fact L 
        inner join bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total conn
        on L.conn_id = conn.`data`.group_id
        left outer join  bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim fac
        on fac.facility_id = conn.`data`.destination_address.id
        
        )D
        group by source_id,
            destination_id,
            shipment_type
        )B
        on 
        A.source_id = B.source_id
        and 
        A.destination_id = B.destination_id
        and
        A.shipment_type =  B.shipment_type

    )lead_time
    on breach_wtf.source_hub_id = lead_time.source_id 
    and breach_wtf.destination_hub_id = lead_time.destination_id 
    and breach_wtf.shipment_type = lead_time.shipment_type
    )nw_comp_breach
group by 
vendor_tracking_id,
used_first_hop,
facility_name_type_string,
lookup_date(mh_received_at_datetime),
lookup_time(mh_received_at_datetime),
mh_received_at_datetime,
source_hub_id,
destination_hub_id,
lookupkey('facility_id',source_hub_id),
lookupkey('facility_id',destination_hub_id),
shipment_delivered_at_datetime,
customer_promise_datetime,
lookup_date(shipment_delivered_at_datetime),
lookup_date(customer_promise_datetime),
if(shipment_delivered_at_datetime is not null,
    if(lookup_date(shipment_delivered_at_datetime) > lookup_date(customer_promise_datetime),1,0)
    ,0);
