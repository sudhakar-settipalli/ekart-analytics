INSERT OVERWRITE TABLE tpt_breach_90_fact
select 
shipment_reach.vendor_tracking_id as tracking_id,
1 as breach_score,
case 
when  shipment_reach.shipment_priority_flag in ('Normal',null)
then if(unix_timestamp(coalesce(incoming_bag_destination_hub_inscan_datetime,incoming_bag_receive_datetime,from_unixtime(unix_timestamp()))) > unix_timestamp(concat_ws(' ',cast(to_date(logistics_promise_datetime) as string),'00:00:00')) + 9*3600,1,0)
when shipment_reach.shipment_priority_flag ='NDD'
then if(unix_timestamp(coalesce(incoming_bag_destination_hub_inscan_datetime,incoming_bag_receive_datetime,from_unixtime(unix_timestamp()))) > unix_timestamp(concat_ws(' ',cast(to_date(logistics_promise_datetime) as string),'00:00:00')) + 15*3600,1,0)
when shipment_reach.shipment_priority_flag ='SDD'
then if(unix_timestamp(coalesce(incoming_bag_destination_hub_inscan_datetime,incoming_bag_receive_datetime,from_unixtime(unix_timestamp()))) > unix_timestamp(concat_ws(' ',cast(to_date(logistics_promise_datetime) as string),'00:00:00')) + 17*3600,1,0)
else 0 
end as breach_flag,
coalesce(incoming_bag_destination_hub_inscan_datetime,incoming_bag_receive_datetime) as dh_first_bag_received_datetime
FROM 
(select 
vendor_tracking_id,
incoming_consignment_create_datetime,
incoming_bag_destination_hub_inscan_datetime,
incoming_bag_receive_datetime,
shipment_facility_inscan_datetime,
shipment_priority_flag,
logistics_promise_datetime
from
(select vendor_tracking_id,
incoming_consignment_create_datetime,
incoming_bag_destination_hub_inscan_datetime,
incoming_bag_receive_datetime,
shipment_facility_inscan_datetime,
shipment_priority_flag,
logistics_promise_datetime,
ROW_NUMBER() OVER (PARTITION BY vendor_tracking_id order by incoming_consignment_create_datetime ASC
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as rank
    from 
    (select vendor_tracking_id,
    incoming_consignment_create_datetime,
    incoming_bag_destination_hub_inscan_datetime,
    incoming_bag_receive_datetime,
    shipment_facility_inscan_datetime,
    shipment_priority_flag,
    logistics_promise_datetime
    from bigfoot_external_neo.scp_ekl__shipment_facility_l2_90_fact
    where shipment_carrier = 'FSD' and  shipment_current_status NOT IN ('pickup_leg_completed') and ekl_shipment_type IN ('forward','unapproved_rto','approved_rto') and incoming_consignment_destination_hub_type in ('PICKUP_CENTER','DELIVERY_HUB')
    ) filter_table
)ranker
where rank =1 
)shipment_reach;

