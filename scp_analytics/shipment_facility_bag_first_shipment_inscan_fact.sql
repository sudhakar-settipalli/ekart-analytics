INSERT OVERWRITE TABLE shipment_facility_bag_first_shipment_inscan_fact
select
incoming_hop_source_type,
incoming_consignment_id,
incoming_bag_id,
min(shipment_facility_outscan_datetime) as bag_shipment_first_inscan_datetime
from bigfoot_external_neo.scp_ekl__shipment_facility_l2_90_fact 
where outgoing_consignment_source_hub_id is not null
and 
(incoming_consignment_destination_hub_id = outgoing_consignment_source_hub_id or incoming_hop_source_type ='MP_BAG')
group by incoming_consignment_id,incoming_bag_id,incoming_hop_source_type;
