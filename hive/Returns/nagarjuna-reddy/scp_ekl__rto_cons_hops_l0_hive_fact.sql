INSERT OVERWRITE TABLE rto_cons_hops_l0_hive_fact
Select vendor_tracking_id,
consignment_id,
consignment_source_hub_id,
consignment_destination_hub_id,
consignment_create_datetime,
consignment_source_hub_type,
consignment_destination_hub_type,
consignment_received_datetime,
bag_source_hub_id,
bag_assigned_hub_id

--RANK() OVER (PARTITION BY vendor_tracking_id ORDER BY consignment_create_datetime ) as ranks
from 
bigfoot_external_neo.scp_ekl__tracking_shipment_hive_fact
where ekl_shipment_type IN ('approved_rto','unapproved_rto')
order by consignment_create_datetime;
