INSERT OVERWRITE TABLE rto_cons_l0_hive_fact
Select D.vendor_tracking_id as vendor_tracking_id,
min(D.consignment_create_datetime) as first_rto_consignment_create_time
from 
(Select vendor_tracking_id,
consignment_id,
consignment_source_hub_id,
consignment_destination_hub_id,
consignment_create_datetime,
consignment_source_hub_type,
consignment_destination_hub_type,
consignment_received_datetime,
bag_source_hub_id,
bag_assigned_hub_id,
RANK() OVER (PARTITION BY vendor_tracking_id ORDER BY consignment_create_datetime) as ranks
from 
bigfoot_external_neo.scp_ekl__tracking_shipment_hive_fact
where ekl_shipment_type IN ('approved_rto','unapproved_rto')
order by consignment_create_datetime) D

--left join
--(Select vendor_tracking_id,
--consignment_id,
--consignment_source_hub_id,
--consignment_destination_hub_id,
--consignment_create_datetime,
--consignment_source_hub_type,
--consignment_destination_hub_type,
--consignment_received_datetime,
--bag_source_hub_id,
--bag_assigned_hub_id,
--RANK() OVER (PARTITION BY vendor_tracking_id ORDER BY consignment_create_datetime ) as ranks
--from 
--bigfoot_external_neo.scp_ekl__tracking_shipment_hive_fact
--where ekl_shipment_type IN ('approved_rto','unapproved_rto')
--and vendor_tracking_id IN ('FMPP0210351953')
--order by consignment_create_datetime) E
--ON D.vendor_tracking_id=E.vendor_tracking_id
where D.consignment_destination_hub_id=D.bag_assigned_hub_id 
group by D.vendor_tracking_id;

