INSERT OVERWRITE TABLE fc_qc_ticket_item_l0_hive_fact
select
warehouse_company,
qc_ticket_item_entity_id,
qc_ticket_item_created_at,
qc_ticket_item_created_date_key,
qc_ticket_item_created_time_key,
qc_ticket_item_created_by,
qc_ticket_item_id,
qc_ticket_item_inventory_item_id,
qc_ticket_item_qc_reason_description,
qc_ticket_item_qc_sub_reason_description,
qc_ticket_id,
qc_ticket_item_qc_type,
qc_ticket_item_qc_verification_reason,
qc_ticket_item_qc_verification_sub_reason,
qc_ticket_item_shipment_item_id,
qc_ticket_item_ticket_caused_by,
qc_ticket_item_ticket_context,
qc_ticket_item_ticket_executive_note,
qc_ticket_item_ticket_qc_verification_reason,
qc_ticket_item_updated_at,
qc_ticket_item_updated_date_key,
qc_ticket_item_updated_time_key,
qc_ticket_item_updated_by,
dummy,
inventory_quantity,
inventory_item_storage_location_id_key,
warehouse_dim_key,
qc_ticket_status,
product_id_key,
grn_document_id,
grn_document_type,
irn_vendor_site_id
from 
(
select 'wsr' as warehouse_company,
qti.entityid as qc_ticket_item_entity_id,
qti.data.created_at as qc_ticket_item_created_at,
lookup_date(qti.data.created_at) as qc_ticket_item_created_date_key,
lookup_time(qti.data.created_at) as qc_ticket_item_created_time_key,
qti.data.created_by as qc_ticket_item_created_by,
qti.data.id as qc_ticket_item_id,
qti.data.inventory_item_id as qc_ticket_item_inventory_item_id,
qti.data.qc_reason_description as qc_ticket_item_qc_reason_description,
qti.data.qc_sub_reason_description as qc_ticket_item_qc_sub_reason_description,
qti.data.qc_ticket_id as qc_ticket_id,
qti.data.qc_type as qc_ticket_item_qc_type,
qti.data.qc_verification_reason as qc_ticket_item_qc_verification_reason,
qti.data.qc_verification_sub_reason as qc_ticket_item_qc_verification_sub_reason,
qti.data.shipment_item_id as qc_ticket_item_shipment_item_id,
qti.data.ticket_caused_by as qc_ticket_item_ticket_caused_by,
qti.data.ticket_context as qc_ticket_item_ticket_context,
qti.data.ticket_executive_note as qc_ticket_item_ticket_executive_note,
qti.data.ticket_qc_verification_reason as qc_ticket_item_ticket_qc_verification_reason,
qti.data.updated_at as qc_ticket_item_updated_at,
lookup_date(qti.data.updated_at) as qc_ticket_item_updated_date_key,
lookup_time(qti.data.updated_at) as qc_ticket_item_updated_time_key,
qti.data.updated_by as qc_ticket_item_updated_by,
1 as dummy,
ii.inventory_item_quantity as inventory_quantity,
ii.inventory_item_storage_location_id_key,
ii.warehouse_dim_key,
qt.qc_ticket_status,
ii.product_id_key,
grn.grn_document_id,
grn.grn_document_type,
vendor.irn_vendor_site_id
from bigfoot_snapshot.dart_wsr_scp_warehouse_qc_ticket_item_1_view as qti
left join bigfoot_external_neo.scp_warehouse__fc_inventory_item_l0_hive_fact ii
on cast(ii.inventory_item_id as bigint) = qti.data.inventory_item_id
left join bigfoot_external_neo.scp_warehouse__fc_qc_tickets_l0_hive_fact qt
on qt.qc_ticket_id=qti.data.qc_ticket_id
left join bigfoot_external_neo.scp_warehouse__fc_inbound_receiving_l0_hive_fact grn
on grn.grn_id = ii.inventory_item_grn_id
left join (select distinct irn_vendor_site_id,irn_internal_id from bigfoot_external_neo.retail_procurement__fki_wsr_irn_items_l0_fact) vendor
on vendor.irn_internal_id = grn.grn_document_id

UNION ALL

select 'fki' as warehouse_company,
qti2.entityid as qc_ticket_item_entity_id,
qti2.data.created_at as qc_ticket_item_created_at,
lookup_date(qti2.data.created_at) as qc_ticket_item_created_date_key,
lookup_time(qti2.data.created_at) as qc_ticket_item_created_time_key,
qti2.data.created_by as qc_ticket_item_created_by,
qti2.data.id as qc_ticket_item_id,
qti2.data.inventory_item_id as qc_ticket_item_inventory_item_id,
qti2.data.qc_reason_description as qc_ticket_item_qc_reason_description,
qti2.data.qc_sub_reason_description as qc_ticket_item_qc_sub_reason_description,
qti2.data.qc_ticket_id as qc_ticket_id,
qti2.data.qc_type as qc_ticket_item_qc_type,
qti2.data.qc_verification_reason as qc_ticket_item_qc_verification_reason,
qti2.data.qc_verification_sub_reason as qc_ticket_item_qc_verification_sub_reason,
qti2.data.shipment_item_id as qc_ticket_item_shipment_item_id,
qti2.data.ticket_caused_by as qc_ticket_item_ticket_caused_by,
qti2.data.ticket_context as qc_ticket_item_ticket_context,
qti2.data.ticket_executive_note as qc_ticket_item_ticket_executive_note,
qti2.data.ticket_qc_verification_reason as qc_ticket_item_ticket_qc_verification_reason,
qti2.data.updated_at as qc_ticket_item_updated_at,
lookup_date(qti2.data.updated_at) as qc_ticket_item_updated_date_key,
lookup_time(qti2.data.updated_at) as qc_ticket_item_updated_time_key,
qti2.data.updated_by as qc_ticket_item_updated_by,
1 as dummy,
ii.inventory_item_quantity as inventory_quantity,
ii.inventory_item_storage_location_id_key,
ii.warehouse_dim_key,
qt.qc_ticket_status,
ii.product_id_key,
grn.grn_document_id,
grn.grn_document_type,
vendor.irn_vendor_site_id
from bigfoot_snapshot.dart_fki_scp_warehouse_qc_ticket_item_1_view as qti2
left join bigfoot_external_neo.scp_warehouse__fc_inventory_item_l0_hive_fact ii
on cast(ii.inventory_item_id as bigint) = qti2.data.inventory_item_id
left join bigfoot_external_neo.scp_warehouse__fc_qc_tickets_l0_hive_fact qt
on qt.qc_ticket_id=qti2.data.qc_ticket_id
left join bigfoot_external_neo.scp_warehouse__fc_inbound_receiving_l0_hive_fact grn
on grn.grn_id = ii.inventory_item_grn_id
left join (select distinct irn_vendor_site_id,irn_internal_id from bigfoot_external_neo.retail_procurement__fki_wsr_irn_items_l0_fact) vendor
on vendor.irn_internal_id = grn.grn_document_id
) t1
