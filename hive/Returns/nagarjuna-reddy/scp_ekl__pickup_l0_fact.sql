insert overwrite table pickup_l0_fact 
select
pi.`data`.ref_entity_id as ref_entity_id,
--pi.`data`.tenant_id as  tenant_id, -- exists in pickup also
pi.`data`.quantity as pickup_quantity,
pi.`data`.qty_qc_passed as qty_qc_passed,
pi.`data`.pickup_item_id as pickup_item_id,
--pi.`data`.created_at -- exists in pickup also
pi.`data`.quantity_received_in_wh as quantity_received_in_wh,
--pi.`data`.pickup_id -- exists in pickup also
pi.`data`.quantity_picked as quantity_picked,
--pi.`data`.updated_at -- exists in pickup also
pi.`data`.ref_entity as ref_entity,
pi.`data`.seller_id as seller_id,
lookupkey('seller_id', pi.`data`.seller_id) AS seller_id_key , -- Newly addes feild
pi.`data`.status as pickup_status,
pi.`data`.qty_qc_failed as qty_qc_failed,
p.`data`.tenant_id as tenant_id,
p.`data`.pickup_from_address_id as pickup_from_address_id,
p.`data`.pickup_to_address_id as pickup_to_address_id,
p.`data`.promise_date as pickup_promise_date,
lookup_date(p.`data`.promise_date) AS pickup_promise_date_key ,  -- Newly addes feild
p.`data`.created_at as pickup_request_date,
lookup_date(p.`data`.created_at) AS pickup_request_date_key ,  -- Newly addes feild
p.`data`.shipment_id as pickup_tracking_id,
p.`data`.customer_will_return as customer_will_return,
p.`data`.pickup_id as pickup_id,
p.`data`.pickup_to_party_id as pickup_to_party_id,
p.`data`.latched_shipment_id as latched_shipment_id,
p.`data`.courier_name as courier_name,
p.`data`.updated_at,
p.`data`.pickup_from_party_id as pickup_from_party_id,
--p.`data`.seller_id
p.`data`.tracking_id as pickup_shipment_id,
pr.`data`.order_item_id as order_item_id,
pr.`data`.product_id as product_id,
pr.`data`.category_id as category_id,
pr.`data`.title as title,
pr.`data`.notional_value as notional_value,
concat(pr.`data`.reverse_action,'_',pr.`data`.forward_action) as actions,
p.`data`.latched_shipment_id as reference_id,
p.`data`.pickup_to_address_id as return_to_address_id


from 
bigfoot_snapshot.dart_fkint_scp_ro_pickup_item_1_view pi
left outer join
bigfoot_snapshot.dart_fkint_scp_ro_pickup_1_view p
on pi.`data`.pickup_id = p.`data`.pickup_id
left join bigfoot_snapshot.dart_fkint_scp_ro_product_exchange_item_1_view pr
on pi.`data`.ref_entity_id=pr.`data`.product_exchange_item_id
where pi.`data`.ref_entity='product_exchange_item';
