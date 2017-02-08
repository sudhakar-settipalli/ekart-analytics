INSERT overwrite TABLE shipment_item_l1_90_fact 
SELECT ship_entity.shipment_id, 
       ship_entity.ekl_vendor_tracking_id, 
       ship_entity.product_id, 
       ship_entity.shipment_weight, 
       ship_entity.item_quantity, 
       ship_entity.item_value, 
       ship_entity.tax_value, 
       count(ship_entity.product_id) OVER (partition BY ship_entity.ekl_vendor_tracking_id rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following)  AS count_of_shipment_items,
       sum(ship_entity.item_quantity) OVER (partition BY ship_entity.ekl_vendor_tracking_id rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) AS total_quantity,
       ship_entity.profiler_flag 
FROM   ( 
                SELECT   data.vendor_tracking_id AS ekl_vendor_tracking_id,
                         shipment_item.product_id AS product_id,
                         max(entityid) AS shipment_id,
                         max(shipment_item.quantity) AS item_quantity,
                         max(shipment_item.value.value) AS item_value,
                         max(data.shipment_items[0].tax_value.value) AS tax_value,
                         max(data.shipment_weight.physical)   AS shipment_weight,
                         max(data.shipment_weight.updated_by) AS profiler_flag
                FROM     bigfoot_snapshot.dart_wsr_scp_ekl_shipment_4_view lateral VIEW explode(data.shipment_items) exploded_table AS shipment_item
                GROUP BY data.vendor_tracking_id, 
                         shipment_item.product_id 
		) AS ship_entity
