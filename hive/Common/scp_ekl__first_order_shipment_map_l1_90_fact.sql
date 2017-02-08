INSERT overwrite TABLE first_order_shipment_map_l1_90_fact

select order_item_unit_shipment_id,
order_id,
order_external_id,
order_item_date
from (
SELECT distinct a.order_item_unit_shipment_id,
       a.order_id,
       c.`data`.order_external_id AS order_external_id,
       a.order_item_date as order_item_date,
       a.order_item_timestamp,
       row_number() over (partition by a.order_item_unit_shipment_id order by a.order_item_timestamp asc) as ranking
       from
(SELECT   order_item_unit_exp.order_item_unit_shipment_id AS order_item_unit_shipment_id,
         `data`.order_id AS order_id,
         max(`data`.order_item_date) as order_item_date,
         unix_timestamp(max(`data`.order_item_date)) AS order_item_timestamp
FROM     bigfoot_snapshot.dart_fkint_scp_oms_order_item_0_11_view lateral VIEW explode(`data`.order_item_unit) exploded_table AS order_item_unit_exp
GROUP BY order_item_unit_exp.order_item_unit_shipment_id,`data`.order_id) a left outer join bigfoot_snapshot.dart_fkint_scp_oms_order_0_5_view c
ON a.order_id = c.entityid
) s
where s.ranking = 1;
