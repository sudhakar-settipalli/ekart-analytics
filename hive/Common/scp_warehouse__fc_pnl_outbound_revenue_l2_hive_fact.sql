INSERT OVERWRITE TABLE fc_pnl_outbound_revenue_l2_hive_fact 
select
inner_query.fc_pnl_outbound_warehouse  as  fc_pnl_outbound_warehouse,
inner_query.fc_pnl_outbound_product_id_key  as  fc_pnl_outbound_product_id_key,
inner_query.fc_pnl_outbound_date_key  as  fc_pnl_outbound_date_key,
inner_query.fc_pnl_outbound_single_shipment_volume  as  fc_pnl_outbound_single_shipment_volume,
inner_query.fc_pnl_outbound_processing_units  as  fc_pnl_outbound_processing_units,
inner_query.fc_pnl_outbound_processing_revenue  as  fc_pnl_outbound_processing_revenue,
inner_query.fc_pnl_outbound_freebie_units  as  fc_pnl_outbound_freebie_units,
inner_query.fc_pnl_outbound_freebie_revenue  as  fc_pnl_outbound_freebie_revenue,
inner_query.fc_pnl_outbound_fragile_units as fc_pnl_outbound_fragile_units,
inner_query.fc_pnl_outbound_fragile_revenue as fc_pnl_outbound_fragile_revenue,
inner_query.fc_pnl_outbound_destination_warehouse as fc_pnl_outbound_destination_warehouse,
inner_query.fc_pnl_outbound_processing_revenue_type as fc_pnl_outbound_processing_revenue_type
from
(
select 
a.shipment_warehouse_id as fc_pnl_outbound_warehouse,
lookupkey('product_detail_product_id',concat(a.shipment_item_product_id,a.warehouse_company)) as fc_pnl_outbound_product_id_key,
lookup_date(a.shipment_dispatched_timestamp) as fc_pnl_outbound_date_key,
max(b.single_shipment_volume) as fc_pnl_outbound_single_shipment_volume,
sum(if(a.product_cms_vertical<>'sim_card',a.shipment_item_quantity,0)) as fc_pnl_outbound_processing_units,
sum(if(a.product_cms_vertical<>'sim_card',a.shipment_item_quantity,0)*c.fc_pnl_rate_fc_outbound_processing) as fc_pnl_outbound_processing_revenue,
sum(if(a.product_cms_vertical='sim_card',a.shipment_item_quantity,0)) as fc_pnl_outbound_freebie_units,
sum(if(a.product_cms_vertical='sim_card',a.shipment_item_quantity,0)*c.fc_pnl_rate_fc_outbound_freebie) as fc_pnl_outbound_freebie_revenue,
sum(if(b.product_detail_is_fragile=1,a.shipment_item_quantity,0)) as fc_pnl_outbound_fragile_units,
sum(if(b.product_detail_is_fragile=1,a.shipment_item_quantity,0)*c.fc_pnl_rate_fc_outbound_fragility) as fc_pnl_outbound_fragile_revenue,
null as fc_pnl_outbound_destination_warehouse,
'outbound' as fc_pnl_outbound_processing_revenue_type
from bigfoot_external_neo.scp_warehouse__fc_shipment_item_l0_hive_fact as a 
left outer join bigfoot_external_neo.scp_warehouse__fc_product_detail_hive_dim as b 
  on  lookupkey('product_detail_product_id',concat(a.shipment_item_product_id,a.warehouse_company))=b.fc_product_detail_hive_dim_key
left outer join bigfoot_common.fc_pnl_rate_card as c on 
FROM_UNIXTIME(UNIX_TIMESTAMP(a.shipment_dispatched_timestamp),'yyyyMM')=c.fc_pnl_rate_month
where a.shipment_destination_type='customer' and a.shipment_dispatched_timestamp is not null and  if(b.single_shipment_volume is null,0,b.single_shipment_volume) >= c.fc_pnl_rate_fc_outbound_volume_bucket_min and
if(b.single_shipment_volume is null,0,b.single_shipment_volume) < c.fc_pnl_rate_fc_outbound_volume_bucket_max 
group by a.shipment_warehouse_id
,lookupkey('product_detail_product_id',concat(a.shipment_item_product_id,a.warehouse_company)),
lookup_date(a.shipment_dispatched_timestamp)

union all

select 
a.shipment_warehouse_id as fc_pnl_outbound_warehouse,
lookupkey('product_detail_product_id',concat(a.shipment_item_product_id,a.warehouse_company)) as fc_pnl_outbound_product_id_key,
lookup_date(a.shipment_dispatched_timestamp) as fc_pnl_outbound_date_key,
null as fc_pnl_outbound_single_shipment_volume,
sum(a.shipment_item_quantity) as fc_pnl_outbound_processing_units,
sum(a.shipment_item_quantity*mapping.outbound_cost) as fc_pnl_outbound_processing_revenue,
null as fc_pnl_outbound_freebie_units,
null as fc_pnl_outbound_freebie_revenue,
null as fc_pnl_outbound_fragile_units,
null as fc_pnl_outbound_fragile_revenue,
ore.destination_warehouse as fc_pnl_outbound_destination_warehouse,
'iwit outbound' as fc_pnl_outbound_processing_revenue_type
from bigfoot_external_neo.scp_warehouse__fc_shipment_item_l0_hive_fact as a 
left outer join bigfoot_external_neo.scp_warehouse__fc_product_detail_hive_dim as b 
  on  lookupkey('product_detail_product_id',concat(a.shipment_item_product_id,a.warehouse_company))=b.fc_product_detail_hive_dim_key
inner join 
(
select outbound_request_company,outbound_request_shipment_id,max(outbound_request_destination_party_id) as destination_warehouse
from bigfoot_external_neo.scp_warehouse__fc_outbound_requests_l0_hive_fact where outbound_request_type='iwit'
group by outbound_request_company,outbound_request_shipment_id
) ore
on (a.shipment_id=ore.outbound_request_shipment_id and a.warehouse_company=ore.outbound_request_company)
 left outer join 
(
select
month,
source,
destination,
outbound_cost
 from
 bigfoot_common.fc_pnl_iwit_cost_mapping
) mapping on (FROM_UNIXTIME(UNIX_TIMESTAMP(a.shipment_dispatched_timestamp),'yyyyMM')= mapping.month
and a.shipment_warehouse_id=mapping.source and ore.destination_warehouse  = mapping.destination)   
where a.shipment_entity_type='consignment' and a.shipment_dispatched_timestamp is not null 
group by a.shipment_warehouse_id
,lookupkey('product_detail_product_id',concat(a.shipment_item_product_id,a.warehouse_company)),
lookup_date(a.shipment_dispatched_timestamp),
ore.destination_warehouse
) inner_query
