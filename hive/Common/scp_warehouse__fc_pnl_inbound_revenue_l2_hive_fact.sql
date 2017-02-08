 INSERT OVERWRITE TABLE fc_pnl_inbound_revenue_l2_hive_fact 
 select
inner_query.fc_pnl_inbound_warehouse as fc_pnl_inbound_warehouse, 
inner_query.fc_pnl_inbound_product_id_key as fc_pnl_inbound_product_id_key,
inner_query.fc_pnl_inbound_date_key as fc_pnl_inbound_date_key,
inner_query.fc_pnl_inbound_product_volume as fc_pnl_inbound_product_volume,
inner_query.fc_pnl_inbound_processing_units as fc_pnl_inbound_processing_units, 
inner_query.fc_pnl_inbound_processing_revenue as fc_pnl_inbound_processing_revenue,
inner_query.fc_pnl_inbound_processing_revenue_type as fc_pnl_inbound_processing_revenue_type
from
 (
 select
a.grn_warehouse_id as fc_pnl_inbound_warehouse,lookupkey('product_detail_product_id',
concat(a.grn_product_id,a.warehouse_company)) as fc_pnl_inbound_product_id_key,
lookup_date(a.grn_created_at) as fc_pnl_inbound_date_key,
max(b.product_detail_volume) as fc_pnl_inbound_product_volume,
sum(a.grn_quantity) as fc_pnl_inbound_processing_units,
sum(a.grn_quantity*c.fc_pnl_rate_fc_inbound_processing) as fc_pnl_inbound_processing_revenue,
'inbound' as fc_pnl_inbound_processing_revenue_type
from bigfoot_external_neo.scp_warehouse__fc_inbound_receiving_l0_hive_fact as a 
left outer join bigfoot_external_neo.scp_warehouse__fc_product_detail_hive_dim as b 
 on  lookupkey('product_detail_product_id',concat(a.grn_product_id,a.warehouse_company))=b.fc_product_detail_hive_dim_key
left outer join bigfoot_common.fc_pnl_rate_card as c on 
FROM_UNIXTIME(UNIX_TIMESTAMP(a.grn_created_at),'yyyyMM')=c.fc_pnl_rate_month
where  b.product_detail_cms_vertical<>'sim_card' and   if(b.product_detail_volume is null,0,b.product_detail_volume) >= c.fc_pnl_rate_fc_inbound_volume_bucket_min and
if(b.product_detail_volume is null,0,b.product_detail_volume) < c.fc_pnl_rate_fc_inbound_volume_bucket_max
and a.grn_document_type in ('irn','fbf_asn')
group by a.grn_warehouse_id,
lookupkey('product_detail_product_id',concat(a.grn_product_id,a.warehouse_company)),
lookup_date(a.grn_created_at) 

union all

select
a.grn_warehouse_id as fc_pnl_inbound_warehouse,lookupkey('product_detail_product_id',
concat(a.grn_product_id,a.warehouse_company)) as fc_pnl_inbound_product_id_key,
lookup_date(a.grn_created_at) as fc_pnl_inbound_date_key,
null as fc_pnl_inbound_product_volume,
sum(a.grn_quantity) as fc_pnl_inbound_processing_units,
sum(a.grn_quantity * mapping.inbound_cost) as fc_pnl_inbound_processing_revenue,
'iwit inbound' as fc_pnl_inbound_processing_revenue_type
from bigfoot_external_neo.scp_warehouse__fc_inbound_receiving_l0_hive_fact as a 
left outer join bigfoot_external_neo.scp_warehouse__fc_product_detail_hive_dim as b 
 on  lookupkey('product_detail_product_id',concat(a.grn_product_id,a.warehouse_company))=b.fc_product_detail_hive_dim_key
 left outer join 
 (
 select
 month,
 destination,
 max(inbound_cost) as inbound_cost
 from
 bigfoot_common.fc_pnl_iwit_cost_mapping
 group by  
 month,
 destination
 ) mapping on (FROM_UNIXTIME(UNIX_TIMESTAMP(a.grn_created_at),'yyyyMM')= mapping.month
and a.grn_warehouse_id=mapping.destination)   
inner join 
(
select 
distinct inbound_request_document_id
from bigfoot_external_neo.scp_warehouse__fc_inbound_request_item_l0_hive_fact 
where inbound_request_type='iwit'
) iri
on (a.grn_document_id=iri.inbound_request_document_id )
where a.grn_document_type = 'Consignment'
group by a.grn_warehouse_id,
lookupkey('product_detail_product_id',concat(a.grn_product_id,a.warehouse_company)),
lookup_date(a.grn_created_at)  
 ) inner_query 
