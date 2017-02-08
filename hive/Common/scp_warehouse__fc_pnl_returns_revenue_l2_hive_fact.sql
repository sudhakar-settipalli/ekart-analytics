INSERT OVERWRITE TABLE fc_pnl_returns_revenue_l2_hive_fact 
select
a.grn_warehouse_id as fc_pnl_returns_warehouse,
lookupkey('product_detail_product_id',concat(a.grn_product_id,a.warehouse_company)) as fc_pnl_returns_product_id_key,
lookup_date(a.grn_created_at) as fc_pnl_returns_date_key,
max(b.single_shipment_volume) as fc_pnl_returns_single_shipment_volume,
sum(a.grn_quantity) as fc_pnl_returns_processing_units,
sum(a.grn_quantity*c.fc_pnl_rate_fc_returns_processing) as fc_pnl_returns_processing_revenue,
sum(if(b.product_detail_cms_vertical = 'mobile',a.grn_quantity,0)) as fc_pnl_returns_mobile_units,
sum(if(b.product_detail_cms_vertical = 'mobile' ,a.grn_quantity  ,0)*c.fc_pnl_rate_rc_returns_mobile) as fc_pnl_returns_mobile_revenue
from bigfoot_external_neo.scp_warehouse__fc_inbound_receiving_l0_hive_fact as a 
left outer join bigfoot_external_neo.scp_warehouse__fc_product_detail_hive_dim as b 
 on  lookupkey('product_detail_product_id',concat(a.grn_product_id,a.warehouse_company))=b.fc_product_detail_hive_dim_key
left outer join bigfoot_common.fc_pnl_rate_card as c on 
FROM_UNIXTIME(UNIX_TIMESTAMP(a.grn_created_at),'yyyyMM')=c.fc_pnl_rate_month
left outer join bigfoot_external_neo.scp_warehouse__fc_shipment_l1_hive_fact as d on a.grn_document_id=d.shipment_display_id
where   b.product_detail_cms_vertical<>'sim_card'  and  if(b.single_shipment_volume is null,0,b.single_shipment_volume) >= c.fc_pnl_rate_fc_inbound_volume_bucket_min and
if(b.single_shipment_volume is null,0,b.single_shipment_volume) < c.fc_pnl_rate_fc_inbound_volume_bucket_max
and a.grn_document_type ='shipment' and d.shipment_status = 'received'
group by a.grn_warehouse_id,
lookupkey('product_detail_product_id',concat(a.grn_product_id,a.warehouse_company)),
lookup_date(a.grn_created_at)
