insert overwrite table fc_cms_vertical_estimate_l1_hive_fact
select 
product_cms_vertical,
percentile(cast((product_length * product_breadth * product_height)/0.36614 as int),0.5) as median_cms_vol_wt,
percentile(cast(box_actual_weight * 1000 as int),0.5) as median_cms_dead_wt
from bigfoot_external_neo.scp_warehouse__fc_shipment_item_l0_hive_fact 
where shipment_item_quantity = 1
and  product_length is not null
and product_breadth is not null
and product_height is not null
and (product_length * product_breadth * product_height) < 10000
group by product_cms_vertical;
