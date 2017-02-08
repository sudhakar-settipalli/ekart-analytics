INSERT OVERWRITE TABLE fc_pnl_monthly_revenue_l4_hive_fact
select
e.fc_pnl_warehouse as fc_pnl_warehouse,
e.fc_pnl_month as fc_pnl_month,
e.fc_pnl_units as fc_pnl_units,
e.fc_pnl_revenue_type as fc_pnl_revenue_type,
e.fc_pnl_revenue as fc_pnl_revenue,
e.fc_pnl_freebie_units as fc_pnl_freebie_units,
e.fc_pnl_freebie_revenue as fc_pnl_freebie_revenue,
e.fc_pnl_fragile_units as fc_pnl_fragile_units,
e.fc_pnl_fragile_revenue as fc_pnl_fragile_revenue,
e.fc_pnl_returns_mobile_units as fc_pnl_returns_mobile_units,
e.fc_pnl_returns_mobile_revenue as fc_pnl_returns_mobile_revenue,
e.fc_pnl_storage_space_utilized_in_cubic_feet as fc_pnl_storage_space_utilized_in_cubic_feet,
e.fc_pnl_commitment_outbound_inbound_units as fc_pnl_commitment_outbound_inbound_units,
e.fc_pnl_commitment_inventory_fc_rc_storage as fc_pnl_commitment_inventory_fc_rc_storage,
e.fc_pnl_commitment_outbound_inbound_revenue as fc_pnl_commitment_outbound_inbound_revenue,
e.fc_pnl_commitment_inventory_fc_rc_storage_revenue as fc_pnl_commitment_inventory_fc_rc_storage_revenue,
 if(
e.fc_pnl_revenue_type in ('fc storage','rc storage'), 
if(e.fc_pnl_revenue>=e.fc_pnl_commitment_inventory_fc_rc_storage_revenue,e.fc_pnl_revenue,e.fc_pnl_commitment_inventory_fc_rc_storage_revenue),
 nvl(e.fc_pnl_revenue,0) + nvl(e.fc_pnl_freebie_revenue,0) + nvl(e.fc_pnl_fragile_revenue,0) + nvl(e.fc_pnl_returns_mobile_revenue,0) + 
 nvl(e.fc_pnl_commitment_outbound_inbound_revenue,0)
) as fc_pnl_total_revenue
from
(select
d.fc_pnl_warehouse as fc_pnl_warehouse,
d.fc_pnl_month as fc_pnl_month,
d.fc_pnl_units as fc_pnl_units,
d.fc_pnl_revenue_type as fc_pnl_revenue_type,
d.fc_pnl_revenue as fc_pnl_revenue,
d.fc_pnl_freebie_units as fc_pnl_freebie_units,
d.fc_pnl_freebie_revenue as fc_pnl_freebie_revenue,
d.fc_pnl_fragile_units as fc_pnl_fragile_units,
d.fc_pnl_fragile_revenue as fc_pnl_fragile_revenue,
d.fc_pnl_returns_mobile_units as fc_pnl_returns_mobile_units,
d.fc_pnl_returns_mobile_revenue as fc_pnl_returns_mobile_revenue,
d.fc_pnl_storage_space_utilized_in_cubic_feet as fc_pnl_storage_space_utilized_in_cubic_feet,
d.fc_pnl_commitment_outbound_inbound_units as fc_pnl_commitment_outbound_inbound_units,
d.fc_pnl_commitment_inventory_fc_rc_storage as fc_pnl_commitment_inventory_fc_rc_storage,
if(d.fc_pnl_revenue_type='outbound', c.fc_pnl_rate_fc_outbound_under_utilization * 
if((nvl(d.fc_pnl_commitment_outbound_inbound_units,0)-nvl(d.fc_pnl_units,0))>0,(nvl(d.fc_pnl_commitment_outbound_inbound_units,0)-nvl(d.fc_pnl_units,0)),0),
if(d.fc_pnl_revenue_type='inbound',c.fc_pnl_rate_fc_inbound_under_utilization * if((nvl(d.fc_pnl_commitment_outbound_inbound_units,0)-nvl(d.fc_pnl_units,0))>0,
(nvl(d.fc_pnl_commitment_outbound_inbound_units,0)-nvl(d.fc_pnl_units,0)),0),null)) as fc_pnl_commitment_outbound_inbound_revenue,
d.fc_pnl_commitment_inventory_fc_rc_storage*c.fc_pnl_rate_inventory_storage as fc_pnl_commitment_inventory_fc_rc_storage_revenue
from
(
select
a.fc_pnl_warehouse as fc_pnl_warehouse,
a.fc_pnl_month as fc_pnl_month,
a.fc_pnl_units as  fc_pnl_units,
a.fc_pnl_revenue_type as fc_pnl_revenue_type,
a.fc_pnl_revenue as fc_pnl_revenue,
a.fc_pnl_freebie_units as fc_pnl_freebie_units,
a.fc_pnl_freebie_revenue as fc_pnl_freebie_revenue,
a.fc_pnl_fragile_units as fc_pnl_fragile_units,
a.fc_pnl_fragile_revenue as fc_pnl_fragile_revenue,
a.fc_pnl_returns_mobile_units as fc_pnl_returns_mobile_units,
a.fc_pnl_returns_mobile_revenue as fc_pnl_returns_mobile_revenue,
a.fc_pnl_storage_space_utilized_in_cubic_feet as fc_pnl_storage_space_utilized_in_cubic_feet,
if(a.fc_pnl_revenue_type='outbound',b.fc_pnl_commitment_outbound_units, if(a.fc_pnl_revenue_type='inbound',b.fc_pnl_commitment_inbound_units,null))
 as fc_pnl_commitment_outbound_inbound_units , 
if(a.fc_pnl_revenue_type='fc storage',b.fc_pnl_commitment_inventory_fc_storage, if(a.fc_pnl_revenue_type='rc storage',b.fc_pnl_commitment_inventory_rc_storage,null)) 
as fc_pnl_commitment_inventory_fc_rc_storage
from 
(
select
dr.fc_pnl_warehouse as fc_pnl_warehouse,
substring((dr.fc_pnl_date_key),1,6) as fc_pnl_month,
 sum(dr.fc_pnl_units) as  fc_pnl_units,
dr.fc_pnl_revenue_type as fc_pnl_revenue_type,
 sum(dr.fc_pnl_revenue) as fc_pnl_revenue,
 sum(dr.fc_pnl_freebie_units) as fc_pnl_freebie_units,
 sum(dr.fc_pnl_freebie_revenue) as fc_pnl_freebie_revenue,
 sum(dr.fc_pnl_fragile_units) as fc_pnl_fragile_units,
 sum(dr.fc_pnl_fragile_revenue) as fc_pnl_fragile_revenue,
 sum(dr.fc_pnl_returns_mobile_units) as fc_pnl_returns_mobile_units,
 sum(dr.fc_pnl_returns_mobile_revenue) as fc_pnl_returns_mobile_revenue,
sum(dr.fc_pnl_storage_space_utilized_in_cubic_feet) as fc_pnl_storage_space_utilized_in_cubic_feet
from
bigfoot_external_neo.scp_warehouse__fc_pnl_daily_revenue_l3_hive_fact as dr
group by 
dr.fc_pnl_warehouse
,substring((dr.fc_pnl_date_key),1,6)
, dr.fc_pnl_revenue_type 
) a
left outer join bigfoot_common.fc_pnl_cp_commitments as b on 
a.fc_pnl_warehouse=b.fc_pnl_commitment_warehouse
 and a.fc_pnl_month = concat(substr(b.fc_pnl_commitment_month,3,4),'0',substr(b.fc_pnl_commitment_month,1,1))

)
 d 
left outer join 
(
select 
f.fc_pnl_rate_month
 as fc_pnl_rate_month
,
max(f.fc_pnl_rate_fc_outbound_under_tutilization) as fc_pnl_rate_fc_outbound_under_utilization,
max(f.fc_pnl_rate_fc_inbound_under_tutilization) as fc_pnl_rate_fc_inbound_under_utilization,
max(f.fc_pnl_rate_inventory_storage) as fc_pnl_rate_inventory_storage
from bigfoot_common.fc_pnl_rate_card as f
group by f.fc_pnl_rate_month
)
 c 
on d.fc_pnl_month =c.fc_pnl_rate_month
) e ;
