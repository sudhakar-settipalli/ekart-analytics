INSERT OVERWRITE TABLE cancellation_reason_hive_dim 
select c.cancellation_reason_dim_key,c.cancellation_reason,c.cancellation_sub_reason,
if(rd.cancellation_derived_reason is not null, rd.cancellation_derived_reason,if(c.cancellation_sub_reason = 'direct_confirmed_fraud' or
c.cancellation_sub_reason = 'direct_fake_order_dexter' or
c.cancellation_sub_reason = 'direct_fake_order_ops' or
c.cancellation_sub_reason = 'direct_freebie_abuser_ops' or
c.cancellation_sub_reason = 'direct_reseller_customer' or
c.cancellation_sub_reason = 'direct_reseller_customer_FE' or
c.cancellation_sub_reason = 'direct_return_abuser_ops' or
c.cancellation_sub_reason = 'direct_toa_abuser_ops' or
c.cancellation_sub_reason = 'fake_order_ops' or
c.cancellation_sub_reason = 'pan_not_available' or
c.cancellation_sub_reason = 'direct_fraud',
'Fraud',
Null)) as cancellation_derived_reason
from (SELECT DISTINCT lookupkey('cancellation_reason',`data`.Cancellation_Reason,'cancellation_sub_reason',`data`.Cancellation_Sub_Reason) AS cancellation_reason_dim_key, `data`.Cancellation_Sub_Reason AS cancellation_sub_reason, `data`.Cancellation_Reason AS cancellation_reason from bigfoot_snapshot.dart_fkint_scp_oms_cancellation_0_3_view) c left outer join bigfoot_common.cancellation_reason_dim rd on c.cancellation_reason_dim_key=rd.cancellation_reason_dim_key;
