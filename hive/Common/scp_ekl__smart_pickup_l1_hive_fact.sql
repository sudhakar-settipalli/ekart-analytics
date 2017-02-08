INSERT OVERWRITE TABLE smart_pickup_l1_hive_fact
select 
A.`data`.subitemid,
A.`data`.shipmentid,
A.`data`.shipmentitemid,
B.ekl_shipment_type,
B.reverse_shipment_type,
B.shipment_value,
B.seller_type,
B.ekl_fin_zone,
B.shipment_carrier,
B.shipment_rvp_pk_number_of_attempts,
B.source_address_pincode,
B.customer_address_id,
B.fsd_assigned_hub_id,
B.reverse_pickup_hub_id,
B.shipment_first_received_dh_id,
B.shipment_last_received_dh_id,
B.shipment_current_status_datetime,
B.shipment_delivered_at_datetime,
B.shipment_first_dispatched_to_merchant_datetime,
B.fsd_first_dh_received_datetime,
B.shipment_first_rvp_pickup_time,
B.rto_complete_datetime,
B.first_mh_tc_receive_datetime,
B.last_mh_tc_receive_datetime,
B.shipment_created_at_datetime,
A.`data`.agentid,
A.`data`.fetimestamp,
if(lower(A.`data`.rvphelplinedialed)='true',1,0) as rvphelplinedialed,
A.`data`.subreasoncode,
A.`data`.tltimestamp,
if(lower(A.`data`.csoverride)='true',0,1) as csoverride,
A.`data`.shipmentstatusreason,
if(lower(A.`data`.tlverificationresult)='true',1,if(lower(A.`data`.tlverificationresult)='false',0,null)) as outertlresult,
if(lower(A.`data`.feverificationresult)='true',1,if(lower(A.`data`.feverificationresult)='false',0,null)) as outerferesult,
A.`data`.subitemname,
A.`data`.sheetid,
A.`data`.reasoncode,
A.`data`.teamleaderldap,
A.`data`.returntype,
A.`data`.shipmentstatuscode,
if(lower(A.feresult)='true',1,if(lower(A.feresult)='false',0,null)) as feresult,
--the last 0 here should ideally be null here
--*/
A.inputs,
if(lower(A.tlresult)='true',1,if(lower(A.tlresult)='false',0,null)) as tlresult,
A.checkname,
A.checkid,
if(lower(A.ismandatory)='true',1,if(lower(A.ismandatory)='false',0,null)) as ismandatory,
C.mis_shipment_filter as mis_shipment_filter,
C.wsn_display_id as wsn_display_id,
C.pv_max_numdate as pv_max_numdate,
C.pv_max_num_date_key as pv_max_num_date_key,
C.pv_max_num_time_key as pv_max_num_time_key,
C.pv_bucket as pv_bucket,
C.product_id_key as product_id_key,
C.seller_id as seller_id,
C.return_reason as return_reason,
C.return_sub_reason as return_sub_reason,
C.detailed_pv_reasons as detailed_pv_reasons,
C.detailed_pv_sub_reasons as detailed_pv_sub_reasons,
C.initial_pv_done_by as initial_pv_done_by,
C.return_type as return_type,
C.inventory_item_wid as inventory_item_wid,
C.re_pv_done_by as re_pv_done_by,
C.order_id as order_id,
if(A.`data`.feverificationresult='false' and A.`data`.csoverride='false' and C.pv_max_numdate is not null,1,0) as pk_wo_cs_or_afr_fail,
if(C.mis_shipment_filter='Mis-shipment' and EF.rrr_reason NOT IN ('MISSHIPMENT','ITEM_NOT_AS_DESCRIBED'),1,0) as reverse_misshipment,
if(C.return_type='prexo_return' and C.wh_storage_location_max='fraud_bulk' and C.serial_number <> C.prexo_initial_imei_sl_number_captured,1,0) as imei_mismatch,

if(A.checkname='C_ICLOUD_LOCK_CHECK',
if(C.detailed_pv_sub_reasons LIKE ('%IS_ICLOUD_LOCKED%'),1,0),
if(A.checkname='D_BODY_DAMAGE_CHECK_LAPTOP_PREXO',
if(C.detailed_pv_sub_reasons LIKE ('%BODY_DAMAGED%') or C.detailed_pv_sub_reasons LIKE ('%PREXO_PRODUCT_DAMAGED%') or C.detailed_pv_sub_reasons LIKE ('%DENTS_ON_CABINET_BODY%'),1,0),
if(A.checkname='D_DAMAGE_CTH_CHECK',
if(C.detailed_pv_sub_reasons LIKE ('%BODY_DAMAGED%') or C.detailed_pv_sub_reasons LIKE ('%LACESOLE_DAMAGED%') or C.detailed_pv_sub_reasons LIKE ('%BROKEN_PRODUCT%') or C.detailed_pv_sub_reasons LIKE ('%PRODUCT_TORN%') or C.detailed_pv_sub_reasons LIKE ('%DAMAGED_PRODUCT_RECEIVED%') or C.detailed_pv_sub_reasons LIKE ('%TORNHOLECUT_ON_FABRIC%'),1,0),
if(A.checkname='D_MAJOR_BODY_DAMAGE_CHECK',
if(C.detailed_pv_sub_reasons LIKE ('%BODY_DAMAGED%') or C.detailed_pv_sub_reasons LIKE ('%PREXO_PRODUCT_DAMAGED%') or C.detailed_pv_sub_reasons LIKE ('%DENTS_ON_CABINET_BODY%'),1,0),
if(A.checkname='D_SCREEN_DAMAGE_CHECK',
if(C.detailed_pv_sub_reasons LIKE ('%SCREEN_DAMAGED%') or C.detailed_pv_sub_reasons LIKE ('%PREXO_PRODUCT_DAMAGED%'),1,0),
if(A.checkname='D_STAIN_CHECK',
if(C.detailed_pv_sub_reasons LIKE ('%MAJ%STAINS_ON_PRODUCT%') or C.detailed_pv_sub_reasons LIKE ('%STAIN_ON_PRODUCT%') or C.detailed_pv_sub_reasons IN ('STAIN_ON_PRODUCT'),1,0),
if(A.checkname='D_SWITCH_ON_CHECK',
if(C.detailed_pv_sub_reasons LIKE ('%NOT_SWITCHING_ON%'),1,0),
if(A.checkname='M_PRODUCT_IMAGE_COLOR_PATTERN_MATCH',
if(C.detailed_pv_sub_reasons LIKE ('%BOTH_PIECES_DONT_MATCH%'),1,0),
if(A.checkname='P_AVAILABILITY_CHECK',
if(C.detailed_pv_sub_reasons LIKE ('%MAIN_PRODUCT_MISSIN%'),1,0),
if(A.checkname='P_BOX_AVAILABLE_CHECK',
if(C.detailed_pv_sub_reasons LIKE ('%NO_PACKAGING%'),1,0),
if(A.checkname='P_BOX_CONDITION_CHECK',
if(C.detailed_pv_sub_reasons LIKE ('%MAJOR_BOX_DAMAGE%') or C.detailed_pv_sub_reasons LIKE ('%OUTER_PACKAGING_DAMAGED%') or C.detailed_pv_sub_reasons LIKE ('%NON_REPAIRABLE_MAJOR_ISSUE_ON_PACKAGING%'),1,0),
if(A.checkname='P_MRP_VALUE_CHECK',
if(C.detailed_pv_sub_reasons LIKE ('%MRP_TAG_MISSING%') or C.detailed_pv_sub_reasons LIKE ('%MRP_NOT_VISIBLE%'),1,0),
if(A.checkname='P_TAG_AVAILABLE_CHECK',
if(C.detailed_pv_sub_reasons LIKE ('%MRP_TAG_MISSING%'),1,0),0))))))))))))) as is_match,
D.primary_product_key,
EF.rrr_reason,
EF.rrr_sub_reason,
EF.return_type,
B.shipment_current_status
from 
(select 
data,
checks.feresult as feresult,
checks.inputs as inputs,
checks.tlresult as tlresult,
checks.checkname as checkname,
checks.checkid as checkid,
checks.ismandatory as ismandatory
from bigfoot_snapshot.dart_wsr_scp_ekl_smartpickupdetail_1_view lateral view explode(`data`.checkinfolist) checklist as checks) a 
left join bigfoot_external_neo.scp_ekl__shipment_l1_90_fact B 
on A.`data`.shipmentid=B.vendor_tracking_id 
left join bigfoot_external_neo.scp_ekl__qc_liquidation_l2_hive_fact C 
on A.`data`.shipmentid=C.shipmentid
left join 
( select vendor_tracking_id,primary_product_key from bigfoot_external_neo.scp_ekl__shipment_hive_90_fact
where ekl_shipment_type='rvp' and reverse_shipment_type<>'PREXO' and vendor_tracking_id NOT IN ('not_assigned') and vendor_tracking_id is not null
) D on  A.`data`.shipmentid=D.vendor_tracking_id
left join
( select 
return_item_shipment_id as rrr_shipmentid,
return_type,
max(return_item_reason) as rrr_reason,
max(return_item_sub_reason) as rrr_sub_reason
from bigfoot_external_neo.scp_rrr__return_l1_fact
group by 
return_item_shipment_id,
return_type
) EF ON A.`data`.shipmentid=EF.rrr_shipmentid


