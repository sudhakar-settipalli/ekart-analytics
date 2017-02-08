INSERT OVERWRITE TABLE smartpickup_l1_fact
select distinct
rvp_rto_det.shipmentid as return_shipment_id,
lookup_date(rvp_rto_det.createdat) as pickup_creation_date_key,
lookup_time(rvp_rto_det.createdat) as pickup_creation_time_key,
rvp_rto_det.agentid as agent_id,
lookupkey('agent_id',rvp_rto_det.agentid) as agent_id_key,
rvp_rto_det.type as reverse_type,
rvp_rto_det.mode as checkedby,
rvp_rto_det.pickupreasoncode as customer_return_reason,
rvp_rto_det.pickupsubreasoncode as customer_return_subreason,
if(size(rvp_rto_det.smartpickupreportlist) > 0,'y','n') as checks_done,
if(lower(rvp_rto_det.shipmentstatus) = 'pickup_picked_complete' and rvp_rto_det.csoverride = false,'no_issues',rvp_rto_det.shipmentstatusreason) as verification_result,
if(rvp_rto_det.rvphelplinedialed is not null,'y','n') as rvp_helpline_dialed,
if(rvp_rto_det.csoverride = true,'y','n') as cs_override,
rvp_rto_det.pickupreasondescription as pickup_status_description,
case when upper(rvp_rto_det.pickupreasoncode)='MISSHIPMENT' and upper(rvp_rto_det.pickupsubreasoncode) ='ORDERED_A_RECEIVED_B' then 'NOT_DONE' 
when rvp_rto_det.verification_status = true then 'PASS' 
when rvp_rto_det.verification_status = false then 'FAIL' 
else 'NOT_DONE' END as verification_status,
rvp_rto_det.shipmentstatus as latest_status,
-- external_order_id is made as null because source team stopped ingesting data into the column dart_fkint_scp_rrr_return_4_0_view.'external_order_id'
-- other_return_det.external_order_id as order_id,
null as order_id,

other_det.original_tracking_id as forward_tracking_id,
lookupkey('facility_id',rvp_rto_det.hub_id) as hub_id_key,
other_det.agent_id_key as fe_id_key,
other_det.actioned_runsheet_id as pickupsheet_id,
other_det.shipment_rvp_pk_number_of_attempts as pickup_no_of_attempts,
other_det.shipment_value as shipment_value,
other_det.reverse_shipment_type as reverse_shipment_type,
other_det.payment_type as payment_type,
other_det.shipment_weight as shipment_weight,
other_det.seller_type as seller_type,
other_det.hub_notes as hub_notes,
other_det.shipment_destination_facility_id_key as shipment_destination_facility_id_key,
other_det.item_quantity as item_count,
other_return_det.return_id as return_id,  -- no changes made in source
-- return_product_category_name is made as null because source team stopped ingesting data into the column dart_fkint_scp_rrr_return_item_3_0_view.'return_product_category_name'
--other_return_det.return_product_category_name as return_product_category_name,
null as return_product_category_name,

if(lower(substr(rvp_rto_det.shipmentid,1,3)) = 'myn','myntra','flipkart') as courier_bucket,
-- returned_product_id_key is made as null because source team stopped ingesting data into the column dart_fkint_scp_rrr_return_item_3_0_view.'returned_product_id_key'
-- return_product_title is made as null because source team stopped ingesting data into the column dart_fkint_scp_rrr_return_item_3_0_view.'return_product_title'

-- other_return_det.returned_product_id_key as returned_product_id_key,
-- other_return_det.return_product_title  as return_product_title,

null as returned_product_id_key,
null  as return_product_title,

-- return_status is made as return_item_status in dart_fkint_scp_rrr_return_item_3_0_view
-- other_return_det.return_status as return_status,
other_return_det.return_item_status as return_status,

rvp_rto_det.smartpickupreportlist[0].reportname as check1_report_name,
case when upper(rvp_rto_det.smartpickupreportlist[0].reportstatus) ='MATCHED' then 'PASS' 
when upper(rvp_rto_det.smartpickupreportlist[0].reportstatus) ='UNMATCHED' then 'FAILED'
when upper(rvp_rto_det.smartpickupreportlist[0].reportstatus) ='NOTVERIFIED' then 'NOT_DONE'
else rvp_rto_det.smartpickupreportlist[0].reportstatus END as check1_report_status,
rvp_rto_det.smartpickupreportlist[0].reportvalue as check1_report_value,
rvp_rto_det.smartpickupreportlist[1].reportname as check2_report_name,
case when upper(rvp_rto_det.smartpickupreportlist[1].reportstatus) ='MATCHED' then 'PASS' 
when upper(rvp_rto_det.smartpickupreportlist[1].reportstatus) ='UNMATCHED' then 'FAILED'
when upper(rvp_rto_det.smartpickupreportlist[1].reportstatus) ='NOTVERIFIED' then 'NOT_DONE'
else rvp_rto_det.smartpickupreportlist[1].reportstatus END as check2_report_status,
rvp_rto_det.smartpickupreportlist[1].reportvalue as check2_report_value,
rvp_rto_det.smartpickupreportlist[2].reportname as check3_report_name,
case when upper(rvp_rto_det.smartpickupreportlist[2].reportstatus) ='MATCHED' then 'PASS' 
when upper(rvp_rto_det.smartpickupreportlist[2].reportstatus) ='UNMATCHED' then 'FAILED'
when upper(rvp_rto_det.smartpickupreportlist[2].reportstatus) ='NOTVERIFIED' then 'NOT_DONE'
else rvp_rto_det.smartpickupreportlist[2].reportstatus END as check3_report_status,
rvp_rto_det.smartpickupreportlist[2].reportvalue as check3_report_value,
rvp_rto_det.smartpickupreportlist[3].reportname as check4_report_name,
case when upper(rvp_rto_det.smartpickupreportlist[3].reportstatus) ='MATCHED' then 'PASS' 
when upper(rvp_rto_det.smartpickupreportlist[3].reportstatus) ='UNMATCHED' then 'FAILED'
when upper(rvp_rto_det.smartpickupreportlist[3].reportstatus) ='NOTVERIFIED' then 'NOT_DONE'
else rvp_rto_det.smartpickupreportlist[3].reportstatus END as check4_report_status,
rvp_rto_det.smartpickupreportlist[3].reportvalue as check4_report_value,
rvp_rto_det.smartpickupreportlist[4].reportname as check5_report_name,
case when upper(rvp_rto_det.smartpickupreportlist[4].reportstatus) ='MATCHED' then 'PASS' 
when upper(rvp_rto_det.smartpickupreportlist[4].reportstatus) ='UNMATCHED' then 'FAILED'
when upper(rvp_rto_det.smartpickupreportlist[4].reportstatus) ='NOTVERIFIED' then 'NOT_DONE'
else rvp_rto_det.smartpickupreportlist[4].reportstatus END as check5_report_status,
rvp_rto_det.smartpickupreportlist[4].reportvalue as check5_report_value,
rvp_rto_det.smartpickupreportlist[5].reportname as check6_report_name,
case when upper(rvp_rto_det.smartpickupreportlist[5].reportstatus) ='MATCHED' then 'PASS' 
when upper(rvp_rto_det.smartpickupreportlist[5].reportstatus) ='UNMATCHED' then 'FAILED'
when upper(rvp_rto_det.smartpickupreportlist[5].reportstatus) ='NOTVERIFIED' then 'NOT_DONE'
else rvp_rto_det.smartpickupreportlist[5].reportstatus END as check6_report_status,
rvp_rto_det.smartpickupreportlist[5].reportvalue as check6_report_value,
rvp_rto_det.smartpickupreportlist[6].reportname as check7_report_name,
case when upper(rvp_rto_det.smartpickupreportlist[6].reportstatus) ='MATCHED' then 'PASS' 
when upper(rvp_rto_det.smartpickupreportlist[6].reportstatus) ='UNMATCHED' then 'FAILED'
when upper(rvp_rto_det.smartpickupreportlist[6].reportstatus) ='NOTVERIFIED' then 'NOT_DONE'
else rvp_rto_det.smartpickupreportlist[6].reportstatus END as check7_report_status,
rvp_rto_det.smartpickupreportlist[6].reportvalue as check7_report_value,
rvp_rto_det.smartpickupreportlist[7].reportname as check8_report_name,
case when upper(rvp_rto_det.smartpickupreportlist[7].reportstatus) ='MATCHED' then 'PASS' 
when upper(rvp_rto_det.smartpickupreportlist[7].reportstatus) ='UNMATCHED' then 'FAILED'
when upper(rvp_rto_det.smartpickupreportlist[7].reportstatus) ='NOTVERIFIED' then 'NOT_DONE'
else rvp_rto_det.smartpickupreportlist[7].reportstatus END as check8_report_status,
rvp_rto_det.smartpickupreportlist[7].reportvalue as check8_report_value,
rvp_rto_det.smartpickupreportlist[8].reportname as check9_report_name,
case when upper(rvp_rto_det.smartpickupreportlist[8].reportstatus) ='MATCHED' then 'PASS' 
when upper(rvp_rto_det.smartpickupreportlist[8].reportstatus) ='UNMATCHED' then 'FAILED'
when upper(rvp_rto_det.smartpickupreportlist[8].reportstatus) ='NOTVERIFIED' then 'NOT_DONE'
else rvp_rto_det.smartpickupreportlist[8].reportstatus END as check9_report_status,
rvp_rto_det.smartpickupreportlist[8].reportvalue as check9_report_value,
rvp_rto_det.smartpickupreportlist[9].reportname as check10_report_name,
case when upper(rvp_rto_det.smartpickupreportlist[9].reportstatus) ='MATCHED' then 'PASS' 
when upper(rvp_rto_det.smartpickupreportlist[9].reportstatus) ='UNMATCHED' then 'FAILED'
when upper(rvp_rto_det.smartpickupreportlist[9].reportstatus) ='NOTVERIFIED' then 'NOT_DONE'
else rvp_rto_det.smartpickupreportlist[9].reportstatus END as check10_report_status,
rvp_rto_det.smartpickupreportlist[9].reportvalue as check10_report_value,
rvp_rto_det.smartpickupreportlist[10].reportname as check11_report_name,
case when upper(rvp_rto_det.smartpickupreportlist[10].reportstatus) ='MATCHED' then 'PASS' 
when upper(rvp_rto_det.smartpickupreportlist[10].reportstatus) ='UNMATCHED' then 'FAILED'
when upper(rvp_rto_det.smartpickupreportlist[10].reportstatus) ='NOTVERIFIED' then 'NOT_DONE'
else rvp_rto_det.smartpickupreportlist[10].reportstatus END as check11_report_status,
rvp_rto_det.smartpickupreportlist[10].reportvalue as check11_report_value,
rvp_rto_det.smartpickupreportlist[11].reportname as check12_report_name,
case when upper(rvp_rto_det.smartpickupreportlist[11].reportstatus) ='MATCHED' then 'PASS' 
when upper(rvp_rto_det.smartpickupreportlist[11].reportstatus) ='UNMATCHED' then 'FAILED'
when upper(rvp_rto_det.smartpickupreportlist[11].reportstatus) ='NOTVERIFIED' then 'NOT_DONE'
else rvp_rto_det.smartpickupreportlist[11].reportstatus END as check12_report_status,
rvp_rto_det.smartpickupreportlist[11].reportvalue as check12_report_value,
rvp_rto_det.smartpickupreportlist[12].reportname as check13_report_name,
case when upper(rvp_rto_det.smartpickupreportlist[12].reportstatus) ='MATCHED' then 'PASS' 
when upper(rvp_rto_det.smartpickupreportlist[12].reportstatus) ='UNMATCHED' then 'FAILED'
when upper(rvp_rto_det.smartpickupreportlist[12].reportstatus) ='NOTVERIFIED' then 'NOT_DONE'
else rvp_rto_det.smartpickupreportlist[12].reportstatus END as check13_report_status,
rvp_rto_det.smartpickupreportlist[12].reportvalue as check13_report_value,
rvp_rto_det.smartpickupreportlist[13].reportname as check14_report_name,
case when upper(rvp_rto_det.smartpickupreportlist[13].reportstatus) ='MATCHED' then 'PASS' 
when upper(rvp_rto_det.smartpickupreportlist[13].reportstatus) ='UNMATCHED' then 'FAILED'
when upper(rvp_rto_det.smartpickupreportlist[13].reportstatus) ='NOTVERIFIED' then 'NOT_DONE'
else rvp_rto_det.smartpickupreportlist[13].reportstatus END as check14_report_status,
rvp_rto_det.smartpickupreportlist[13].reportvalue as check14_report_value,
rvp_rto_det.smartpickupreportlist[14].reportname as check15_report_name,
case when upper(rvp_rto_det.smartpickupreportlist[14].reportstatus) ='MATCHED' then 'PASS' 
when upper(rvp_rto_det.smartpickupreportlist[14].reportstatus) ='UNMATCHED' then 'FAILED'
when upper(rvp_rto_det.smartpickupreportlist[14].reportstatus) ='NOTVERIFIED' then 'NOT_DONE'
else rvp_rto_det.smartpickupreportlist[14].reportstatus END as check15_report_status,
rvp_rto_det.smartpickupreportlist[14].reportvalue as check15_report_value
FROM  (
select `data`.shipmentid as shipmentid,
`data`.type as type,
`data`.csoverride as csoverride,
`data`.shipmentstatusreason as shipmentstatusreason,
`data`.createdat as createdat,
`data`.smartpickupreportlist as smartpickupreportlist,
`data`.pickupreasondescription as pickupreasondescription,
`data`.rvphelplinedialed as rvphelplinedialed,
`data`.pickupsubreasoncode as pickupsubreasoncode,
`data`.pickupreasoncode as pickupreasoncode,
`data`.shipmentstatus as shipmentstatus, 
`data`.agentid as agentid,
`data`.mode as mode,
`data`.verificationstatus as verification_status,
`data`.hubid as hub_id,
row_number() OVER(PARTITION BY `data`.shipmentid ORDER BY `data`.createdat desc) as rnk
from bigfoot_journal.dart_wsr_scp_ekl_pickupdetails_3_4 where `data`.type IN  ('RVP','RTO') and upper(`data`.mode)='ERP' 
UNION ALL
select `data`.shipmentid as shipmentid,
`data`.type as type,
`data`.csoverride as csoverride,
`data`.shipmentstatusreason as shipmentstatusreason,
`data`.createdat as createdat,
`data`.smartpickupreportlist as smartpickupreportlist,
`data`.pickupreasondescription as pickupreasondescription,
`data`.rvphelplinedialed as rvphelplinedialed,
`data`.pickupsubreasoncode as pickupsubreasoncode,
`data`.pickupreasoncode as pickupreasoncode,
`data`.shipmentstatus as shipmentstatus, 
`data`.agentid as agentid,
`data`.mode as mode,
`data`.verificationstatus as verification_status,
`data`.hubid as hub_id,
row_number() OVER(PARTITION BY `data`.shipmentid ORDER BY `data`.createdat desc) as rnk
from bigfoot_journal.dart_wsr_scp_ekl_pickupdetails_3_4 where `data`.type IN  ('RVP','RTO') and upper(`data`.mode)='LM_APP') rvp_rto_det
left join bigfoot_external_neo.scp_ekl__shipment_hive_90_fact other_det
ON rvp_rto_det.shipmentid = other_det.vendor_tracking_id
left join bigfoot_external_neo.scp_rrr__return_l1_fact other_return_det
ON other_return_det.return_item_shipment_id = rvp_rto_det.shipmentid
where rvp_rto_det.rnk=1;
