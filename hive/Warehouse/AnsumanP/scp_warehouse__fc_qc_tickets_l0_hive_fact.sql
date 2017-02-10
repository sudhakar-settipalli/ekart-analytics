INSERT OVERWRITE TABLE fc_qc_tickets_l0_hive_fact
select warehouse_company,
qc_ticket_entity_id,
qc_ticket_created_at,
qc_ticket_created_date_key,
qc_ticket_created_time_key,
qc_ticket_created_by,
goods_receipt_note_id,
qc_ticket_id,
qc_ticket_lost_quantity,
qc_ticket_product_id,
qc_reason_id,
qc_ticket_quantity,
qc_ticket_status,
qc_ticket_updated_at,
qc_ticket_updated_date_key,
qc_ticket_updated_time_key,
qc_ticket_updated_by,
qc_ticket_warehouse_id,
qc_ticket_wid,
qc_ticket_product_key,
qc_ticket_warehouse_id_key,
ticket_status_created,
ticket_created_time,
ticket_created_date_key,
ticket_created_time_key,
ticket_status_pending_reco,
ticket_pending_reco_time,
ticket_pending_reco_date_key,
ticket_pending_reco_time_key,
ticket_status_bd_escalated,
ticket_bd_escalated_time,
ticket_bd_escalated_date_key,
ticket_bd_escalated_time_key,
ticket_status_marked_4_liquidn,
ticket_marked_4_liquidn_time,
ticket_marked_4_liquidn_date_key,
ticket_marked_4_liquidn_time_key,
ticket_status_marked_4_supplier_return,
ticket_marked_4_supplier_ret_time,
ticket_marked_4_sup_ret_date_key,
ticket_marked_4_sup_ret_time_key,
ticket_status_invalid,
ticket_invalid_time,
ticket_invalid_date_key,
ticket_invalid_time_key,
ticket_final_status_lost,
ticket_lost_time,
ticket_lost_date_key,
ticket_lost_time_key,
ticket_final_status_rejected,
ticket_reject_time,
ticket_reject_date_key,
ticket_reject_time_key,
ticket_status_sent_2_liquidn,
ticket_sent_2_liquidn_time,
ticket_sent_2_liquidn_date_key,
ticket_sent_2_liquidn_time_key,
ticket_status_sent_2_supp_ret,
ticket_sent_2_supp_ret_time,
ticket_sent_2_supp_ret_date_key,
ticket_sent_2_supp_ret_time_key,
ticket_status_wh_accepted,
ticket_wh_accepted_time,
ticket_wh_accepted_date_key,
ticket_wh_accepted_time_key
from (
select 'wsr' as warehouse_company,
-- qc_ticket
qt.entityid as qc_ticket_entity_id,
qt.data.created_at as qc_ticket_created_at,
lookup_date(qt.data.created_at) as qc_ticket_created_date_key,
lookup_time(qt.data.created_at) as qc_ticket_created_time_key,
qt.data.created_by as qc_ticket_created_by,
qt.data.goods_receipt_note_id as goods_receipt_note_id,
qt.data.id as qc_ticket_id,
qt.data.lost_quantity as qc_ticket_lost_quantity,
qt.data.product_id as qc_ticket_product_id,
qt.data.qc_reason_id as qc_reason_id,
qt.data.quantity as qc_ticket_quantity,
qt.data.status as qc_ticket_status,
qt.data.updated_at as qc_ticket_updated_at,
lookup_date(qt.data.updated_at) as qc_ticket_updated_date_key,
lookup_time(qt.data.updated_at) as qc_ticket_updated_time_key,
qt.data.updated_by as qc_ticket_updated_by,
qt.data.warehouse_id as qc_ticket_warehouse_id,
qt.data.wid as qc_ticket_wid,
lookupkey('product_detail_product_id',concat(qt.data.product_id,'wsr')) as qc_ticket_product_key,
lookupkey('warehouse_id',qt.data.warehouse_id) as qc_ticket_warehouse_id_key,
-- ticket_status
ts_created.status as ticket_status_created,
ts_created.status_time as ticket_created_time,
lookup_date(ts_created.status_time) as ticket_created_date_key,
lookup_time(ts_created.status_time) as ticket_created_time_key,
ts_pending_reco.status as ticket_status_pending_reco,
ts_pending_reco.status_time as ticket_pending_reco_time,
lookup_date(ts_pending_reco.status_time) as ticket_pending_reco_date_key,
lookup_time(ts_pending_reco.status_time) as ticket_pending_reco_time_key,
ts_bd_escalated.status as ticket_status_bd_escalated,
ts_bd_escalated.status_time as ticket_bd_escalated_time,
lookup_date(ts_bd_escalated.status_time) as ticket_bd_escalated_date_key,
lookup_time(ts_bd_escalated.status_time) as ticket_bd_escalated_time_key,
ts_mfl.status as ticket_status_marked_4_liquidn,
ts_mfl.status_time as ticket_marked_4_liquidn_time,
lookup_date(ts_mfl.status_time) as ticket_marked_4_liquidn_date_key,
lookup_time(ts_mfl.status_time) as ticket_marked_4_liquidn_time_key,
ts_mfsr.status as ticket_status_marked_4_supplier_return,
ts_mfsr.status_time as ticket_marked_4_supplier_ret_time,
lookup_date(ts_mfsr.status_time) as ticket_marked_4_sup_ret_date_key,
lookup_time(ts_mfsr.status_time) as ticket_marked_4_sup_ret_time_key,
ts_inv.status as ticket_status_invalid,
ts_inv.status_time as ticket_invalid_time,
lookup_date(ts_inv.status_time) as ticket_invalid_date_key,
lookup_time(ts_inv.status_time) as ticket_invalid_time_key,
ts_lost.status as ticket_final_status_lost,
ts_lost.status_time as ticket_lost_time,
lookup_date(ts_lost.status_time) as ticket_lost_date_key,
lookup_time(ts_lost.status_time) as ticket_lost_time_key,
ts_rej.status as ticket_final_status_rejected,
ts_rej.status_time as ticket_reject_time,
lookup_date(ts_rej.status_time) as ticket_reject_date_key,
lookup_time(ts_rej.status_time) as ticket_reject_time_key,
ts_stl.status as ticket_status_sent_2_liquidn,
ts_stl.status_time as ticket_sent_2_liquidn_time,
lookup_date(ts_stl.status_time) as ticket_sent_2_liquidn_date_key,
lookup_time(ts_stl.status_time) as ticket_sent_2_liquidn_time_key,
ts_stsr.status as ticket_status_sent_2_supp_ret,
ts_stsr.status_time as ticket_sent_2_supp_ret_time,
lookup_date(ts_stsr.status_time) as ticket_sent_2_supp_ret_date_key,
lookup_time(ts_stsr.status_time) as ticket_sent_2_supp_ret_time_key,
ts_wa.status as ticket_status_wh_accepted,
ts_wa.status_time as ticket_wh_accepted_time,
lookup_date(ts_wa.status_time) as ticket_wh_accepted_date_key,
lookup_time(ts_wa.status_time) as ticket_wh_accepted_time_key
from bigfoot_snapshot.dart_wsr_scp_warehouse_qc_ticket_1_view as qt
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_wsr_scp_warehouse_ticket_status_1_0_view where data.status='created' group by data.ticket_id,data.status ) ts_created
on qt.entityid = ts_created.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_wsr_scp_warehouse_ticket_status_1_0_view where data.status='pending_reco' group by data.ticket_id,data.status ) ts_pending_reco
on qt.entityid = ts_pending_reco.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_wsr_scp_warehouse_ticket_status_1_0_view where data.status='bd_escalated' group by data.ticket_id,data.status ) ts_bd_escalated
on qt.entityid = ts_bd_escalated.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_wsr_scp_warehouse_ticket_status_1_0_view where data.status='marked_for_liquidation' group by data.ticket_id,data.status ) ts_mfl
on qt.entityid = ts_mfl.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_wsr_scp_warehouse_ticket_status_1_0_view where data.status='marked_for_supplier_return' group by data.ticket_id,data.status ) ts_mfsr
on qt.entityid = ts_mfsr.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_wsr_scp_warehouse_ticket_status_1_0_view where data.status='invalid' group by data.ticket_id,data.status ) ts_inv
on qt.entityid = ts_inv.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_wsr_scp_warehouse_ticket_status_1_0_view where data.status='lost' group by data.ticket_id,data.status ) ts_lost
on qt.entityid = ts_lost.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_wsr_scp_warehouse_ticket_status_1_0_view where data.status='rejected' group by data.ticket_id,data.status ) ts_rej
on qt.entityid = ts_rej.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_wsr_scp_warehouse_ticket_status_1_0_view where data.status='sent_to_liquidation' group by data.ticket_id,data.status ) ts_stl
on qt.entityid = ts_stl.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_wsr_scp_warehouse_ticket_status_1_0_view where data.status='sent_to_supplier_return' group by data.ticket_id,data.status ) ts_stsr
on qt.entityid = ts_stsr.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_wsr_scp_warehouse_ticket_status_1_0_view where data.status='warehouse_accepted' group by data.ticket_id,data.status ) ts_wa
on qt.entityid = ts_wa.ticket_id

UNION ALL

select 'fki' as warehouse_company,
-- qc_ticket
qt2.entityid as qc_ticket_entity_id,
qt2.data.created_at as qc_ticket_created_at,
lookup_date(qt2.data.created_at) as qc_ticket_created_date_key,
lookup_time(qt2.data.created_at) as qc_ticket_created_time_key,
qt2.data.created_by as qc_ticket_created_by,
qt2.data.goods_receipt_note_id as goods_receipt_note_id,
qt2.data.id as qc_ticket_id,
qt2.data.lost_quantity as qc_ticket_lost_quantity,
qt2.data.product_id as qc_ticket_product_id,
qt2.data.qc_reason_id as qc_reason_id,
qt2.data.quantity as qc_ticket_quantity,
qt2.data.status as qc_ticket_status,
qt2.data.updated_at as qc_ticket_updated_at,
lookup_date(qt2.data.updated_at) as qc_ticket_updated_date_key,
lookup_time(qt2.data.updated_at) as qc_ticket_updated_time_key,
qt2.data.updated_by as qc_ticket_updated_by,
qt2.data.warehouse_id as qc_ticket_warehouse_id,
qt2.data.wid as qc_ticket_wid,
lookupkey('product_detail_product_id',concat(qt2.data.product_id,'fki')) as qc_ticket_product_key,
lookupkey('warehouse_id',qt2.data.warehouse_id) as qc_ticket_warehouse_id_key,
-- ticket_status
ts_created.status as ticket_status_created,
ts_created.status_time as ticket_created_time,
lookup_date(ts_created.status_time) as ticket_created_date_key,
lookup_time(ts_created.status_time) as ticket_created_time_key,
ts_pending_reco.status as ticket_status_pending_reco,
ts_pending_reco.status_time as ticket_pending_reco_time,
lookup_date(ts_pending_reco.status_time) as ticket_pending_reco_date_key,
lookup_time(ts_pending_reco.status_time) as ticket_pending_reco_time_key,
ts_bd_escalated.status as ticket_status_bd_escalated,
ts_bd_escalated.status_time as ticket_bd_escalated_time,
lookup_date(ts_bd_escalated.status_time) as ticket_bd_escalated_date_key,
lookup_time(ts_bd_escalated.status_time) as ticket_bd_escalated_time_key,
ts_mfl.status as ticket_status_marked_4_liquidn,
ts_mfl.status_time as ticket_marked_4_liquidn_time,
lookup_date(ts_mfl.status_time) as ticket_marked_4_liquidn_date_key,
lookup_time(ts_mfl.status_time) as ticket_marked_4_liquidn_time_key,
ts_mfsr.status as ticket_status_marked_4_supplier_return,
ts_mfsr.status_time as ticket_marked_4_supplier_ret_time,
lookup_date(ts_mfsr.status_time) as ticket_marked_4_sup_ret_date_key,
lookup_time(ts_mfsr.status_time) as ticket_marked_4_sup_ret_time_key,
ts_inv.status as ticket_status_invalid,
ts_inv.status_time as ticket_invalid_time,
lookup_date(ts_inv.status_time) as ticket_invalid_date_key,
lookup_time(ts_inv.status_time) as ticket_invalid_time_key,
ts_lost.status as ticket_final_status_lost,
ts_lost.status_time as ticket_lost_time,
lookup_date(ts_lost.status_time) as ticket_lost_date_key,
lookup_time(ts_lost.status_time) as ticket_lost_time_key,
ts_rej.status as ticket_final_status_rejected,
ts_rej.status_time as ticket_reject_time,
lookup_date(ts_rej.status_time) as ticket_reject_date_key,
lookup_time(ts_rej.status_time) as ticket_reject_time_key,
ts_stl.status as ticket_status_sent_2_liquidn,
ts_stl.status_time as ticket_sent_2_liquidn_time,
lookup_date(ts_stl.status_time) as ticket_sent_2_liquidn_date_key,
lookup_time(ts_stl.status_time) as ticket_sent_2_liquidn_time_key,
ts_stsr.status as ticket_status_sent_2_supp_ret,
ts_stsr.status_time as ticket_sent_2_supp_ret_time,
lookup_date(ts_stsr.status_time) as ticket_sent_2_supp_ret_date_key,
lookup_time(ts_stsr.status_time) as ticket_sent_2_supp_ret_time_key,
ts_wa.status as ticket_status_wh_accepted,
ts_wa.status_time as ticket_wh_accepted_time,
lookup_date(ts_wa.status_time) as ticket_wh_accepted_date_key,
lookup_time(ts_wa.status_time) as ticket_wh_accepted_time_key
from bigfoot_snapshot.dart_fki_scp_warehouse_qc_ticket_1_view as qt2
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_fki_scp_warehouse_ticket_status_1_0_view where data.status='created' group by data.ticket_id,data.status ) ts_created
on qt2.entityid = ts_created.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_fki_scp_warehouse_ticket_status_1_0_view where data.status='pending_reco' group by data.ticket_id,data.status ) ts_pending_reco
on qt2.entityid = ts_pending_reco.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_fki_scp_warehouse_ticket_status_1_0_view where data.status='bd_escalated' group by data.ticket_id,data.status ) ts_bd_escalated
on qt2.entityid = ts_bd_escalated.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_fki_scp_warehouse_ticket_status_1_0_view where data.status='marked_for_liquidation' group by data.ticket_id,data.status ) ts_mfl
on qt2.entityid = ts_mfl.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_fki_scp_warehouse_ticket_status_1_0_view where data.status='marked_for_supplier_return' group by data.ticket_id,data.status ) ts_mfsr
on qt2.entityid = ts_mfsr.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_fki_scp_warehouse_ticket_status_1_0_view where data.status='invalid' group by data.ticket_id,data.status ) ts_inv
on qt2.entityid = ts_inv.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_fki_scp_warehouse_ticket_status_1_0_view where data.status='lost' group by data.ticket_id,data.status ) ts_lost
on qt2.entityid = ts_lost.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_fki_scp_warehouse_ticket_status_1_0_view where data.status='rejected' group by data.ticket_id,data.status ) ts_rej
on qt2.entityid = ts_rej.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_fki_scp_warehouse_ticket_status_1_0_view where data.status='sent_to_liquidation' group by data.ticket_id,data.status ) ts_stl
on qt2.entityid = ts_stl.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_fki_scp_warehouse_ticket_status_1_0_view where data.status='sent_to_supplier_return' group by data.ticket_id,data.status ) ts_stsr
on qt2.entityid = ts_stsr.ticket_id
left join 
(select data.ticket_id as ticket_id,data.status as status,max(data.status_time) as status_time from 
bigfoot_snapshot.dart_fki_scp_warehouse_ticket_status_1_0_view where data.status='warehouse_accepted' group by data.ticket_id,data.status ) ts_wa
on qt2.entityid = ts_wa.ticket_id) t1;
