
INSERT OVERWRITE TABLE returns_breach_funnel_l3_hive_fact

select distinct 
A.vendor_tracking_id as vendor_tracking_id,
A.rvp_schedule_date_key as rvp_schedule_date_key,
A.promised_completion_date_key as promised_completion_date_key,
A.rto_complete_date_key as actual_completion_date_key,
A.seller_type as seller_type,
A.reverse_shipment_type as reverse_shipment_type,
A.shipment_current_status as shipment_current_status,
A.ekl_fin_zone as ekl_fin_zone,
B.ekl_shipment_type as ekl_shipment_type,
if(B.no_of_attempts_customer_dependency > 0, 1 ,0) as customer_dependency_flag, 
if(B.no_of_attempts_ekl_dependency > 0, 1 ,0) as ekl_dependency_flag,
A.pickup_added_sunday_flag as pickup_added_sunday_flag, 
A.dh_breach_flag as dh_breach_flag,
A.transport_breach_flag as transport_breach_flag,
A.mh_breach_flag as mh_breach_flag,
A.rc_or_ph_breach_flag as rc_or_ph_breach_flag,
A.e2e_breach_flag as e2e_breach_flag,
A.pickup_time_in_hours as pickup_time_in_hours,
A.dh_processing_time_in_hours as dh_processing_time_in_hours,
A.tc_dh_to_1st_mh_time_in_hours as tc_dh_to_1st_mh_time_in_hours,
A.first_mh_to_cons_time_in_hours as first_mh_to_cons_time_in_hours,
A.tc_1stmh_to_last_mh_time_in_hours as tc_1stmh_to_last_mh_time_in_hours,
A.last_mh_to_cons_time_in_hours as last_mh_to_cons_time_in_hours,
A.tc_last_mh_to_ph_time_in_hours as tc_last_mh_to_ph_time_in_hours,
A.rcin_or_ph_to_complete_time_in_hours as rcin_or_ph_to_complete_time_in_hours,
C.facility_id as first_dh_hub_id_key,
C.display_name as first_dh_hub_name,
C.city as first_dh_hub_city,
D.facility_id as first_mh_hub_id_key,
D.display_name as first_mh_hub_name,
D.city as first_mh_hub_city,
if(D.facility_id = E.facility_id,' ',E.facility_id) as last_mh_hub_id_key,
if(D.facility_id = E.facility_id,' ',E.display_name) as last_mh_hub_name,
if(D.facility_id = E.facility_id,' ',E.city) as last_mh_hub_city,
if(C.facility_id = F.facility_id,' ',F.facility_id) as last_dh_hub_id_key,
if(C.facility_id = F.facility_id,' ',F.display_name) as last_dh_hub_name,
if(C.facility_id = F.facility_id,' ',F.city) as last_dh_hub_city,


A.rvp_start_date1 as rvp_start_date1,
A.first_received_dh_date1 as first_received_dh_date1,
A.first_cons_create_dh_date1 as first_cons_create_dh_date1,
A.first_cons_create_dh_receive_date1 as first_cons_create_dh_receive_date1,
A.first_received_mh_date1 as first_received_mh_date1,
A.first_cons_create_mh_date1 first_cons_create_mh_date1,
A.sec_last_cons_create_mh_receive_date1 as sec_last_cons_create_mh_receive_date1,
A.last_received_mh_date1 as last_received_mh_date1,
A.last_cons_create_mh_date1 as last_cons_create_mh_date1,
A.last_cons_create_mh_receive_date1 as last_cons_create_mh_receive_date1,
A.last_received_dh_date1 as last_received_dh_date1,
A.rvp_complete_date1 as rvp_complete_date1,
A.rvp_schedule_date_key as rvp_schedule_date_key,
A.rvp_schedule_time_key as rvp_schedule_time_key,
A.dh_in_date_key as dh_in_date_key,
A.dh_in_time_key as dh_in_time_key,
A.dh_out_date_key as dh_out_date_key,
A.dh_out_time_key as dh_out_time_key,
A.first_mh_in_date_key as first_mh_in_date_key,
A.first_mh_in_time_key as first_mh_in_time_key,
A.first_mh_out_date_key as first_mh_out_date_key,
A.first_mh_out_time_key as first_mh_out_time_key,
A.last_mh_in_date_key as last_mh_in_date_key,
A.last_mh_in_time_key as last_mh_in_time_key,
A.last_mh_out_date_key as last_mh_out_date_key,
A.last_mh_out_time_key as last_mh_out_time_key,
A.phin_or_mh_shipment_out_date_key as phin_or_mh_shipment_out_date_key,
A.phin_or_mh_shipment_out_time_key as phin_or_mh_shipment_out_time_key,
A.rvp_complete_date_key as rvp_complete_date_key,
A.rvp_complete_time_key as rvp_complete_time_key

FROM 

(

select
T2.vendor_tracking_id,
T2.shipment_id,
T2.first_received_dh_id_key,
T2.first_received_dh_date_key,
T2.first_received_dh_time_key,
T2.first_received_mh_id_key,
T2.first_received_mh_date_key,
T2.first_received_mh_time_key,
T2.last_received_dh_id_key,
T2.last_received_dh_date_key,
T2.last_received_dh_time_key,
T2.last_received_mh_id_key,
T2.last_received_mh_date_key,
T2.last_received_mh_time_key,
T2.first_cons_create_dh_id_key,
T2.first_cons_create_dh_dest_id_key,
T2.first_cons_create_dh_date_key,
T2.first_cons_create_dh_time_key,
T2.first_cons_create_dh_receive_date_key,
T2.first_cons_create_dh_receive_time_key,
T2.first_cons_create_mh_id_key,
T2.first_cons_create_mh_date_key,
T2.first_cons_create_mh_time_key,
T2.last_cons_create_mh_id_key,
T2.last_cons_create_mh_dest_id_key,
T2.last_cons_create_mh_date_key,
T2.last_cons_create_mh_time_key,
T2.last_cons_create_mh_receive_date_key,
T2.last_cons_create_mh_receive_time_key,
T2.sec_last_cons_create_mh_id_key,
T2.sec_last_cons_create_mh_dest_id_key,
T2.sec_last_cons_create_mh_date_key,
T2.sec_last_cons_create_mh_time_key,
T2.sec_last_cons_create_mh_receive_date_key,
T2.sec_last_cons_create_mh_receive_time_key,
T2.shipment_current_status,
T2.seller_type,
T2.ekl_shipment_type,
T2.reverse_shipment_type,
T2.ekl_fin_zone,
T2.ekart_lzn_flag,
T2.shipment_rvp_pk_number_of_attempts,
T2.fsd_number_of_ofd_attempts,
T2.shipment_value,
T2.fsd_assigned_hub_id_key,
T2.reverse_pickup_hub_id_key,
T2.shipment_current_hub_id_key,
T2.shipment_current_hub_type,
T2.shipment_created_at_date_key,
T2.shipment_created_at_time_key,
T2.first_mh_tc_receive_date_key,
T2.first_mh_tc_receive_time_key,
T2.last_mh_tc_receive_date_key,
T2.last_mh_tc_receive_time_key,
T2.actual_rvp_pickup_date_key,
T2.actual_rvp_pickup_time_key,
T2.rto_complete_date_key,
T2.rto_complete_time_key,
T2.rvp_start_date1,
T2.first_received_dh_date1,
T2.first_cons_create_dh_date1,
T2.first_cons_create_dh_receive_date1,
T2.first_received_mh_date1,
T2.first_cons_create_mh_date1,
T2.sec_last_cons_create_mh_receive_date1,
T2.last_received_mh_date1,
T2.last_cons_create_mh_date1,
T2.last_cons_create_mh_receive_date1,
T2.last_received_dh_date1,
T2.rvp_complete_date1,
T2.rvp_schedule_date_key,
T2.rvp_schedule_time_key,
T2.dh_in_date_key,
T2.dh_in_time_key,
T2.dh_out_date_key,
T2.dh_out_time_key,
T2.first_mh_in_date_key,
T2.first_mh_in_time_key,
T2.first_mh_out_date_key,
T2.first_mh_out_time_key,
T2.last_mh_in_date_key,
T2.last_mh_in_time_key,
T2.last_mh_out_date_key,
T2.last_mh_out_time_key,
T2.phin_or_mh_shipment_out_date_key,
T2.phin_or_mh_shipment_out_time_key,
T2.rvp_complete_date_key,
T2.rvp_complete_time_key,

T2.pickup_time_in_hours,
T2.dh_processing_time_in_hours,
T2.tc_dh_to_1st_mh_time_in_hours,
T2.first_mh_to_cons_time_in_hours,
T2.tc_1stmh_to_last_mh_time_in_hours,
T2.last_mh_to_cons_time_in_hours,
T2.tc_last_mh_to_ph_time_in_hours,
T2.rcin_or_ph_to_complete_time_in_hours,
T2.promised_date,
lookup_date(T2.promised_date)  as promised_completion_date_key,
T2.pickup_added_sunday_flag,

if((T2.pickup_time_in_hours + T2.dh_processing_time_in_hours) < 48, 0, 1) as dh_breach_flag,

CASE  
when T2.ekl_fin_zone = 'INTRACITY' 
then
if( T2.seller_type = 'FA' or  T2.seller_type = 'WSR',
if(
if(T2.tc_dh_to_1st_mh_time_in_hours is NULL, 0, T2.tc_dh_to_1st_mh_time_in_hours) +
if(T2.tc_1stmh_to_last_mh_time_in_hours is NULL, 0, T2.tc_1stmh_to_last_mh_time_in_hours) +
if(T2.tc_last_mh_to_ph_time_in_hours is NULL, 0, T2.tc_last_mh_to_ph_time_in_hours) 
< 18, 0, 1),

if(
if(T2.tc_dh_to_1st_mh_time_in_hours is NULL, 0, T2.tc_dh_to_1st_mh_time_in_hours) +
if(T2.tc_1stmh_to_last_mh_time_in_hours is NULL, 0, T2.tc_1stmh_to_last_mh_time_in_hours) +
if(T2.tc_last_mh_to_ph_time_in_hours is NULL, 0, T2.tc_last_mh_to_ph_time_in_hours) 
< 18, 0, 1)
)

when T2.ekl_fin_zone = 'INTRAZONE' 
then
if( T2.seller_type = 'FA' or  T2.seller_type = 'WSR',
if(
if(T2.tc_dh_to_1st_mh_time_in_hours is NULL, 0, T2.tc_dh_to_1st_mh_time_in_hours) +
if(T2.tc_1stmh_to_last_mh_time_in_hours is NULL, 0, T2.tc_1stmh_to_last_mh_time_in_hours) +
if(T2.tc_last_mh_to_ph_time_in_hours is NULL, 0, T2.tc_last_mh_to_ph_time_in_hours) 
< 48, 0, 1),

if(
if(T2.tc_dh_to_1st_mh_time_in_hours is NULL, 0, T2.tc_dh_to_1st_mh_time_in_hours) +
if(T2.tc_1stmh_to_last_mh_time_in_hours is NULL, 0, T2.tc_1stmh_to_last_mh_time_in_hours) +
if(T2.tc_last_mh_to_ph_time_in_hours is NULL, 0, T2.tc_last_mh_to_ph_time_in_hours) 
< 60, 0, 1)
)

when T2.ekl_fin_zone = 'INTERZONE' 
then 
if( T2.seller_type = 'FA' or  T2.seller_type = 'WSR',
if(
if(T2.tc_dh_to_1st_mh_time_in_hours is NULL, 0, T2.tc_dh_to_1st_mh_time_in_hours) +
if(T2.tc_1stmh_to_last_mh_time_in_hours is NULL, 0, T2.tc_1stmh_to_last_mh_time_in_hours) +
if(T2.tc_last_mh_to_ph_time_in_hours is NULL, 0, T2.tc_last_mh_to_ph_time_in_hours) 
< 108, 0, 1),

if(
if(T2.tc_dh_to_1st_mh_time_in_hours is NULL, 0, T2.tc_dh_to_1st_mh_time_in_hours) +
if(T2.tc_1stmh_to_last_mh_time_in_hours is NULL, 0, T2.tc_1stmh_to_last_mh_time_in_hours) +
if(T2.tc_last_mh_to_ph_time_in_hours is NULL, 0, T2.tc_last_mh_to_ph_time_in_hours) 
< 120, 0, 1)
)
END as transport_breach_flag,

CASE  
when T2.ekl_fin_zone = 'INTRACITY' 
then 
if( T2.seller_type = 'FA' or  T2.seller_type = 'WSR',
if(
if(T2.first_mh_to_cons_time_in_hours is NULL, 0, T2.first_mh_to_cons_time_in_hours) +
if(T2.last_mh_to_cons_time_in_hours is NULL, 0, T2.last_mh_to_cons_time_in_hours)
< 24, 0, 1),

if(
if(T2.first_mh_to_cons_time_in_hours is NULL, 0, T2.first_mh_to_cons_time_in_hours) +
if(T2.last_mh_to_cons_time_in_hours is NULL, 0, T2.last_mh_to_cons_time_in_hours)
< 24, 0, 1)
)

when T2.ekl_fin_zone = 'INTRAZONE' 
then
if( T2.seller_type = 'FA' or  T2.seller_type = 'WSR',
if(
if(T2.first_mh_to_cons_time_in_hours is NULL, 0, T2.first_mh_to_cons_time_in_hours) +
if(T2.last_mh_to_cons_time_in_hours is NULL, 0, T2.last_mh_to_cons_time_in_hours)
< 48, 0, 1),

if(
if(T2.first_mh_to_cons_time_in_hours is NULL, 0, T2.first_mh_to_cons_time_in_hours) +
if(T2.last_mh_to_cons_time_in_hours is NULL, 0, T2.last_mh_to_cons_time_in_hours)
< 48, 0, 1)
)
when T2.ekl_fin_zone = 'INTERZONE' 
then 
if( T2.seller_type = 'FA' or  T2.seller_type = 'WSR',
if(
if(T2.first_mh_to_cons_time_in_hours is NULL, 0, T2.first_mh_to_cons_time_in_hours) +
if(T2.last_mh_to_cons_time_in_hours is NULL, 0, T2.last_mh_to_cons_time_in_hours)
< 48, 0, 1),

if(
if(T2.first_mh_to_cons_time_in_hours is NULL, 0, T2.first_mh_to_cons_time_in_hours) +
if(T2.last_mh_to_cons_time_in_hours is NULL, 0, T2.last_mh_to_cons_time_in_hours)
< 48, 0, 1)
)
END as mh_breach_flag,

if(T2.seller_type = 'FA' or  T2.seller_type = 'WSR',
if(T2.rcin_or_ph_to_complete_time_in_hours < 6, 0, 1),
if(T2.rcin_or_ph_to_complete_time_in_hours < 24, 0, 1)) as rc_or_ph_breach_flag,

if(T2.shipment_current_status NOT IN ('PICKUP_Cancelled'),
if(T2.rto_complete_date_key is NULL, 
if(lookup_date(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1)) <= lookup_date(T2.promised_date),0,1),
if(T2.rto_complete_date_key <= lookup_date(T2.promised_date),0,1)
), 0 
) as e2e_breach_flag

FROM
(
select 
T1.vendor_tracking_id,
T1.shipment_id,
T1.first_received_dh_id_key,
T1.first_received_dh_date_key,
T1.first_received_dh_time_key,
T1.first_received_pc_id_key,
T1.first_received_pc_date_key,
T1.first_received_pc_time_key,
T1.first_received_mh_id_key,
T1.first_received_mh_date_key,
T1.first_received_mh_time_key,
T1.second_received_mh_id_key,
T1.second_received_mh_date_key,
T1.second_received_mh_time_key,
T1.third_received_mh_id_key,
T1.third_received_mh_date_key,
T1.third_received_mh_time_key,
T1.fourth_received_mh_id_key,
T1.fourth_received_mh_date_key,
T1.fourth_received_mh_time_key,
T1.last_received_dh_id_key,
T1.last_received_dh_date_key,
T1.last_received_dh_time_key,
T1.last_received_mh_id_key,
T1.last_received_mh_date_key,
T1.last_received_mh_time_key,
T1.second_last_received_mh_id_key,
T1.second_last_received_mh_date_key,
T1.second_last_received_mh_time_key,
T1.first_cons_create_dh_id_key,
T1.first_cons_create_dh_dest_id_key,
T1.first_cons_create_dh_date_key,
T1.first_cons_create_dh_time_key,
T1.first_cons_create_dh_receive_date_key,
T1.first_cons_create_dh_receive_time_key,
T1.first_cons_create_mh_id_key,
T1.first_cons_create_mh_dest_id_key,
T1.first_cons_create_mh_date_key,
T1.first_cons_create_mh_time_key,
T1.first_cons_create_mh_receive_date_key,
T1.first_cons_create_mh_receive_time_key,
T1.second_cons_create_mh_id_key,
T1.second_cons_create_mh_dest_id_key,
T1.second_cons_create_mh_date_key,
T1.second_cons_create_mh_time_key,
T1.second_cons_create_mh_receive_date_key,
T1.second_cons_create_mh_receive_time_key,
T1.third_cons_create_mh_id_key,
T1.third_cons_create_mh_dest_id_key,
T1.third_cons_create_mh_date_key,
T1.third_cons_create_mh_time_key,
T1.third_cons_create_mh_receive_date_key,
T1.third_cons_create_mh_receive_time_key,
T1.last_cons_create_mh_id_key,
T1.last_cons_create_mh_dest_id_key,
T1.last_cons_create_mh_date_key,
T1.last_cons_create_mh_time_key,
T1.last_cons_create_mh_receive_date_key,
T1.last_cons_create_mh_receive_time_key,
T1.sec_last_cons_create_mh_id_key,
T1.sec_last_cons_create_mh_dest_id_key,
T1.sec_last_cons_create_mh_date_key,
T1.sec_last_cons_create_mh_time_key,
T1.sec_last_cons_create_mh_receive_date_key,
T1.sec_last_cons_create_mh_receive_time_key,
T1.last_cons_dest_dh_id_key,
T1.last_cons_dh_source_hub_id_key,
T1.last_cons_dh_source_create_date_key,
T1.last_cons_dh_source_create_time_key,
T1.last_cons_dh_receive_date_key,
T1.last_cons_dh_receive_time_key,
T1.shipment_current_status,
T1.seller_type,
T1.shipment_dg_flag,
T1.shipment_priority_flag,
T1.service_tier,
T1.surface_mandatory_flag,
T1.ekl_shipment_type,
T1.reverse_shipment_type,
T1.number_of_hops,
T1.number_of_air_hops,
T1.ekl_fin_zone,
T1.ekart_lzn_flag,
T1.shipment_fa_flag,
T1.shipment_carrier,
T1.shipment_rvp_pk_number_of_attempts,
T1.fsd_number_of_ofd_attempts,
T1.shipment_value,
T1.vendor_id_key,
T1.seller_id_key,
T1.primary_product_key,
T1.source_pincode_key,
T1.destination_pincode_key,
T1.customer_address_id_key,
T1.vendor_service_type,
T1.shipment_first_bag_tracking_id,
T1.shipment_last_bag_tracking_id,
T1.shipment_first_consignment_id,
T1.shipment_last_consignment_id,
T1.shipment_last_bag_status,
T1.shipment_last_consignment_status,
T1.misroute_flag,
T1.customer_misroute_flag,
T1.profiler_flag,
T1.shipment_origin_facility_id_key,
T1.shipment_origin_mh_facility_id_key,
T1.shipment_destination_mh_facility_id_key,
T1.shipment_destination_facility_id_key,
T1.shipment_first_received_dh_id_key,
T1.shipment_last_received_dh_id_key,
T1.shipment_first_received_pc_id_key,
T1.shipment_last_received_pc_id_key,
T1.shipment_first_received_hub_id_key,
T1.shipment_last_received_hub_id_key,
T1.shipment_first_received_mh_id_key,
T1.shipment_last_received_mh_id_key,
T1.fsd_assigned_hub_id_key,
T1.reverse_pickup_hub_id_key,
T1.shipment_current_hub_id_key,
T1.shipment_current_hub_type,
T1.shipment_last_consignment_source_hub_id_key,
T1.shipment_first_consignment_destination_hub_id_key,
T1.shipment_last_consignment_destination_hub_id_key,
T1.shipment_first_bag_created_hub_id_key,
T1.shipment_first_bag_final_hub_id_key,
T1.shipment_last_bag_created_hub_id_key,
T1.shipment_last_bag_final_hub_id_key,
T1.shipment_last_bag_hub_id_key,
T1.shipment_created_at_date_key,
T1.shipment_created_at_time_key,
T1.shipment_delivered_at_date_key,
T1.shipment_delivered_at_time_key,
T1.shipment_first_dispatched_to_merchant_date_key,
T1.shipment_first_dispatched_to_merchant_time_key,
T1.fsd_last_update_date_key,
T1.fsd_last_update_time_key,
T1.fsd_first_dh_received_date_key,
T1.fsd_first_dh_received_time_key,
T1.fsd_last_dh_received_date_key,
T1.fsd_last_dh_received_time_key,
T1.shipment_first_received_pc_date_key,
T1.shipment_first_received_pc_time_key,
T1.shipment_last_received_pc_date_key,
T1.shipment_last_received_pc_time_key,
T1.fsd_first_ofd_date_key,
T1.fsd_first_ofd_time_key,
T1.fsd_last_ofd_date_key,
T1.fsd_last_ofd_time_key,
T1.shipment_first_rvp_pickup_date_key,
T1.shipment_first_rvp_pickup_time_key,
T1.shipment_last_rvp_pickup_date_key,
T1.shipment_last_rvp_pickup_time_key,
T1.order_item_date_key,
T1.order_item_time_key,
T1.shipment_first_bag_create_date_key,
T1.shipment_first_bag_create_time_key,
T1.shipment_first_bag_closed_date_key,
T1.shipment_first_bag_closed_time_key,
T1.shipment_first_bag_reach_date_key,
T1.shipment_first_bag_reach_time_key,
T1.shipment_first_bag_final_received_date_key,
T1.shipment_first_bag_final_received_time_key,
T1.shipment_last_bag_create_date_key,
T1.shipment_last_bag_create_time_key,
T1.shipment_last_bag_close_date_key,
T1.shipment_last_bag_close_time_key,
T1.shipment_last_bag_reach_date_key,
T1.shipment_last_bag_reach_time_key,
T1.shipment_last_bag_receive_date_key,
T1.shipment_last_bag_receive_time_key,
T1.shipment_first_consignment_create_date_key,
T1.shipment_first_consignment_create_time_key,
T1.shipment_first_consignment_receive_date_key,
T1.shipment_first_consignment_receive_time_key,
T1.shipment_last_consignment_create_date_key,
T1.shipment_last_consignment_create_time_key,
T1.shipment_last_consignment_receive_date_key,
T1.shipment_last_consignment_receive_time_key,
T1.first_mh_tc_receive_date_key,
T1.first_mh_tc_receive_time_key,
T1.last_mh_tc_receive_date_key,
T1.last_mh_tc_receive_time_key,
T1.actual_rvp_pickup_date_key,
T1.actual_rvp_pickup_time_key,
T1.rto_complete_date_key,
T1.rto_complete_time_key,
T1.customer_dependency_flag,

if(T1.reverse_shipment_type ='Pickup_Only',
if(T1.shipment_created_at_date_key IS NOT NULL AND T1.first_cons_create_dh_date_key IS NOT NULL,
if( from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'),'u') = 6,
if(T1.shipment_created_at_date_key = T1.fsd_first_dh_received_date_key, 0,1), 0),0),0) as pickup_added_sunday_flag,

if(T1.reverse_shipment_type ='Pickup_Only',
if(T1.shipment_created_at_date_key IS NOT NULL AND T1.fsd_first_dh_received_date_key IS NOT NULL,
(datediff(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),
to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd')))) * 24 
+ round((T1.first_cons_create_dh_time_key / 100)) - round((T1.shipment_created_at_time_key / 100)) 
+ round(((pmod(T1.first_cons_create_dh_time_key,100) - pmod(T1.shipment_created_at_time_key,100))/60),2)) -
if( from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'),'u') = 6,
if(T1.shipment_created_at_date_key = T1.fsd_first_dh_received_date_key, 0,24), 0),0),0) as pickup_time_in_hours,

if(T1.fsd_first_dh_received_date_key IS NOT NULL AND T1.first_cons_create_dh_date_key IS NOT NULL,
(datediff(to_date(from_unixtime(unix_timestamp(cast(T1.first_cons_create_dh_date_key as string),'yyyyMMdd'))),
to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd')))) * 24 
+ round(T1.first_cons_create_dh_time_key / 100) - round(T1.fsd_first_dh_received_time_key / 100)
+ round(((pmod(T1.first_cons_create_dh_time_key,100) - pmod(T1.fsd_first_dh_received_time_key,100))/60),2)
) , 0) as dh_processing_time_in_hours,

if(T1.first_cons_create_dh_receive_date_key IS NOT NULL AND T1.first_cons_create_dh_date_key IS NOT NULL,
((datediff(to_date(from_unixtime(unix_timestamp(cast(T1.first_cons_create_dh_receive_date_key as string),'yyyyMMdd'))),
to_date(from_unixtime(unix_timestamp(cast(T1.first_cons_create_dh_date_key as string),'yyyyMMdd')))) * 24 
+ round(T1.first_cons_create_dh_receive_time_key / 100) - round(T1.first_cons_create_dh_time_key / 100)
+ round(((pmod(T1.first_cons_create_dh_receive_time_key,100) - pmod(T1.first_cons_create_dh_time_key,100))/60),2)
) ), 0) as tc_dh_to_1st_mh_time_in_hours,


if( T1.first_received_mh_id_key = T1.last_received_mh_id_key, 
if(T1.first_received_mh_date_key IS NOT NULL AND T1.first_cons_create_dh_receive_date_key IS NOT NULL,
((datediff(to_date(from_unixtime(unix_timestamp(cast(T1.first_received_mh_date_key as string),'yyyyMMdd'))),
to_date(from_unixtime(unix_timestamp(cast(T1.first_cons_create_dh_receive_date_key as string),'yyyyMMdd')))) * 24 
+ round(T1.first_received_mh_time_key / 100) - round(T1.first_cons_create_dh_receive_time_key / 100)
+ round(((pmod(T1.first_received_mh_time_key,100) - pmod(T1.first_cons_create_dh_receive_time_key,100))/60),2)
) ), 0) , 

if(T1.first_cons_create_mh_date_key IS NOT NULL AND T1.first_cons_create_dh_receive_date_key IS NOT NULL,
((datediff(to_date(from_unixtime(unix_timestamp(cast(T1.first_cons_create_mh_date_key as string),'yyyyMMdd'))),
to_date(from_unixtime(unix_timestamp(cast(T1.first_cons_create_dh_receive_date_key as string),'yyyyMMdd')))) * 24 
+ round(T1.first_cons_create_mh_time_key / 100) - round(T1.first_cons_create_dh_receive_time_key / 100)
+ round(((pmod(T1.first_cons_create_mh_time_key,100) - pmod(T1.first_cons_create_dh_receive_time_key,100))/60),2)
)), 0) 
)as first_mh_to_cons_time_in_hours,

if( T1.first_received_mh_id_key = T1.last_received_mh_id_key, 0 ,
if(T1.seller_type = 'FA' or  T1.seller_type = 'WSR',
if(T1.last_cons_create_mh_receive_date_key IS NOT NULL AND T1.first_cons_create_mh_date_key IS NOT NULL,
((datediff(to_date(from_unixtime(unix_timestamp(cast(T1.last_cons_create_mh_receive_date_key as string),'yyyyMMdd'))),
to_date(from_unixtime(unix_timestamp(cast(T1.first_cons_create_mh_date_key as string),'yyyyMMdd')))) * 24 
+ round(T1.last_cons_create_mh_receive_time_key / 100) - round(T1.first_cons_create_mh_time_key / 100)
+ round(((pmod(T1.last_cons_create_mh_receive_time_key,100) - pmod(T1.first_cons_create_mh_time_key,100))/60),2)
)), 0),

if(T1.sec_last_cons_create_mh_receive_date_key IS NOT NULL AND T1.first_cons_create_mh_date_key IS NOT NULL,
((datediff(to_date(from_unixtime(unix_timestamp(cast(T1.sec_last_cons_create_mh_receive_date_key as string),'yyyyMMdd'))),
to_date(from_unixtime(unix_timestamp(cast(T1.first_cons_create_mh_date_key as string),'yyyyMMdd')))) * 24 
+ round(T1.sec_last_cons_create_mh_receive_time_key / 100) - round(T1.first_cons_create_mh_time_key / 100)
+ round(((pmod(T1.last_cons_create_mh_receive_time_key,100) - pmod(T1.first_cons_create_mh_time_key,100))/60),2))), 0)
)) as tc_1stmh_to_last_mh_time_in_hours,


if( T1.first_received_mh_id_key = T1.last_received_mh_id_key,0,
if(T1.seller_type = 'FA' or  T1.seller_type = 'WSR',
if(T1.last_received_mh_date_key IS NOT NULL AND T1.last_cons_create_mh_receive_date_key IS NOT NULL,
((datediff(to_date(from_unixtime(unix_timestamp(cast(T1.last_received_mh_date_key as string),'yyyyMMdd'))),
to_date(from_unixtime(unix_timestamp(cast(T1.last_cons_create_mh_receive_date_key as string),'yyyyMMdd')))) * 24 
+ round(T1.last_received_mh_time_key / 100) - round(T1.last_cons_create_mh_receive_time_key / 100)
+ round(((pmod(T1.last_received_mh_time_key,100) - pmod(T1.last_cons_create_mh_receive_time_key,100))/60),2)
)), 0),

if(T1.last_cons_create_mh_date_key IS NOT NULL AND T1.sec_last_cons_create_mh_receive_date_key IS NOT NULL,
((datediff(to_date(from_unixtime(unix_timestamp(cast(T1.last_cons_create_mh_date_key as string),'yyyyMMdd'))),
to_date(from_unixtime(unix_timestamp(cast(T1.sec_last_cons_create_mh_receive_date_key as string),'yyyyMMdd')))) * 24 
+ round(T1.last_cons_create_mh_time_key / 100) - round(T1.sec_last_cons_create_mh_receive_time_key / 100)
+ round(((pmod(T1.last_cons_create_mh_time_key,100) - pmod(T1.sec_last_cons_create_mh_receive_time_key,100))/60),2)
)), 0)
)) as last_mh_to_cons_time_in_hours,

if(T1.seller_type = 'FA' or  T1.seller_type = 'WSR', 0,
if(T1.last_cons_create_mh_receive_date_key IS NOT NULL AND T1.last_cons_create_mh_date_key IS NOT NULL,
((datediff(to_date(from_unixtime(unix_timestamp(cast(T1.last_cons_create_mh_receive_date_key as string),'yyyyMMdd'))),
to_date(from_unixtime(unix_timestamp(cast(T1.last_cons_create_mh_date_key as string),'yyyyMMdd')))) * 24 
+ round(T1.last_cons_create_mh_receive_time_key / 100) - round(T1.last_cons_create_mh_time_key / 100)
+ round(((pmod(T1.last_cons_create_mh_receive_time_key,100) - pmod(T1.last_cons_create_mh_time_key,100))/60),2)
)), 0)) as tc_last_mh_to_ph_time_in_hours,

if(T1.seller_type = 'FA' or  T1.seller_type = 'WSR',
if(T1.rto_complete_date_key IS NOT NULL AND T1.last_received_mh_date_key IS NOT NULL,
((datediff(to_date(from_unixtime(unix_timestamp(cast(T1.rto_complete_date_key as string),'yyyyMMdd'))),
to_date(from_unixtime(unix_timestamp(cast(T1.last_received_mh_date_key as string),'yyyyMMdd')))) * 24
+ round(T1.rto_complete_time_key / 100) - round(T1.last_received_mh_time_key / 100)
+ round(((pmod(T1.rto_complete_time_key,100) - pmod(T1.last_received_mh_time_key,100))/60),2)
)), 0),

if(T1.fsd_assigned_hub_id_key = T1.last_received_dh_id_key AND T1.rto_complete_date_key IS NOT NULL AND T1.last_cons_create_mh_receive_date_key is not null,
((datediff(to_date(from_unixtime(unix_timestamp(cast(T1.rto_complete_date_key as string),'yyyyMMdd'))),
to_date(from_unixtime(unix_timestamp(cast(T1.last_cons_create_mh_receive_date_key as string),'yyyyMMdd')))) * 24 
+ round(T1.rto_complete_time_key / 100) - round(T1.last_cons_create_mh_receive_time_key / 100)
+ round(((pmod(T1.rto_complete_time_key,100) - pmod(T1.last_cons_create_mh_receive_time_key,100))/60),2)
)), 0)) as rcin_or_ph_to_complete_time_in_hours,

if(T1.ekl_shipment_type='rvp' and T1.reverse_shipment_type ='Pickup_Only',
CASE  
when T1.ekl_fin_zone = 'INTRACITY' and (T1.seller_type = 'FA' or  T1.seller_type = 'WSR')
then 
if(from_unixtime(unix_timestamp(date_add(to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'))),4),'yyyy-MM-dd'),'EEEE') = 'Sunday',
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'))),5),
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'))),4))

when T1.ekl_fin_zone = 'INTRACITY' and T1.seller_type <> 'FA' and  T1.seller_type <> 'WSR'
then 
if(from_unixtime(unix_timestamp(date_add(to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'))),5),'yyyy-MM-dd'),'EEEE') = 'Sunday',
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'))),6),
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'))),5))

when T1.ekl_fin_zone = 'INTRAZONE' and (T1.seller_type = 'FA' or  T1.seller_type = 'WSR')
then 
if(from_unixtime(unix_timestamp(date_add(to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'))),7),'yyyy-MM-dd'),'EEEE') = 'Sunday',
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'))),8),
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'))),7))

when T1.ekl_fin_zone = 'INTRAZONE' and T1.seller_type <> 'FA' and  T1.seller_type <> 'WSR'
then 
if(from_unixtime(unix_timestamp(date_add(to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'))),8),'yyyy-MM-dd'),'EEEE') = 'Sunday',
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'))),9),
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'))),8))

when T1.ekl_fin_zone = 'INTERZONE' and (T1.seller_type = 'FA' or  T1.seller_type = 'WSR')
then 
if(from_unixtime(unix_timestamp(date_add(to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'))),10),'yyyy-MM-dd'),'EEEE') = 'Sunday',
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'))),11),
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'))),10))

when T1.ekl_fin_zone = 'INTERZONE' and T1.seller_type <> 'FA' and  T1.seller_type <> 'WSR'
then 
if(from_unixtime(unix_timestamp(date_add(to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'))),11),'yyyy-MM-dd'),'EEEE') = 'Sunday',
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'))),12),
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.shipment_created_at_date_key as string),'yyyyMMdd'))),11))
END,

CASE  
when T1.ekl_fin_zone = 'INTRACITY' and (T1.seller_type = 'FA' or  T1.seller_type = 'WSR')
then 
if(from_unixtime(unix_timestamp(date_add(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),4),'yyyy-MM-dd'),'EEEE') = 'Sunday',
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),5),
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),4))

when T1.ekl_fin_zone = 'INTRACITY' and T1.seller_type <> 'FA' and  T1.seller_type <> 'WSR'
then 
if(from_unixtime(unix_timestamp(date_add(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),5),'yyyy-MM-dd'),'EEEE') = 'Sunday',
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),6),
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),5))

when T1.ekl_fin_zone = 'INTRAZONE' and (T1.seller_type = 'FA' or  T1.seller_type = 'WSR')
then 
if(from_unixtime(unix_timestamp(date_add(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),7),'yyyy-MM-dd'),'EEEE') = 'Sunday',
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),8),
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),7))

when T1.ekl_fin_zone = 'INTRAZONE' and T1.seller_type <> 'FA' and  T1.seller_type <> 'WSR'
then 
if(from_unixtime(unix_timestamp(date_add(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),8),'yyyy-MM-dd'),'EEEE') = 'Sunday',
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),9),
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),8))

when T1.ekl_fin_zone = 'INTERZONE' and (T1.seller_type = 'FA' or  T1.seller_type = 'WSR')
then 
if(from_unixtime(unix_timestamp(date_add(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),10),'yyyy-MM-dd'),'EEEE') = 'Sunday',
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),11),
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),10))

when T1.ekl_fin_zone = 'INTERZONE' and T1.seller_type <> 'FA' and  T1.seller_type <> 'WSR'
then 
if(from_unixtime(unix_timestamp(date_add(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),11),'yyyy-MM-dd'),'EEEE') = 'Sunday',
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),12),
date_add(to_date(from_unixtime(unix_timestamp(cast(T1.fsd_first_dh_received_date_key as string),'yyyyMMdd'))),11))
END
) as promised_date,


if(T1.ekl_shipment_type='rvp' and T1.reverse_shipment_type ='Pickup_Only',
if(T1.shipment_created_at_date_key is not null and T1.shipment_created_at_time_key is not Null,
CONCAT(SUBSTR(T1.shipment_created_at_date_key,5,2),'/',SUBSTR(T1.shipment_created_at_date_key,7,2),'/',SUBSTR(T1.shipment_created_at_date_key,1,4),'  ',
if((T1.shipment_created_at_time_key/100) >12,floor(T1.shipment_created_at_time_key/100) - 12, floor(T1.shipment_created_at_time_key/100)),':',
pmod(T1.shipment_created_at_time_key,100),':00 ', if((T1.shipment_created_at_time_key/100) >12,'PM','AM')), ' ' ),

if(T1.fsd_first_dh_received_date_key is not null and T1.fsd_first_dh_received_time_key is not Null,
CONCAT(SUBSTR(T1.fsd_first_dh_received_date_key,5,2),'/',SUBSTR(T1.fsd_first_dh_received_date_key,7,2),'/',SUBSTR(T1.fsd_first_dh_received_date_key,1,4),'  ',
if((T1.fsd_first_dh_received_time_key/100) >12,floor(T1.fsd_first_dh_received_time_key/100) - 12, floor(T1.fsd_first_dh_received_time_key/100)),':',
pmod(T1.fsd_first_dh_received_time_key,100),':00 ', if((T1.fsd_first_dh_received_time_key/100) >12,'PM','AM')), ' ' )) as rvp_start_date1,

if(T1.first_received_dh_date_key is not null and T1.first_received_dh_time_key is not Null,
CONCAT(SUBSTR(T1.first_received_dh_date_key,5,2),'/',SUBSTR(T1.first_received_dh_date_key,7,2),'/',SUBSTR(T1.first_received_dh_date_key,1,4),'  ',
if((T1.first_received_dh_time_key/100) >12,floor(T1.first_received_dh_time_key/100) - 12, floor(T1.first_received_dh_time_key/100)),':',
pmod(T1.first_received_dh_time_key,100),':00 ', if((T1.first_received_dh_time_key/100) >12,'PM','AM')),' ' ) as first_received_dh_date1,

if(T1.first_cons_create_dh_date_key is not null and T1.first_cons_create_dh_time_key is not Null,
CONCAT(SUBSTR(T1.first_cons_create_dh_date_key,5,2),'/',SUBSTR(T1.first_cons_create_dh_date_key,7,2),'/',SUBSTR(T1.first_cons_create_dh_date_key,1,4),'  ',
if((T1.first_cons_create_dh_time_key/100) >12,floor(T1.first_cons_create_dh_time_key/100) - 12, floor(T1.first_cons_create_dh_time_key/100)),':',
pmod(T1.first_cons_create_dh_time_key,100),':00 ', if((T1.first_cons_create_dh_time_key/100) >12,'PM','AM')), ' ') as first_cons_create_dh_date1,

if(T1.first_cons_create_dh_receive_date_key is not null and T1.first_cons_create_dh_receive_time_key is not Null,
CONCAT(SUBSTR(T1.first_cons_create_dh_receive_date_key,5,2),'/',SUBSTR(T1.first_cons_create_dh_receive_date_key,7,2),'/',SUBSTR(T1.first_cons_create_dh_receive_date_key,1,4),'  ',
if((T1.first_cons_create_dh_receive_time_key/100) >12,floor(T1.first_cons_create_dh_receive_time_key/100) - 12, floor(T1.first_cons_create_dh_receive_time_key/100)),':',
pmod(T1.first_cons_create_dh_receive_time_key,100),':00 ', if((T1.first_cons_create_dh_receive_time_key/100) >12,'PM','AM')), ' ') as first_cons_create_dh_receive_date1,


if(T1.first_received_mh_date_key is not null and T1.first_received_mh_time_key is not Null,
CONCAT(SUBSTR(T1.first_received_mh_date_key,5,2),'/',SUBSTR(T1.first_received_mh_date_key,7,2),'/',SUBSTR(T1.first_received_mh_date_key,1,4),'  ',
if((T1.first_received_mh_time_key/100) >12,floor(T1.first_received_mh_time_key/100) - 12, floor(T1.first_received_mh_time_key/100)),':',
pmod(T1.first_received_mh_time_key,100),':00 ', if((T1.first_received_mh_time_key/100) >12,'PM','AM')),' ') as first_received_mh_date1,

if(T1.first_cons_create_mh_date_key is not null and T1.first_cons_create_mh_time_key is not Null,
CONCAT(SUBSTR(T1.first_cons_create_mh_date_key,5,2),'/',SUBSTR(T1.first_cons_create_mh_date_key,7,2),'/',SUBSTR(T1.first_cons_create_mh_date_key,1,4),'  ',
if((T1.first_cons_create_mh_time_key/100) >12,floor(T1.first_cons_create_mh_time_key/100) - 12, floor(T1.first_cons_create_mh_time_key/100)),':',
pmod(T1.first_cons_create_mh_time_key,100),':00 ', if((T1.first_cons_create_mh_time_key/100) >12,'PM','AM')), ' ') as first_cons_create_mh_date1,

if(T1.sec_last_cons_create_mh_receive_date_key is not null and T1.sec_last_cons_create_mh_receive_time_key is not Null,
CONCAT(SUBSTR(T1.sec_last_cons_create_mh_receive_date_key,5,2),'/',SUBSTR(T1.sec_last_cons_create_mh_receive_date_key,7,2),'/',SUBSTR(T1.sec_last_cons_create_mh_receive_date_key,1,4),'  ',
if((T1.sec_last_cons_create_mh_receive_time_key/100) >12,floor(T1.sec_last_cons_create_mh_receive_time_key/100) - 12, floor(T1.sec_last_cons_create_mh_receive_time_key/100)),':',
pmod(T1.sec_last_cons_create_mh_receive_time_key,100),':00 ', if((T1.sec_last_cons_create_mh_receive_time_key/100) >12,'PM','AM')), ' ' ) as sec_last_cons_create_mh_receive_date1,

if(T1.last_received_mh_date_key is not null and T1.last_received_mh_time_key is not Null and T1.first_received_mh_id_key != T1.last_received_mh_id_key,
CONCAT(SUBSTR(T1.last_received_mh_date_key,5,2),'/',SUBSTR(T1.last_received_mh_date_key,7,2),'/',SUBSTR(T1.last_received_mh_date_key,1,4),'  ',
if((T1.last_received_mh_time_key/100) >12,floor(T1.last_received_mh_time_key/100) - 12, floor(T1.last_received_mh_time_key/100)),':',
pmod(T1.last_received_mh_time_key,100),':00 ', if((T1.last_received_mh_time_key/100) >12,'PM','AM')), ' ' ) as last_received_mh_date1,

if(T1.last_cons_create_mh_date_key is not null and T1.last_cons_create_mh_time_key is not Null and T1.first_received_mh_id_key != T1.last_received_mh_id_key,
CONCAT(SUBSTR(T1.last_cons_create_mh_date_key,5,2),'/',SUBSTR(T1.last_cons_create_mh_date_key,7,2),'/',SUBSTR(T1.last_cons_create_mh_date_key,1,4),'  ',
if((T1.last_cons_create_mh_time_key/100) >12,floor(T1.last_cons_create_mh_time_key/100) - 12, floor(T1.last_cons_create_mh_time_key/100)),':',
pmod(T1.last_cons_create_mh_time_key,100),':00 ', if((T1.last_cons_create_mh_time_key/100) >12,'PM','AM')), ' ' ) as last_cons_create_mh_date1,

if(T1.last_cons_create_mh_receive_date_key is not null and T1.last_cons_create_mh_receive_time_key is not Null,
CONCAT(SUBSTR(T1.last_cons_create_mh_receive_date_key,5,2),'/',SUBSTR(T1.last_cons_create_mh_receive_date_key,7,2),'/',SUBSTR(T1.last_cons_create_mh_receive_date_key,1,4),'  ',
if((T1.last_cons_create_mh_receive_time_key/100) >12,floor(T1.last_cons_create_mh_receive_time_key/100) - 12, floor(T1.last_cons_create_mh_receive_time_key/100)),':',
pmod(T1.last_cons_create_mh_receive_time_key,100),':00 ', if((T1.last_cons_create_mh_receive_time_key/100) >12,'PM','AM')), ' ') as last_cons_create_mh_receive_date1,


if(T1.last_received_dh_date_key is not null and T1.last_received_dh_time_key is not Null and T1.seller_type <> 'FA' and  T1.seller_type <> 'WSR' ,
CONCAT(SUBSTR(T1.last_received_dh_date_key,5,2),'/',SUBSTR(T1.last_received_dh_date_key,7,2),'/',SUBSTR(T1.last_received_dh_date_key,1,4),'  ',
if((T1.last_received_dh_time_key/100) >12,floor(T1.last_received_dh_time_key/100) - 12, floor(T1.last_received_dh_time_key/100)),':',
pmod(T1.last_received_dh_time_key,100),':00 ', if((T1.last_received_dh_time_key/100) >12,'PM','AM')), ' ' ) as last_received_dh_date1,

if(T1.rto_complete_date_key is not null and T1.rto_complete_time_key is not Null,
CONCAT(SUBSTR(T1.rto_complete_date_key,5,2),'/',SUBSTR(T1.rto_complete_date_key,7,2),'/',SUBSTR(T1.rto_complete_date_key,1,4),'  ',
if((T1.rto_complete_time_key/100) >12,floor(T1.rto_complete_time_key/100) - 12, floor(T1.rto_complete_time_key/100)),':',
pmod(T1.rto_complete_time_key,100),':00 ', if((T1.rto_complete_time_key/100) >12,'PM','AM')), ' ') as rvp_complete_date1,


if( T1.reverse_shipment_type ='Pickup_Only', T1.shipment_created_at_date_key, 
if( T1.reverse_shipment_type ='Replacement', T1.fsd_first_dh_received_date_key,  NULL)) as rvp_schedule_date_key,

if( T1.reverse_shipment_type ='Pickup_Only', T1.shipment_created_at_time_key, 
if( T1.reverse_shipment_type ='Replacement', T1.fsd_first_dh_received_time_key,  NULL)) as rvp_schedule_time_key,

T1.first_received_dh_date_key as dh_in_date_key,
T1.first_received_dh_time_key as dh_in_time_key,
T1.first_cons_create_dh_date_key as dh_out_date_key,
T1.first_cons_create_dh_time_key as dh_out_time_key,
T1.first_cons_create_dh_receive_date_key as first_mh_in_date_key,
T1.first_cons_create_dh_receive_time_key as first_mh_in_time_key,

if( T1.first_received_mh_id_key = T1.last_received_mh_id_key, 
if(T1.seller_type = 'FA' or  T1.seller_type = 'WSR', NULL, T1.first_cons_create_mh_date_key),T1.first_cons_create_mh_date_key) as first_mh_out_date_key,

if( T1.first_received_mh_id_key = T1.last_received_mh_id_key, 
if(T1.seller_type = 'FA' or  T1.seller_type = 'WSR', NULL, T1.first_cons_create_mh_time_key),T1.first_cons_create_mh_time_key) as first_mh_out_time_key,

if( T1.first_received_mh_id_key = T1.last_received_mh_id_key, NULL,
if(T1.seller_type = 'FA' or  T1.seller_type = 'WSR', T1.last_cons_create_mh_receive_date_key, T1.sec_last_cons_create_mh_receive_date_key)) as last_mh_in_date_key,

if( T1.first_received_mh_id_key = T1.last_received_mh_id_key, NULL,
if(T1.seller_type = 'FA' or  T1.seller_type = 'WSR', T1.last_cons_create_mh_receive_time_key, T1.sec_last_cons_create_mh_receive_time_key)) as last_mh_in_time_key,

if( T1.first_received_mh_id_key = T1.last_received_mh_id_key, NULL,
if(T1.seller_type = 'FA' or  T1.seller_type = 'WSR', NULL, T1.last_cons_create_mh_date_key)) as last_mh_out_date_key,

if( T1.first_received_mh_id_key = T1.last_received_mh_id_key, NULL,
if(T1.seller_type = 'FA' or  T1.seller_type = 'WSR', NULL, T1.last_cons_create_mh_time_key)) as last_mh_out_time_key,

if(T1.seller_type = 'FA' or  T1.seller_type = 'WSR',
if( T1.first_received_mh_id_key = T1.last_received_mh_id_key, T1.first_received_mh_date_key, T1.last_received_mh_date_key) , 
T1.last_cons_create_mh_receive_date_key ) as phin_or_mh_shipment_out_date_key,

if(T1.seller_type = 'FA' or  T1.seller_type = 'WSR',
if( T1.first_received_mh_id_key = T1.last_received_mh_id_key, T1.first_received_mh_time_key, T1.last_received_mh_time_key)  ,
T1.last_cons_create_mh_receive_time_key ) as phin_or_mh_shipment_out_time_key,

T1.rto_complete_date_key as rvp_complete_date_key,
T1.rto_complete_time_key as rvp_complete_time_key


FROM bigfoot_external_neo.scp_ekl__returns_ship_final_l2_hive_fact as T1 
) T2 

) as A 

left outer join bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim as C
on A.first_received_dh_id_key = C.ekl_hive_facility_dim_key

left outer join bigfoot_external_neo.scp_ekl__returns_customer_ekl_dependency_l0_hive_fact as B
on A.vendor_tracking_id = B.vendor_tracking_id
and A.ekl_shipment_type = B.ekl_shipment_type
and C.facility_id = B.dh_hub_id

left outer join 
bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim as D
on A.first_received_mh_id_key = D.ekl_hive_facility_dim_key

left outer join 
bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim as E
on A.last_received_mh_id_key = E.ekl_hive_facility_dim_key

left outer join 
bigfoot_external_neo.scp_ekl__ekl_hive_facility_dim as F
on A.last_received_dh_id_key = F.ekl_hive_facility_dim_key

where A.reverse_shipment_type <>'PREXO'
and A.ekl_fin_zone IN ('INTRACITY', 'INTRAZONE', 'INTERZONE')
;

