INSERT overwrite table fulfillment_tpl_shipment_intermediate_fact

select sr_id,
shipment_reference_ids,
shipment_current_status,
shipment_current_secondary_status,
shipment_current_status_date_time,
shipment_current_location,
shipment_current_remarks,
pickup_created_first_datetime,
out_for_pickup_first_datetime,
pickup_completed_first_datetime,
pickup_failed_first_datetime,
pickup_cancelled_first_datetime,
in_scan_at_hub_first_datetime,
in_transit_first_datetime,
received_at_delivery_hub_first_datetime,
out_for_delivery_first_datetime,
undelivered_attempted_first_datetime,
delivered_first_datetime,
damaged_first_datetime,
lost_first_datetime,
update_lpd_first_datetime,
ready_to_dispatch_first_datetime,
rvp_scheduled_first_datetime,
rvp_out_for_pickup_first_datetime,
rvp_pickup_completed_first_datetime,
rvp_pickup_failed_first_datetime,
rvp_cancelled_first_datetime,
rvp_in_scan_at_hub_first_datetime,
rvp_in_transit_first_datetime,
rvp_received_at_delivery_hub_first_datetime,
rvp_out_for_delivery_first_datetime,
rvp_undelivered_attempted_first_datetime,
rvp_completed_first_datetime,
rto_confirmed_first_datetime,
rto_received_at_delivery_hub_first_datetime,
rto_out_for_delivery_first_datetime,
rto_undelivered_attempted_first_datetime,
rto_completed_first_datetime,
pickup_failed_first_secondary_status,
undelivered_attempted_first_secondary_status,
rvp_undelivered_attempted_first_secondary_status,
pickup_failed_first_remarks,
undelivered_attempted_first_remarks,
rvp_undelivered_attempted_first_remarks,
pickup_failed_second_datetime,
pickup_failed_second_secondary_status,
pickup_failed_second_remarks,
vendor_tag,
design_sla as design_sla_datetime,
customer_sla as customer_sla_datetime,
created_at,
transit_type,
shipment_movement_type,
payment_type,
updated_at,
size,
vendor_tracking_id,
destination_pincode,
destination_type,
shipment_currency,
shipment_value,
amount_to_collect_currency,
amount_to_collect_value,
source_pincode,
source_type,
order_item_unit_tracking_id,
order_item_unit_rts_breach,
order_item_unit_deliver_date_key,
order_item_unit_deliver_time_key,
order_item_unit_ready_to_ship_time_key,
order_item_unit_ready_to_ship_date_key,
order_id,
order_external_id,
order_item_unit_id,
order_item_id,
order_item_unit_shipment_id,
e2e_cp_breach_bucket,
e2e_lp_breach_bucket,
pickup_failed_datetime,
pickup_failed_status,
pickup_failed_remarks,
pickup_failed_attribution,
case when pickup_failed_first_datetime is not null then 1 else 0 end as re_attempt_flag,
case when pickup_failed_first_datetime is not null and pickup_failed_attribution='Seller' then 1 else 0 end as re_attempt_flag_seller,
case when pickup_failed_first_datetime is not null and pickup_failed_attribution='Vendor' then 1 else 0 end as re_attempt_flag_vendor,


if(to_date(delivered_first_datetime)<=to_date(customer_sla),'Delivered_within_promise', 
if(to_date(out_for_delivery_first_datetime)<=to_date(customer_sla) or to_date(undelivered_attempted_first_datetime)<=to_date(customer_sla),'Customer_Dependency-Attempted Undelivered',
if(to_date(design_sla)<=to_date(customer_sla),if(fulfill_item_unit_dispatch_service_tier='EXPRESS',
if(((out_for_pickup_first_datetime='' or out_for_pickup_first_datetime is null) and (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null)) or ((lookup_time(update_lpd_first_datetime)<=1400) and (lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR
((lookup_time(update_lpd_first_datetime)>1400) and ((lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(out_for_pickup_first_datetime)<> lookup_date(date_add(update_lpd_first_datetime,1)))))
,'Pre-dispatch-Vendor',
if(((lookup_time(update_lpd_first_datetime)<=1400) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null) OR
((lookup_time(update_lpd_first_datetime)>1400) and ((lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(date_add(update_lpd_first_datetime,1)))))
,if(pickup_failed_attribution='Seller','Pre-dispatch-Seller','Pre-dispatch-Vendor'),
'Post-dispatch-Vendor')),

if(((out_for_pickup_first_datetime='' or out_for_pickup_first_datetime is null) and (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null)) or ((lookup_time(update_lpd_first_datetime)<=1000) and (lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR
((lookup_time(update_lpd_first_datetime)>1000) and ((lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(out_for_pickup_first_datetime)<> lookup_date(date_add(update_lpd_first_datetime,1)))))
,'Pre-dispatch-Vendor',
if(((lookup_time(update_lpd_first_datetime)<=1000) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null) OR
((lookup_time(update_lpd_first_datetime)>1000) and ((lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(date_add(update_lpd_first_datetime,1)))))
,if(pickup_failed_attribution='Seller','Pre-dispatch-Seller','Pre-dispatch-Vendor'),
'Post-dispatch-Vendor'))),if(order_item_unit_rts_breach = 'Breach' , 'Pre-dispatch-Seller','Pre-Dispatch-FSE')))) as e2e_cp_breach_bucket_l1,





if(to_date(delivered_first_datetime)<=to_date(customer_sla),'Delivered_within_promise', 
if(to_date(out_for_delivery_first_datetime)<=to_date(customer_sla) or to_date(undelivered_attempted_first_datetime)<=to_date(customer_sla),'Customer_Dependency-Attempted Undelivered',
if(to_date(design_sla)<=to_date(customer_sla),
if(fulfill_item_unit_dispatch_service_tier='EXPRESS',
if(((out_for_pickup_first_datetime='' or out_for_pickup_first_datetime is null) and (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null)) or ((lookup_time(update_lpd_first_datetime)<=1400) and (lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR
((lookup_time(update_lpd_first_datetime)>1400) and ((lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(out_for_pickup_first_datetime)<> lookup_date(date_add(update_lpd_first_datetime,1)))))
,'3PL First Mile Breach',
if(((lookup_time(update_lpd_first_datetime)<=1400) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null) OR
((lookup_time(update_lpd_first_datetime)>1400) and ((lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(date_add(update_lpd_first_datetime,1)))))
,if(pickup_failed_attribution='Seller','Pick Up Breach Seller','Pick Up Breach Vendor'),
if( (in_transit_first_datetime='' or in_transit_first_datetime is null) OR (lookup_date(pickup_completed_first_datetime) <> lookup_date(in_transit_first_datetime)),'3PL Connection breach',
if(((lookup_time(received_at_delivery_hub_first_datetime) < 1430) and (lookup_date(out_for_delivery_first_datetime) > lookup_date(received_at_delivery_hub_first_datetime))) ,'3PL Last Mile breach','3PL Line Haul Breach')))),

if(((out_for_pickup_first_datetime='' or out_for_pickup_first_datetime is null) and (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null)) or ((lookup_time(update_lpd_first_datetime)<=1000) and (lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR
((lookup_time(update_lpd_first_datetime)>1000) and ((lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(out_for_pickup_first_datetime)<> lookup_date(date_add(update_lpd_first_datetime,1)))))
,'3PL First Mile Breach',
if(((lookup_time(update_lpd_first_datetime)<=1000) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null) OR
((lookup_time(update_lpd_first_datetime)>1000) and ((lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(date_add(update_lpd_first_datetime,1)))))
,if(pickup_failed_attribution='Seller','Pick Up Breach Seller','Pick Up Breach Vendor'),
if( (in_transit_first_datetime='' or in_transit_first_datetime is null) OR (lookup_date(pickup_completed_first_datetime) <> lookup_date(in_transit_first_datetime)),'3PL Connection breach',
if(((lookup_time(received_at_delivery_hub_first_datetime) < 1430) and (lookup_date(out_for_delivery_first_datetime) > lookup_date(received_at_delivery_hub_first_datetime))) ,'3PL Last Mile breach','3PL Line Haul Breach'))))), if(order_item_unit_rts_breach = 'Breach' , 'RTS Breach',

if(fulfill_item_unit_dispatch_service_tier='EXPRESS',
if(((out_for_pickup_first_datetime='' or out_for_pickup_first_datetime is null) and (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null)) or ((lookup_time(update_lpd_first_datetime)<=1400) and (lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR
((lookup_time(update_lpd_first_datetime)>1400) and ((lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(out_for_pickup_first_datetime)<> lookup_date(date_add(update_lpd_first_datetime,1)))))
,'3PL First Mile Breach',
if(((lookup_time(update_lpd_first_datetime)<=1400) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null) OR
((lookup_time(update_lpd_first_datetime)>1400) and ((lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(date_add(update_lpd_first_datetime,1)))))
,if(pickup_failed_attribution='Seller','Pick Up Breach Seller','Pick Up Breach Vendor'),
if( (in_transit_first_datetime='' or in_transit_first_datetime is null) OR (lookup_date(pickup_completed_first_datetime) <> lookup_date(in_transit_first_datetime)),'3PL Connection breach',
if(((lookup_time(received_at_delivery_hub_first_datetime) < 1430) and (lookup_date(out_for_delivery_first_datetime) > lookup_date(received_at_delivery_hub_first_datetime))) ,'3PL Last Mile breach','3PL Line Haul Breach')))),

if(((out_for_pickup_first_datetime='' or out_for_pickup_first_datetime is null) and (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null)) or ((lookup_time(update_lpd_first_datetime)<=1000) and (lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR
((lookup_time(update_lpd_first_datetime)>1000) and ((lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(out_for_pickup_first_datetime)<> lookup_date(date_add(update_lpd_first_datetime,1)))))
,'3PL First Mile Breach',
if(((lookup_time(update_lpd_first_datetime)<=1000) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null) OR
((lookup_time(update_lpd_first_datetime)>1000) and ((lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(date_add(update_lpd_first_datetime,1)))))
,if(pickup_failed_attribution='Seller','Pick Up Breach Seller','Pick Up Breach Vendor'),
if( (in_transit_first_datetime='' or in_transit_first_datetime is null) OR (lookup_date(pickup_completed_first_datetime) <> lookup_date(in_transit_first_datetime)),'3PL Connection breach',
if(((lookup_time(received_at_delivery_hub_first_datetime) < 1430) and (lookup_date(out_for_delivery_first_datetime) > lookup_date(received_at_delivery_hub_first_datetime))) ,'3PL Last Mile breach','3PL Line Haul Breach'))))))))) as e2e_cp_breach_bucket_l2,


if(to_date(delivered_first_datetime)<=to_date(design_sla),'Delivered_within_promise', 
if(to_date(out_for_delivery_first_datetime)<=to_date(design_sla) or to_date(undelivered_attempted_first_datetime)<=to_date(design_sla) ,'Customer_Dependency-Attempted Undelivered',	
if(fulfill_item_unit_dispatch_service_tier='EXPRESS',
if(((out_for_pickup_first_datetime='' or out_for_pickup_first_datetime is null) and (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null)) or ((lookup_time(update_lpd_first_datetime)<=1400) and (lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR
((lookup_time(update_lpd_first_datetime)>1400) and ((lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(out_for_pickup_first_datetime)<> lookup_date(date_add(update_lpd_first_datetime,1))) ))
,'Pre-dispatch-Vendor',
if(((lookup_time(update_lpd_first_datetime)<=1400) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null) OR
((lookup_time(update_lpd_first_datetime)>1400) and ((lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(date_add(update_lpd_first_datetime,1)))))
,if(pickup_failed_attribution='Seller','Pre-dispatch-Seller','Pre-dispatch-Vendor'),
if((in_transit_first_datetime='' or in_transit_first_datetime is null) OR (lookup_date(pickup_completed_first_datetime) <> lookup_date(in_transit_first_datetime)),'Post-dispatch-Vendor',
if(((lookup_time(received_at_delivery_hub_first_datetime) < 1430) and (lookup_date(out_for_delivery_first_datetime) > lookup_date(received_at_delivery_hub_first_datetime))) ,'Post-dispatch-Vendor','Post-dispatch-Vendor')))),

if(((out_for_pickup_first_datetime='' or out_for_pickup_first_datetime is null) and (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null)) or ((lookup_time(update_lpd_first_datetime)<=1000) and (lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR
((lookup_time(update_lpd_first_datetime)>1000) and ((lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(out_for_pickup_first_datetime)<> lookup_date(date_add(update_lpd_first_datetime,1))) ))
,'Pre-dispatch-Vendor',
if(((lookup_time(update_lpd_first_datetime)<=1000) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null) OR
((lookup_time(update_lpd_first_datetime)>1000) and ((lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(date_add(update_lpd_first_datetime,1)))))
,if(pickup_failed_attribution='Seller','Pre-dispatch-Seller','Pre-dispatch-Vendor'),
if((in_transit_first_datetime='' or in_transit_first_datetime is null) OR (lookup_date(pickup_completed_first_datetime) <> lookup_date(in_transit_first_datetime)),'Post-dispatch-Vendor',
if(((lookup_time(received_at_delivery_hub_first_datetime) < 1430) and (lookup_date(out_for_delivery_first_datetime) > lookup_date(received_at_delivery_hub_first_datetime))) ,'Post-dispatch-Vendor','Post-dispatch-Vendor'))))))) as e2e_lp_breach_bucket_l1,


if(to_date(delivered_first_datetime)<=to_date(design_sla),'Delivered_within_promise', 
if(to_date(out_for_delivery_first_datetime)<=to_date(design_sla) or to_date(undelivered_attempted_first_datetime)<=to_date(design_sla),'Customer_Dependency-Attempted Undelivered',	
if(fulfill_item_unit_dispatch_service_tier='EXPRESS',
if(((out_for_pickup_first_datetime='' or out_for_pickup_first_datetime is null) and (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null)) or ((lookup_time(update_lpd_first_datetime)<=1400) and (lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR
((lookup_time(update_lpd_first_datetime)>1400) and ((lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(out_for_pickup_first_datetime)<> lookup_date(date_add(update_lpd_first_datetime,1)))))
,'3PL First Mile Breach-2',
if(((lookup_time(update_lpd_first_datetime)<=1400) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null) OR
((lookup_time(update_lpd_first_datetime)>1400) and ((lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(date_add(update_lpd_first_datetime,1)))))
,if(pickup_failed_attribution='Seller','Pick Up Breach Seller','Pick Up Breach Vendor'),
if((in_transit_first_datetime='' or in_transit_first_datetime is null) OR (lookup_date(pickup_completed_first_datetime) <> lookup_date(in_transit_first_datetime)),'3PL Connection breach',
if(((lookup_time(received_at_delivery_hub_first_datetime) < 1430) and (lookup_date(out_for_delivery_first_datetime) > lookup_date(received_at_delivery_hub_first_datetime))) ,'3PL Last Mile breach','3PL Line Haul Breach')))),

if(((out_for_pickup_first_datetime='' or out_for_pickup_first_datetime is null) and (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null)) or ((lookup_time(update_lpd_first_datetime)<=1000) and (lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR
((lookup_time(update_lpd_first_datetime)>1000) and ((lookup_date(out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(out_for_pickup_first_datetime)<> lookup_date(date_add(update_lpd_first_datetime,1)))))
,'3PL First Mile Breach-2',
if(((lookup_time(update_lpd_first_datetime)<=1000) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR (pickup_completed_first_datetime='' or pickup_completed_first_datetime is null) OR
((lookup_time(update_lpd_first_datetime)>1000) and ((lookup_date(pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(pickup_completed_first_datetime) <> lookup_date(date_add(update_lpd_first_datetime,1)))))
,if(pickup_failed_attribution='Seller','Pick Up Breach Seller','Pick Up Breach Vendor'),
if((in_transit_first_datetime='' or in_transit_first_datetime is null) OR (lookup_date(pickup_completed_first_datetime) <> lookup_date(in_transit_first_datetime)),'3PL Connection breach',
if(((lookup_time(received_at_delivery_hub_first_datetime) < 1430) and (lookup_date(out_for_delivery_first_datetime) > lookup_date(received_at_delivery_hub_first_datetime))) ,'3PL Last Mile breach','3PL Line Haul Breach'))))))) as e2e_lp_breach_bucket_l2,


if(pickup_failed_first_attribution='Seller' and pickup_failed_first_datetime is not null,1,0) as re_attempt_seller_1,
if(pickup_failed_second_attribution='Seller' and pickup_failed_second_datetime is not null,1,0) as re_attempt_seller_2,
if(pickup_failed_third_attribution='Seller' and pickup_failed_third_datetime is not null,1,0) as re_attempt_seller_3,
if(pickup_failed_fourth_attribution='Seller' and pickup_failed_fourth_datetime is not null,1,0) as re_attempt_seller_4,
if(pickup_failed_fifth_attribution='Seller' and pickup_failed_fifth_datetime is not null,1,0) as re_attempt_seller_5,
if(pickup_failed_sixth_attribution='Seller' and pickup_failed_sixth_datetime is not null,1,0) as re_attempt_seller_6,

if(pickup_failed_first_attribution='Vendor' and pickup_failed_first_datetime is not null,1,0) as re_attempt_vendor_1,
if(pickup_failed_second_attribution='Vendor' and pickup_failed_second_datetime is not null,1,0) as re_attempt_vendor_2,
if(pickup_failed_third_attribution='Vendor' and pickup_failed_third_datetime is not null,1,0) as re_attempt_vendor_3,
if(pickup_failed_fourth_attribution='Vendor' and pickup_failed_fourth_datetime is not null,1,0) as re_attempt_vendor_4,
if(pickup_failed_fifth_attribution='Vendor' and pickup_failed_fifth_datetime is not null,1,0) as re_attempt_vendor_5,
if(pickup_failed_sixth_attribution='Vendor' and pickup_failed_sixth_datetime is not null,1,0) as re_attempt_vendor_6,
source_pincode_key,
destination_pincode_key,
seller_id_key,
 business_date_diff(delivered_first_datetime,update_lpd_first_datetime) as buss_delivered_rts_diff,
 datediff(delivered_first_datetime,update_lpd_first_datetime) as delivered_rts_diff,
ekl_fin_zone,
shipping_category,
rto_flag,
  wms_length,
  wms_breadth,
  wms_height,
  volumetric_weight,
  wms_dead_weight,
 fulfill_item_unit_dispatch_service_tier,
 item_cms_category,
 item_cms_vertical,	
 item_invoice_number,
 item_item_content,
 item_listing_breadth,
 item_listing_height,
 item_listing_length,
 item_listing_weight,
 item_lpe_tier,
 item_product_currency,
 item_product_value,
 item_product_breadth,
 item_product_height,
 item_product_length,
 item_product_weight,
 lookupkey('product_id',item_product_id) AS product_id_key,
 item_product_title,
 item_quantity,
 item_seller_pincode,
 item_seller_id,
 item_seller_type,
 item_tax_per_unit_currency,
 item_tax_per_unit_value,
 item_total_tax_currency,
 item_total_tax_value,
 item_is_dangerous,
 pickup_failed_sixth_datetime,
 pickup_failed_last_datetime,
out_for_delivery_third_datetime,
dispatched_to_facility_first_datetime,
dispatched_to_facility_first_status_location,
dispatched_to_merchant_first_datetime,
dispatched_to_merchant_first_status_location,
dispatched_to_seller_first_datetime,
dispatched_to_seller_first_status_location,
dispatched_to_tc_first_datetime,
dispatched_to_tc_first_status_location,
dispatched_to_vendor_first_datetime,
dispatched_to_vendor_first_status_location,
dispatch_failed_first_datetime,
dispatch_failed_first_status_location,
expected_first_datetime,
expected_first_status_location,
marked_for_merchant_dispatch_first_datetime,
marked_for_merchant_dispatch_first_status_location,
marked_for_reshipment_first_datetime,
marked_for_reshipment_first_status_location,
marked_for_seller_return_first_datetime,
marked_for_seller_return_first_status_location,
marked_reshipment_approved_first_datetime,
marked_reshipment_approved_first_status_location,
not_received_first_datetime,
not_received_first_status_location,
pickup_complete_first_datetime,
pickup_complete_first_status_location,
pickup_leg_completed_first_datetime,
pickup_leg_completed_first_status_location,
pickup_out_for_pickup_first_datetime,
pickup_out_for_pickup_first_status_location,
pickup_reattempt_first_datetime,
pickup_reattempt_first_status_location,
pickup_scheduled_first_datetime,
pickup_scheduled_first_status_location,
ready_for_pickup_first_datetime,
ready_for_pickup_first_status_location,
received_first_datetime,
received_first_status_location,
received_by_merchant_first_datetime,
received_by_merchant_first_status_location,
received_by_seller_first_datetime,
received_by_seller_first_status_location,
received_with_error_first_datetime,
received_with_error_first_status_location,
request_for_cancellation_first_datetime,
request_for_cancellation_first_status_location,
request_for_reschedule_first_datetime,
request_for_reschedule_first_status_location,
reshipped_first_datetime,
reshipped_first_status_location,
returned_first_datetime,
returned_first_status_location,
returned_to_seller_first_datetime,
returned_to_seller_first_status_location,
reverse_pickup_delivered_first_datetime,
reverse_pickup_delivered_first_status_location,
reverse_pickup_scheduled_first_datetime,
reverse_pickup_scheduled_first_status_location,
rto_delivered_first_datetime,
rto_delivered_first_status_location,
scheduled_first_datetime,
scheduled_first_status_location,
undelivered_first_datetime,
undelivered_first_status_location,
rto_request_first_datetime,
rto_request_first_status_location,
undelivered_unattempted_first_datetime,
undelivered_unattempted_first_status_location,
vendor_received_first_datetime,
vendor_received_first_status_location,
pickup_reattempt_last_datetime,
pickup_reattempt_second_datetime,
pickup_reattempt_third_datetime,
pickup_reattempt_first_secondary_status,
pickup_reattempt_first_remarks,
facility_id_key,
facility_name,
vendor_service_type,
if(shipment_movement_type='Outgoing',
if(a.delivered_first_datetime<=a.design_sla,'Delivered_within_promise',
if(lookup_date(a.out_for_delivery_first_datetime)<=lookup_date(a.design_sla) or lookup_date(a.undelivered_attempted_first_datetime)<=lookup_date(a.design_sla),'Customer_Dependency-Attempted Undelivered-3PL',
if(lower(item_seller_type) in ('mp_fbf_seller'),
if(T22.destination_zone is not null,
if(ekl_service_facility_id IS NOT NULL,
-- Shared
if(a.in_scan_at_hub_first_datetime<=concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH-2.5,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH-2.5,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH-2.5,'\\.')[0],':','00',':','00'))),if(dispatched_to_vendor_first_datetime < concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),"3PL-Breach","MH-Breach"),
if(a.in_scan_at_hub_first_datetime>=concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.LPHT_FC_Shared,'\\.')[1]=5,concat(split(T22.LPHT_FC_Shared,'\\.')[0],':','30',':','00'),concat(split(T22.LPHT_FC_Shared,'\\.')[0],':','00',':','00'))), if(dispatched_to_vendor_first_datetime < concat(date_add(in_scan_at_hub_first_datetime,1)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),"3PL-Breach","MH-Breach"),if(dispatched_to_vendor_first_datetime between concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))) and concat(date_add(in_scan_at_hub_first_datetime,1)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),"Design-Breach",if(dispatched_to_vendor_first_datetime < concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),"3PL-Breach","MH-Breach")))),
-- Independent
if(a.in_scan_at_hub_first_datetime<=concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH-2.5,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH-2.5,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH-2.5,'\\.')[0],':','00',':','00'))),if(dispatched_to_vendor_first_datetime < concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),"3PL-Breach","MH-Breach"),
if(a.in_scan_at_hub_first_datetime>=concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.LPHT_FC_Independent,'\\.')[1]=5,concat(split(T22.LPHT_FC_Independent,'\\.')[0],':','30',':','00'),concat(split(T22.LPHT_FC_Independent,'\\.')[0],':','00',':','00'))), if(dispatched_to_vendor_first_datetime < concat(date_add(in_scan_at_hub_first_datetime,1)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),"3PL-Breach","MH-Breach"),if(dispatched_to_vendor_first_datetime between concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))) and concat(date_add(in_scan_at_hub_first_datetime,1)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),"Design-Breach",if(dispatched_to_vendor_first_datetime < concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),"3PL-Breach","MH-Breach"))))
),'3PL-RCA-Breach')
,
-- non-fa

if(lower(item_seller_type)= 'mp_non_fbf_seller',
if(T33.destination_zone is not null,
if(ekl_service_facility_id IS NOT NULL,
-- Shared
if(expected_first_datetime<=concat(to_date(expected_first_datetime)," ",if(split(T33.LPHT_FC_Shared,'\\.')[1]=5,concat(split(T33.LPHT_FC_Shared,'\\.')[0],':','30',':','00'),concat(split(T33.LPHT_FC_Shared,'\\.')[0],':','00',':','00'))),
if(dispatched_to_vendor_first_datetime between concat(to_date(expected_first_datetime)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))) and concat(date_add(expected_first_datetime,1)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),if(T33.LPHT_FC_Shared-T33.Vendor_Cutoff_MH<6,"Design-Breach","MH-Breach"),if(dispatched_to_vendor_first_datetime <= concat(date_add(expected_first_datetime,1)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),"3PL-Breach","MH-Breach")),
if(dispatched_to_vendor_first_datetime<=concat(date_add(expected_first_datetime,1)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),"3PL-Breach","MH-Breach")
),
-- Independent
if(expected_first_datetime<=concat(to_date(expected_first_datetime)," ",if(split(T33.LPHT_FC_Independent,'\\.')[1]=5,concat(split(T33.LPHT_FC_Independent,'\\.')[0],':','30',':','00'),concat(split(T33.LPHT_FC_Independent,'\\.')[0],':','00',':','00'))),
if(dispatched_to_vendor_first_datetime between concat(to_date(expected_first_datetime)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))) and concat(date_add(expected_first_datetime,1)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),if(T33.LPHT_FC_Independent-T33.Vendor_Cutoff_MH<6,"Design-Breach","MH-Breach"),if(dispatched_to_vendor_first_datetime <= concat(date_add(expected_first_datetime,1)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),"3PL-Breach","MH-Breach")),
if(dispatched_to_vendor_first_datetime<=concat(date_add(expected_first_datetime,1)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),"3PL-Breach","MH-Breach")
)
),'3PL-RCA-Breach'),"3PL-RCA-Breach")
-- non-fa completion
))),'Not_defined_LPB_Return') as new_tpl_lp_breach_bucket,
delivered_received_datetime,
pickup_completed_received_datetime,

lookup_date(shipment_current_status_date_time) as shipment_current_status_date_key,
lookup_date(pickup_created_first_datetime) as pickup_created_first_date_key,
lookup_date(out_for_pickup_first_datetime) as out_for_pickup_first_date_key,
lookup_date(pickup_completed_first_datetime) as pickup_completed_first_date_key,
lookup_date(pickup_failed_first_datetime) as pickup_failed_first_date_key,
lookup_date(pickup_cancelled_first_datetime) as pickup_cancelled_first_date_key,
lookup_date(in_scan_at_hub_first_datetime) as in_scan_at_hub_first_date_key,
lookup_date(in_transit_first_datetime) as in_transit_first_date_key,
lookup_date(received_at_delivery_hub_first_datetime) as received_at_delivery_hub_first_date_key,
lookup_date(out_for_delivery_first_datetime) as out_for_delivery_first_date_key,
lookup_date(undelivered_attempted_first_datetime) as undelivered_attempted_first_date_key,
lookup_date(delivered_first_datetime) as delivered_first_date_key,
lookup_date(damaged_first_datetime) as damaged_first_date_key,
lookup_date(lost_first_datetime) as lost_first_date_key,
lookup_date(update_lpd_first_datetime) as update_lpd_first_date_key,
lookup_date(ready_to_dispatch_first_datetime) as ready_to_dispatch_first_date_key,
lookup_date(rvp_scheduled_first_datetime) as rvp_scheduled_first_date_key,
lookup_date(rvp_out_for_pickup_first_datetime) as rvp_out_for_pickup_first_date_key,
lookup_date(rvp_pickup_completed_first_datetime) as rvp_pickup_completed_first_date_key,
lookup_date(rvp_pickup_failed_first_datetime) as rvp_pickup_failed_first_date_key,
lookup_date(rvp_cancelled_first_datetime) as rvp_cancelled_first_date_key,
lookup_date(rvp_in_scan_at_hub_first_datetime) as rvp_in_scan_at_hub_first_date_key,
lookup_date(rvp_in_transit_first_datetime) as rvp_in_transit_first_date_key,
lookup_date(rvp_received_at_delivery_hub_first_datetime) as rvp_received_at_delivery_hub_first_date_key,
lookup_date(rvp_out_for_delivery_first_datetime) as rvp_out_for_delivery_first_date_key,
lookup_date(rvp_undelivered_attempted_first_datetime) as rvp_undelivered_attempted_first_date_key,
lookup_date(rvp_completed_first_datetime) as rvp_completed_first_date_key,
lookup_date(rto_confirmed_first_datetime) as rto_confirmed_first_date_key,
lookup_date(rto_received_at_delivery_hub_first_datetime) as rto_received_at_delivery_hub_first_date_key,
lookup_date(rto_out_for_delivery_first_datetime) as rto_out_for_delivery_first_date_key,
lookup_date(rto_undelivered_attempted_first_datetime) as rto_undelivered_attempted_first_date_key,
lookup_date(rto_completed_first_datetime) as rto_completed_first_date_key,
lookup_date(pickup_failed_second_datetime) as pickup_failed_second_date_key,
lookup_date(design_sla) as design_sla_date_key,
lookup_date(customer_sla) as customer_sla_date_key,
lookup_date(pickup_failed_datetime) as pickup_failed_date_key,
lookup_date(pickup_failed_sixth_datetime) as pickup_failed_sixth_date_key,
lookup_date(pickup_failed_last_datetime) as pickup_failed_last_date_key,
lookup_date(out_for_delivery_third_datetime) as out_for_delivery_third_date_key,
lookup_date(dispatched_to_facility_first_datetime) as dispatched_to_facility_first_date_key,
lookup_date(dispatched_to_merchant_first_datetime) as dispatched_to_merchant_first_date_key,
lookup_date(dispatched_to_seller_first_datetime) as dispatched_to_seller_first_date_key,
lookup_date(dispatched_to_tc_first_datetime) as dispatched_to_tc_first_date_key,
lookup_date(dispatched_to_vendor_first_datetime) as dispatched_to_vendor_first_date_key,
lookup_date(dispatch_failed_first_datetime) as dispatch_failed_first_date_key,
lookup_date(expected_first_datetime) as expected_first_date_key,
lookup_date(marked_for_merchant_dispatch_first_datetime) as marked_for_merchant_dispatch_first_date_key,
lookup_date(marked_for_reshipment_first_datetime) as marked_for_reshipment_first_date_key,
lookup_date(marked_for_seller_return_first_datetime) as marked_for_seller_return_first_date_key,
lookup_date(marked_reshipment_approved_first_datetime) as marked_reshipment_approved_first_date_key,
lookup_date(not_received_first_datetime) as not_received_first_date_key,
lookup_date(pickup_complete_first_datetime) as pickup_complete_first_date_key,
lookup_date(pickup_leg_completed_first_datetime) as pickup_leg_completed_first_date_key,
lookup_date(pickup_out_for_pickup_first_datetime) as pickup_out_for_pickup_first_date_key,
lookup_date(pickup_reattempt_first_datetime) as pickup_reattempt_first_date_key,
lookup_date(pickup_scheduled_first_datetime) as pickup_scheduled_first_date_key,
lookup_date(ready_for_pickup_first_datetime) as ready_for_pickup_first_date_key,
lookup_date(received_first_datetime) as received_first_date_key,
lookup_date(received_by_merchant_first_datetime) as received_by_merchant_first_date_key,
lookup_date(received_by_seller_first_datetime) as received_by_seller_first_date_key,
lookup_date(received_with_error_first_datetime) as received_with_error_first_date_key,
lookup_date(request_for_cancellation_first_datetime) as request_for_cancellation_first_date_key,
lookup_date(request_for_reschedule_first_datetime) as request_for_reschedule_first_date_key,
lookup_date(reshipped_first_datetime) as reshipped_first_date_key,
lookup_date(returned_first_datetime) as returned_first_date_key,
lookup_date(returned_to_seller_first_datetime) as returned_to_seller_first_date_key,
lookup_date(reverse_pickup_delivered_first_datetime) as reverse_pickup_delivered_first_date_key,
lookup_date(reverse_pickup_scheduled_first_datetime) as reverse_pickup_scheduled_first_date_key,
lookup_date(rto_delivered_first_datetime) as rto_delivered_first_date_key,
lookup_date(scheduled_first_datetime) as scheduled_first_date_key,
lookup_date(undelivered_first_datetime) as undelivered_first_date_key,
lookup_date(rto_request_first_datetime) as rto_request_first_date_key,
lookup_date(undelivered_unattempted_first_datetime) as undelivered_unattempted_first_date_key,
lookup_date(vendor_received_first_datetime) as vendor_received_first_date_key,
lookup_date(pickup_reattempt_last_datetime) as pickup_reattempt_last_date_key,
lookup_date(pickup_reattempt_second_datetime) as pickup_reattempt_second_date_key,
lookup_date(pickup_reattempt_third_datetime) as pickup_reattempt_third_date_key,
lookup_date(delivered_received_datetime) as delivered_received_date_key,
lookup_date(pickup_completed_received_datetime) as pickup_completed_received_date_key,

lookup_time(shipment_current_status_date_time) as shipment_current_status_time_key,
lookup_time(pickup_created_first_datetime) as pickup_created_first_time_key,
lookup_time(out_for_pickup_first_datetime) as out_for_pickup_first_time_key,
lookup_time(pickup_completed_first_datetime) as pickup_completed_first_time_key,
lookup_time(pickup_failed_first_datetime) as pickup_failed_first_time_key,
lookup_time(pickup_cancelled_first_datetime) as pickup_cancelled_first_time_key,
lookup_time(in_scan_at_hub_first_datetime) as in_scan_at_hub_first_time_key,
lookup_time(in_transit_first_datetime) as in_transit_first_time_key,
lookup_time(received_at_delivery_hub_first_datetime) as received_at_delivery_hub_first_time_key,
lookup_time(out_for_delivery_first_datetime) as out_for_delivery_first_time_key,
lookup_time(undelivered_attempted_first_datetime) as undelivered_attempted_first_time_key,
lookup_time(delivered_first_datetime) as delivered_first_time_key,
lookup_time(damaged_first_datetime) as damaged_first_time_key,
lookup_time(lost_first_datetime) as lost_first_time_key,
lookup_time(update_lpd_first_datetime) as update_lpd_first_time_key,
lookup_time(ready_to_dispatch_first_datetime) as ready_to_dispatch_first_time_key,
lookup_time(rvp_scheduled_first_datetime) as rvp_scheduled_first_time_key,
lookup_time(rvp_out_for_pickup_first_datetime) as rvp_out_for_pickup_first_time_key,
lookup_time(rvp_pickup_completed_first_datetime) as rvp_pickup_completed_first_time_key,
lookup_time(rvp_pickup_failed_first_datetime) as rvp_pickup_failed_first_time_key,
lookup_time(rvp_cancelled_first_datetime) as rvp_cancelled_first_time_key,
lookup_time(rvp_in_scan_at_hub_first_datetime) as rvp_in_scan_at_hub_first_time_key,
lookup_time(rvp_in_transit_first_datetime) as rvp_in_transit_first_time_key,
lookup_time(rvp_received_at_delivery_hub_first_datetime) as rvp_received_at_delivery_hub_first_time_key,
lookup_time(rvp_out_for_delivery_first_datetime) as rvp_out_for_delivery_first_time_key,
lookup_time(rvp_undelivered_attempted_first_datetime) as rvp_undelivered_attempted_first_time_key,
lookup_time(rvp_completed_first_datetime) as rvp_completed_first_time_key,
lookup_time(rto_confirmed_first_datetime) as rto_confirmed_first_time_key,
lookup_time(rto_received_at_delivery_hub_first_datetime) as rto_received_at_delivery_hub_first_time_key,
lookup_time(rto_out_for_delivery_first_datetime) as rto_out_for_delivery_first_time_key,
lookup_time(rto_undelivered_attempted_first_datetime) as rto_undelivered_attempted_first_time_key,
lookup_time(rto_completed_first_datetime) as rto_completed_first_time_key,
lookup_time(pickup_failed_second_datetime) as pickup_failed_second_time_key,
lookup_time(design_sla) as design_sla_time_key,
lookup_time(customer_sla) as customer_sla_time_key,
lookup_time(pickup_failed_datetime) as pickup_failed_time_key,
lookup_time(pickup_failed_sixth_datetime) as pickup_failed_sixth_time_key,
lookup_time(pickup_failed_last_datetime) as pickup_failed_last_time_key,
lookup_time(out_for_delivery_third_datetime) as out_for_delivery_third_time_key,
lookup_time(dispatched_to_facility_first_datetime) as dispatched_to_facility_first_time_key,
lookup_time(dispatched_to_merchant_first_datetime) as dispatched_to_merchant_first_time_key,
lookup_time(dispatched_to_seller_first_datetime) as dispatched_to_seller_first_time_key,
lookup_time(dispatched_to_tc_first_datetime) as dispatched_to_tc_first_time_key,
lookup_time(dispatched_to_vendor_first_datetime) as dispatched_to_vendor_first_time_key,
lookup_time(dispatch_failed_first_datetime) as dispatch_failed_first_time_key,
lookup_time(expected_first_datetime) as expected_first_time_key,
lookup_time(marked_for_merchant_dispatch_first_datetime) as marked_for_merchant_dispatch_first_time_key,
lookup_time(marked_for_reshipment_first_datetime) as marked_for_reshipment_first_time_key,
lookup_time(marked_for_seller_return_first_datetime) as marked_for_seller_return_first_time_key,
lookup_time(marked_reshipment_approved_first_datetime) as marked_reshipment_approved_first_time_key,
lookup_time(not_received_first_datetime) as not_received_first_time_key,
lookup_time(pickup_complete_first_datetime) as pickup_complete_first_time_key,
lookup_time(pickup_leg_completed_first_datetime) as pickup_leg_completed_first_time_key,
lookup_time(pickup_out_for_pickup_first_datetime) as pickup_out_for_pickup_first_time_key,
lookup_time(pickup_reattempt_first_datetime) as pickup_reattempt_first_time_key,
lookup_time(pickup_scheduled_first_datetime) as pickup_scheduled_first_time_key,
lookup_time(ready_for_pickup_first_datetime) as ready_for_pickup_first_time_key,
lookup_time(received_first_datetime) as received_first_time_key,
lookup_time(received_by_merchant_first_datetime) as received_by_merchant_first_time_key,
lookup_time(received_by_seller_first_datetime) as received_by_seller_first_time_key,
lookup_time(received_with_error_first_datetime) as received_with_error_first_time_key,
lookup_time(request_for_cancellation_first_datetime) as request_for_cancellation_first_time_key,
lookup_time(request_for_reschedule_first_datetime) as request_for_reschedule_first_time_key,
lookup_time(reshipped_first_datetime) as reshipped_first_time_key,
lookup_time(returned_first_datetime) as returned_first_time_key,
lookup_time(returned_to_seller_first_datetime) as returned_to_seller_first_time_key,
lookup_time(reverse_pickup_delivered_first_datetime) as reverse_pickup_delivered_first_time_key,
lookup_time(reverse_pickup_scheduled_first_datetime) as reverse_pickup_scheduled_first_time_key,
lookup_time(rto_delivered_first_datetime) as rto_delivered_first_time_key,
lookup_time(scheduled_first_datetime) as scheduled_first_time_key,
lookup_time(undelivered_first_datetime) as undelivered_first_time_key,
lookup_time(rto_request_first_datetime) as rto_request_first_time_key,
lookup_time(undelivered_unattempted_first_datetime) as undelivered_unattempted_first_time_key,
lookup_time(vendor_received_first_datetime) as vendor_received_first_time_key,
lookup_time(pickup_reattempt_last_datetime) as pickup_reattempt_last_time_key,
lookup_time(pickup_reattempt_second_datetime) as pickup_reattempt_second_time_key,
lookup_time(pickup_reattempt_third_datetime) as pickup_reattempt_third_time_key,
lookup_time(delivered_received_datetime) as delivered_received_time_key,
lookup_time(pickup_completed_received_datetime) as pickup_completed_received_time_key,
fulfill_item_unit_new_shipment_movement_type,
vendor_tag_tmp_1 as vendor_name,
if(shipment_movement_type='Outgoing',
if(a.delivered_first_datetime<=a.design_sla,'Delivered_within_promise',
if(lookup_date(a.out_for_delivery_first_datetime)<=lookup_date(a.design_sla) or lookup_date(a.undelivered_attempted_first_datetime)<=lookup_date(a.design_sla),'Customer_Dependency-Attempted Undelivered-3PL',
if(lower(item_seller_type) in ('mp_fbf_seller'), 
if(T22.destination_zone is not null,
if(ekl_service_facility_id IS NOT NULL,
-- Shared
if(a.in_scan_at_hub_first_datetime<=concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH-2.5,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH-2.5,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH-2.5,'\\.')[0],':','00',':','00'))),if(dispatched_to_vendor_first_datetime < concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),if(cast((unix_timestamp(in_scan_at_hub_first_datetime)-unix_timestamp(dispatched_to_vendor_first_datetime)) / 3600  as int)>cast(in_scan_cut_off as int),"3PL First Mile Breach",if(cast((unix_timestamp(in_transit_first_datetime)-unix_timestamp(in_scan_at_hub_first_datetime)) / 3600  as int)>cast(in_transit_cut_off as int),"3PL Connection breach",
if(to_date(received_at_delivery_hub_first_datetime)<=to_date(design_sla),
if(received_at_delivery_hub_first_datetime<=concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime),'3PL Last Mile breach',if(received_at_delivery_hub_first_datetime>concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(received_at_delivery_hub_first_datetime)<to_date(design_sla) and (to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime) or to_date(out_for_delivery_first_datetime)<>to_date(date_add(received_at_delivery_hub_first_datetime,1))),'3PL Last Mile breach',if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach'))),if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach')))),"MH-Breach"),
if(a.in_scan_at_hub_first_datetime>=concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.LPHT_FC_Shared,'\\.')[1]=5,concat(split(T22.LPHT_FC_Shared,'\\.')[0],':','30',':','00'),concat(split(T22.LPHT_FC_Shared,'\\.')[0],':','00',':','00'))), if(dispatched_to_vendor_first_datetime < concat(date_add(in_scan_at_hub_first_datetime,1)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),if(cast((unix_timestamp(in_scan_at_hub_first_datetime)-unix_timestamp(dispatched_to_vendor_first_datetime)) / 3600  as int)>cast(in_scan_cut_off as int),"3PL First Mile Breach",if(cast((unix_timestamp(in_transit_first_datetime)-unix_timestamp(in_scan_at_hub_first_datetime)) / 3600  as int)>cast(in_transit_cut_off as int),"3PL Connection breach",
if(to_date(received_at_delivery_hub_first_datetime)<=to_date(design_sla),
if(received_at_delivery_hub_first_datetime<=concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime),'3PL Last Mile breach',if(received_at_delivery_hub_first_datetime>concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(received_at_delivery_hub_first_datetime)<to_date(design_sla) and (to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime) or to_date(out_for_delivery_first_datetime)<>to_date(date_add(received_at_delivery_hub_first_datetime,1))),'3PL Last Mile breach',if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach'))),if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach')))),"MH-Breach"),if(dispatched_to_vendor_first_datetime between concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))) and concat(date_add(in_scan_at_hub_first_datetime,1)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),"Design-Breach",if(dispatched_to_vendor_first_datetime < concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),if(cast((unix_timestamp(in_scan_at_hub_first_datetime)-unix_timestamp(dispatched_to_vendor_first_datetime)) / 3600  as int)>cast(in_scan_cut_off as int),"3PL First Mile Breach",if(cast((unix_timestamp(in_transit_first_datetime)-unix_timestamp(in_scan_at_hub_first_datetime)) / 3600  as int)>cast(in_transit_cut_off as int),"3PL Connection breach",
if(to_date(received_at_delivery_hub_first_datetime)<=to_date(design_sla),
if(received_at_delivery_hub_first_datetime<=concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime),'3PL Last Mile breach',if(received_at_delivery_hub_first_datetime>concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(received_at_delivery_hub_first_datetime)<to_date(design_sla) and (to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime) or to_date(out_for_delivery_first_datetime)<>to_date(date_add(received_at_delivery_hub_first_datetime,1))),'3PL Last Mile breach',if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach'))),if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach')))),"MH-Breach")))),
-- Independent
if(a.in_scan_at_hub_first_datetime<=concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH-2.5,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH-2.5,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH-2.5,'\\.')[0],':','00',':','00'))),if(dispatched_to_vendor_first_datetime < concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),if(cast((unix_timestamp(in_scan_at_hub_first_datetime)-unix_timestamp(dispatched_to_vendor_first_datetime)) / 3600  as int)>cast(in_scan_cut_off as int),"3PL First Mile Breach",if(cast((unix_timestamp(in_transit_first_datetime)-unix_timestamp(in_scan_at_hub_first_datetime)) / 3600  as int)>cast(in_transit_cut_off as int),"3PL Connection breach",
if(to_date(received_at_delivery_hub_first_datetime)<=to_date(design_sla),
if(received_at_delivery_hub_first_datetime<=concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime),'3PL Last Mile breach',if(received_at_delivery_hub_first_datetime>concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(received_at_delivery_hub_first_datetime)<to_date(design_sla) and (to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime) or to_date(out_for_delivery_first_datetime)<>to_date(date_add(received_at_delivery_hub_first_datetime,1))),'3PL Last Mile breach',if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach'))),if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach')))),"MH-Breach"),
if(a.in_scan_at_hub_first_datetime>=concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.LPHT_FC_Independent,'\\.')[1]=5,concat(split(T22.LPHT_FC_Independent,'\\.')[0],':','30',':','00'),concat(split(T22.LPHT_FC_Independent,'\\.')[0],':','00',':','00'))), if(dispatched_to_vendor_first_datetime < concat(date_add(in_scan_at_hub_first_datetime,1)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),if(cast((unix_timestamp(in_scan_at_hub_first_datetime)-unix_timestamp(dispatched_to_vendor_first_datetime)) / 3600  as int)>cast(in_scan_cut_off as int),"3PL First Mile Breach",if(cast((unix_timestamp(in_transit_first_datetime)-unix_timestamp(in_scan_at_hub_first_datetime)) / 3600  as int)>cast(in_transit_cut_off as int),"3PL Connection breach",
if(to_date(received_at_delivery_hub_first_datetime)<=to_date(design_sla),
if(received_at_delivery_hub_first_datetime<=concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime),'3PL Last Mile breach',if(received_at_delivery_hub_first_datetime>concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(received_at_delivery_hub_first_datetime)<to_date(design_sla) and (to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime) or to_date(out_for_delivery_first_datetime)<>to_date(date_add(received_at_delivery_hub_first_datetime,1))),'3PL Last Mile breach',if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach'))),if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach')))),"MH-Breach"),if(dispatched_to_vendor_first_datetime between concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))) and concat(date_add(in_scan_at_hub_first_datetime,1)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),"Design-Breach",if(dispatched_to_vendor_first_datetime < concat(to_date(in_scan_at_hub_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),if(cast((unix_timestamp(in_scan_at_hub_first_datetime)-unix_timestamp(dispatched_to_vendor_first_datetime)) / 3600  as int)>cast(in_scan_cut_off as int),"3PL First Mile Breach",if(cast((unix_timestamp(in_transit_first_datetime)-unix_timestamp(in_scan_at_hub_first_datetime)) / 3600  as int)>cast(in_transit_cut_off as int),"3PL Connection breach",
if(to_date(received_at_delivery_hub_first_datetime)<=to_date(design_sla),
if(received_at_delivery_hub_first_datetime<=concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime),'3PL Last Mile breach',if(received_at_delivery_hub_first_datetime>concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(received_at_delivery_hub_first_datetime)<to_date(design_sla) and (to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime) or to_date(out_for_delivery_first_datetime)<>to_date(date_add(received_at_delivery_hub_first_datetime,1))),'3PL Last Mile breach',if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach'))),if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach')))),"MH-Breach"))))
),'3PL-RCA-Breach')
,
-- non-fa

if(lower(item_seller_type)= 'mp_non_fbf_seller',
if(T33.destination_zone is not null,
if(ekl_service_facility_id IS NOT NULL,
-- Shared
if(expected_first_datetime<=concat(to_date(expected_first_datetime)," ",if(split(T33.LPHT_FC_Shared,'\\.')[1]=5,concat(split(T33.LPHT_FC_Shared,'\\.')[0],':','30',':','00'),concat(split(T33.LPHT_FC_Shared,'\\.')[0],':','00',':','00'))),
if(dispatched_to_vendor_first_datetime between concat(to_date(expected_first_datetime)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))) and concat(date_add(expected_first_datetime,1)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),if(T33.LPHT_FC_Shared-T33.Vendor_Cutoff_MH<6,"Design-Breach","MH-Breach"),if(dispatched_to_vendor_first_datetime <= concat(date_add(expected_first_datetime,1)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),if(cast((unix_timestamp(in_scan_at_hub_first_datetime)-unix_timestamp(dispatched_to_vendor_first_datetime)) / 3600  as int)>cast(in_scan_cut_off as int),"3PL First Mile Breach",if(cast((unix_timestamp(in_transit_first_datetime)-unix_timestamp(in_scan_at_hub_first_datetime)) / 3600  as int)>cast(in_transit_cut_off as int),"3PL Connection breach",
if(to_date(received_at_delivery_hub_first_datetime)<=to_date(design_sla),
if(received_at_delivery_hub_first_datetime<=concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime),'3PL Last Mile breach',if(received_at_delivery_hub_first_datetime>concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(received_at_delivery_hub_first_datetime)<to_date(design_sla) and (to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime) or to_date(out_for_delivery_first_datetime)<>to_date(date_add(received_at_delivery_hub_first_datetime,1))),'3PL Last Mile breach',if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach'))),if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach')))),"MH-Breach")),
if(dispatched_to_vendor_first_datetime<=concat(date_add(expected_first_datetime,1)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),if(cast((unix_timestamp(in_scan_at_hub_first_datetime)-unix_timestamp(dispatched_to_vendor_first_datetime)) / 3600  as int)>cast(in_scan_cut_off as int),"3PL First Mile Breach",if(cast((unix_timestamp(in_transit_first_datetime)-unix_timestamp(in_scan_at_hub_first_datetime)) / 3600  as int)>cast(in_transit_cut_off as int),"3PL Connection breach",
if(to_date(received_at_delivery_hub_first_datetime)<=to_date(design_sla),
if(received_at_delivery_hub_first_datetime<=concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime),'3PL Last Mile breach',if(received_at_delivery_hub_first_datetime>concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(received_at_delivery_hub_first_datetime)<to_date(design_sla) and (to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime) or to_date(out_for_delivery_first_datetime)<>to_date(date_add(received_at_delivery_hub_first_datetime,1))),'3PL Last Mile breach',if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach'))),if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach')))),"MH-Breach")
),
-- Independent
if(expected_first_datetime<=concat(to_date(expected_first_datetime)," ",if(split(T33.LPHT_FC_Independent,'\\.')[1]=5,concat(split(T33.LPHT_FC_Independent,'\\.')[0],':','30',':','00'),concat(split(T33.LPHT_FC_Independent,'\\.')[0],':','00',':','00'))),
if(dispatched_to_vendor_first_datetime between concat(to_date(expected_first_datetime)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))) and concat(date_add(expected_first_datetime,1)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),if(T33.LPHT_FC_Independent-T33.Vendor_Cutoff_MH<6,"Design-Breach","MH-Breach"),if(dispatched_to_vendor_first_datetime <= concat(date_add(expected_first_datetime,1)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),if(cast((unix_timestamp(in_scan_at_hub_first_datetime)-unix_timestamp(dispatched_to_vendor_first_datetime)) / 3600  as int)>cast(in_scan_cut_off as int),"3PL First Mile Breach",if(cast((unix_timestamp(in_transit_first_datetime)-unix_timestamp(in_scan_at_hub_first_datetime)) / 3600  as int)>cast(in_transit_cut_off as int),"3PL Connection breach",
if(to_date(received_at_delivery_hub_first_datetime)<=to_date(design_sla),
if(received_at_delivery_hub_first_datetime<=concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime),'3PL Last Mile breach',if(received_at_delivery_hub_first_datetime>concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(received_at_delivery_hub_first_datetime)<to_date(design_sla) and (to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime) or to_date(out_for_delivery_first_datetime)<>to_date(date_add(received_at_delivery_hub_first_datetime,1))),'3PL Last Mile breach',if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach'))),if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach')))),"MH-Breach")),
if(dispatched_to_vendor_first_datetime<=concat(date_add(expected_first_datetime,1)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1]=5,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))),if(cast((unix_timestamp(in_scan_at_hub_first_datetime)-unix_timestamp(dispatched_to_vendor_first_datetime)) / 3600  as int)>cast(in_scan_cut_off as int),"3PL First Mile Breach",if(cast((unix_timestamp(in_transit_first_datetime)-unix_timestamp(in_scan_at_hub_first_datetime)) / 3600  as int)>cast(in_transit_cut_off as int),"3PL Connection breach",
if(to_date(received_at_delivery_hub_first_datetime)<=to_date(design_sla),
if(received_at_delivery_hub_first_datetime<=concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime),'3PL Last Mile breach',if(received_at_delivery_hub_first_datetime>concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(received_at_delivery_hub_first_datetime)<to_date(design_sla) and (to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime) or to_date(out_for_delivery_first_datetime)<>to_date(date_add(received_at_delivery_hub_first_datetime,1))),'3PL Last Mile breach',if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach'))),if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach')))),"MH-Breach")
)
),'3PL-RCA-Breach'),"3PL-RCA-Breach")
-- non-fa completion
))),'Not_defined_LPB_Return') as new_tpl_lp_breach_bucket_l2,
rvp_out_for_pickup_last_datetime,
case when dh_mapping.dh_name is null then destination_pincode else dh_mapping.dh_name end as dh_name,
case when dh_mapping.service_type is null then 'independent' else dh_mapping.service_type end as dh_vendor_service_type,
rvp_pickup_failed_first_attribution,
rvp_pickup_failed_second_attribution,
rvp_pickup_failed_last_attribution,
undelivered_attempted_first_attribution,
undelivered_attempted_second_attribution,
undelivered_attempted_last_attribution,
rvp_undelivered_attempted_first_attribution,
rvp_undelivered_attempted_second_attribution,
rvp_undelivered_attempted_last_attribution,
rto_confirmed_first_attribution,
rto_confirmed_second_attribution,
rto_confirmed_last_attribution,
rto_undelivered_attempted_first_attribution,
rto_undelivered_attempted_second_attribution,
rto_undelivered_attempted_last_attribution,
rvp_pickup_failed_first_secondary_status,
rvp_pickup_failed_second_secondary_status,
rvp_pickup_failed_last_secondary_status,
undelivered_attempted_second_secondary_status,
undelivered_attempted_last_secondary_status,
rvp_undelivered_attempted_second_secondary_status,
rvp_undelivered_attempted_last_secondary_status,
rto_undelivered_attempted_first_secondary_status,
rto_undelivered_attempted_second_secondary_status,
rto_undelivered_attempted_last_secondary_status,
rto_confirmed_first_secondary_status,
rto_confirmed_second_secondary_status,
rto_confirmed_last_secondary_status,
pickup_failed_third_secondary_status,
pickup_failed_fourth_secondary_status,
pickup_failed_fifth_secondary_status,
pickup_failed_sixth_secondary_status,
rto_handover_completed_datetime,
rvp_handover_completed_datetime,
rto_handover_initiated_datetime,
rvp_handover_initiated_datetime,
fulfill_item_unit_lpe_tier,
fulfill_item_unit_reserve_actual_time,
order_item_approve_date_time,
no_dispatch_flag,
geo_zone_flag,

if(shipment_movement_type='Outgoing',
if(a.delivered_first_datetime<=a.design_sla,'Delivered_within_promise',
if(lookup_date(a.out_for_delivery_first_datetime)<=lookup_date(a.design_sla) or lookup_date(a.undelivered_attempted_first_datetime)<=lookup_date(a.design_sla),'Customer_Dependency-Attempted Undelivered-3PL',
-- fa
if(lower(item_seller_type) in ('mp_fbf_seller'),
if(T22.destination_zone is not null,
if(ekl_service_facility_id IS NOT NULL,
-- Shared
if(a.dispatched_to_vendor_first_datetime between concat(to_date(dispatched_to_vendor_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1] is not null,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))) and concat(to_date(dispatched_to_vendor_first_datetime)," ",if(split(T22.LPHT_FC_Shared,'\\.')[1] is not null,concat(split(T22.LPHT_FC_Shared,'\\.')[0],':','30',':','00'),concat(split(T22.LPHT_FC_Shared,'\\.')[0],':','00',':','00'))),'Design-Breach','3PL-Breach'),
-- Independent
if(a.dispatched_to_vendor_first_datetime between concat(to_date(dispatched_to_vendor_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1] is not null,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))) and concat(to_date(dispatched_to_vendor_first_datetime)," ",if(split(T22.LPHT_FC_Independent,'\\.')[1] is not null,concat(split(T22.LPHT_FC_Independent,'\\.')[0],':','30',':','00'),concat(split(T22.LPHT_FC_Independent,'\\.')[0],':','00',':','00'))),'Design-Breach','3PL-Breach')),'3PL-RCA-Breach'),

-- non-fa
if(lower(item_seller_type)= 'mp_non_fbf_seller',
if(T33.destination_zone is not null,
if(ekl_service_facility_id IS NOT NULL,
-- Shared
if(a.dispatched_to_vendor_first_datetime between concat(to_date(dispatched_to_vendor_first_datetime)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1] is not null,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))) and concat(to_date(dispatched_to_vendor_first_datetime)," ",if(split(T33.LPHT_FC_Shared,'\\.')[1] is not null,concat(split(T33.LPHT_FC_Shared,'\\.')[0],':','30',':','00'),concat(split(T33.LPHT_FC_Shared,'\\.')[0],':','00',':','00'))),'Design-Breach','3PL-Breach'),
-- Independent
if(a.dispatched_to_vendor_first_datetime between concat(to_date(dispatched_to_vendor_first_datetime)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1] is not null,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))) and concat(to_date(dispatched_to_vendor_first_datetime)," ",if(split(T33.LPHT_FC_Independent,'\\.')[1] is not null,concat(split(T33.LPHT_FC_Independent,'\\.')[0],':','30',':','00'),concat(split(T33.LPHT_FC_Independent,'\\.')[0],':','00',':','00'))),'Design-Breach','3PL-Breach')),'3PL-RCA-Breach'),'3PL-RCA-Breach')))),'Not_defined_LPB_Return') as revised_tpl_lp_breach_bucket,

if(shipment_movement_type='Outgoing',
if(a.delivered_first_datetime<=a.design_sla,'Delivered_within_promise',
if(lookup_date(a.out_for_delivery_first_datetime)<=lookup_date(a.design_sla) or lookup_date(a.undelivered_attempted_first_datetime)<=lookup_date(a.design_sla),'Customer_Dependency-Attempted Undelivered-3PL',
-- fa
if(lower(item_seller_type) in ('mp_fbf_seller'),
if(T22.destination_zone is not null,
if(ekl_service_facility_id IS NOT NULL,
-- Shared
if(a.dispatched_to_vendor_first_datetime between concat(to_date(dispatched_to_vendor_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1] is not null,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))) and concat(to_date(dispatched_to_vendor_first_datetime)," ",if(split(T22.LPHT_FC_Shared,'\\.')[1] is not null,concat(split(T22.LPHT_FC_Shared,'\\.')[0],':','30',':','00'),concat(split(T22.LPHT_FC_Shared,'\\.')[0],':','00',':','00'))),'Design-Breach',if(cast((unix_timestamp(in_scan_at_hub_first_datetime)-unix_timestamp(dispatched_to_vendor_first_datetime)) / 3600  as int)>cast(in_scan_cut_off as int),"3PL First Mile Breach",if(cast((unix_timestamp(in_transit_first_datetime)-unix_timestamp(in_scan_at_hub_first_datetime)) / 3600  as int)>cast(in_transit_cut_off as int),"3PL Connection breach",
if(to_date(received_at_delivery_hub_first_datetime)<=to_date(design_sla),
if(received_at_delivery_hub_first_datetime<=concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime),'3PL Last Mile breach',if(received_at_delivery_hub_first_datetime>concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(received_at_delivery_hub_first_datetime)<to_date(design_sla) and (to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime) or to_date(out_for_delivery_first_datetime)<>to_date(date_add(received_at_delivery_hub_first_datetime,1))),'3PL Last Mile breach',if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach'))),if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach'))))),
-- Independent
if(a.dispatched_to_vendor_first_datetime between concat(to_date(dispatched_to_vendor_first_datetime)," ",if(split(T22.Vendor_Cutoff_MH,'\\.')[1] is not null,concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T22.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))) and concat(to_date(dispatched_to_vendor_first_datetime)," ",if(split(T22.LPHT_FC_Independent,'\\.')[1] is not null,concat(split(T22.LPHT_FC_Independent,'\\.')[0],':','30',':','00'),concat(split(T22.LPHT_FC_Independent,'\\.')[0],':','00',':','00'))),'Design-Breach',if(cast((unix_timestamp(in_scan_at_hub_first_datetime)-unix_timestamp(dispatched_to_vendor_first_datetime)) / 3600  as int)>cast(in_scan_cut_off as int),"3PL First Mile Breach",if(cast((unix_timestamp(in_transit_first_datetime)-unix_timestamp(in_scan_at_hub_first_datetime)) / 3600  as int)>cast(in_transit_cut_off as int),"3PL Connection breach",
if(to_date(received_at_delivery_hub_first_datetime)<=to_date(design_sla),
if(received_at_delivery_hub_first_datetime<=concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime),'3PL Last Mile breach',if(received_at_delivery_hub_first_datetime>concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(received_at_delivery_hub_first_datetime)<to_date(design_sla) and (to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime) or to_date(out_for_delivery_first_datetime)<>to_date(date_add(received_at_delivery_hub_first_datetime,1))),'3PL Last Mile breach',if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach'))),if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach')))))),'3PL-RCA-Breach'),

-- non-fa
if(lower(item_seller_type)= 'mp_non_fbf_seller',
if(T33.destination_zone is not null,
if(ekl_service_facility_id IS NOT NULL,
-- Shared
if(a.dispatched_to_vendor_first_datetime between concat(to_date(dispatched_to_vendor_first_datetime)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1] is not null,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))) and concat(to_date(dispatched_to_vendor_first_datetime)," ",if(split(T33.LPHT_FC_Shared,'\\.')[1] is not null,concat(split(T33.LPHT_FC_Shared,'\\.')[0],':','30',':','00'),concat(split(T33.LPHT_FC_Shared,'\\.')[0],':','00',':','00'))),'Design-Breach',if(cast((unix_timestamp(in_scan_at_hub_first_datetime)-unix_timestamp(dispatched_to_vendor_first_datetime)) / 3600  as int)>cast(in_scan_cut_off as int),"3PL First Mile Breach",if(cast((unix_timestamp(in_transit_first_datetime)-unix_timestamp(in_scan_at_hub_first_datetime)) / 3600  as int)>cast(in_transit_cut_off as int),"3PL Connection breach",
if(to_date(received_at_delivery_hub_first_datetime)<=to_date(design_sla),
if(received_at_delivery_hub_first_datetime<=concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime),'3PL Last Mile breach',if(received_at_delivery_hub_first_datetime>concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(received_at_delivery_hub_first_datetime)<to_date(design_sla) and (to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime) or to_date(out_for_delivery_first_datetime)<>to_date(date_add(received_at_delivery_hub_first_datetime,1))),'3PL Last Mile breach',if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach'))),if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach'))))),
-- Independent
if(a.dispatched_to_vendor_first_datetime between concat(to_date(dispatched_to_vendor_first_datetime)," ",if(split(T33.Vendor_Cutoff_MH,'\\.')[1] is not null,concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','30',':','00'),concat(split(T33.Vendor_Cutoff_MH,'\\.')[0],':','00',':','00'))) and concat(to_date(dispatched_to_vendor_first_datetime)," ",if(split(T33.LPHT_FC_Independent,'\\.')[1] is not null,concat(split(T33.LPHT_FC_Independent,'\\.')[0],':','30',':','00'),concat(split(T33.LPHT_FC_Independent,'\\.')[0],':','00',':','00'))),'Design-Breach',if(cast((unix_timestamp(in_scan_at_hub_first_datetime)-unix_timestamp(dispatched_to_vendor_first_datetime)) / 3600  as int)>cast(in_scan_cut_off as int),"3PL First Mile Breach",if(cast((unix_timestamp(in_transit_first_datetime)-unix_timestamp(in_scan_at_hub_first_datetime)) / 3600  as int)>cast(in_transit_cut_off as int),"3PL Connection breach",
if(to_date(received_at_delivery_hub_first_datetime)<=to_date(design_sla),
if(received_at_delivery_hub_first_datetime<=concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime),'3PL Last Mile breach',if(received_at_delivery_hub_first_datetime>concat(received_at_delivery_hub_first_datetime,' 14:30:00') and to_date(received_at_delivery_hub_first_datetime)<to_date(design_sla) and (to_date(out_for_delivery_first_datetime)<>to_date(received_at_delivery_hub_first_datetime) or to_date(out_for_delivery_first_datetime)<>to_date(date_add(received_at_delivery_hub_first_datetime,1))),'3PL Last Mile breach',if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach'))),if(mh_cut_offs.vendor_name is not null,'3PL Line Haul Breach','3PL-Breach')))))),'3PL-RCA-Breach'),'3PL-RCA-Breach')))),'Not_defined_LPB_Return') as revised_tpl_lp_breach_bucket_l2,
pickup_failed_first_attribution,
pickup_failed_second_attribution,
pickup_failed_third_attribution,
pickup_failed_fourth_attribution,
pickup_failed_fifth_attribution,
pickup_failed_sixth_attribution,
out_for_delivery_fourth_datetime,
out_for_delivery_fifth_datetime,
out_for_delivery_sixth_datetime,
out_for_delivery_seventh_datetime,
undelivered_attempted_second_datetime,
undelivered_attempted_third_datetime,
undelivered_attempted_fourth_datetime,
undelivered_attempted_fifth_datetime,
undelivered_attempted_sixth_datetime,
undelivered_attempted_seventh_datetime,
undelivered_attempted_third_secondary_status,
undelivered_attempted_fourth_secondary_status,
undelivered_attempted_fifth_secondary_status,
undelivered_attempted_sixth_secondary_status,
undelivered_attempted_seventh_secondary_status,
rto_confirmed_second_datetime,
rto_confirmed_last_datetime,
rto_cancelled_first_datetime,
rto_cancelled_second_datetime,
rto_cancelled_last_datetime,
if(datediff(delivered_first_datetime,customer_sla)>0,datediff(delivered_first_datetime,customer_sla),0) as breach_index_num,
if(datediff(delivered_first_datetime,customer_sla)>0,1,0) as breach_index_den,
if(datediff(rto_confirmed_first_datetime,design_sla)>0 and datediff(out_for_delivery_first_datetime,design_sla)>0 and datediff(undelivered_attempted_first_datetime,design_sla)>0,1,0) as rto_after_lp_flag,
if(datediff(rto_confirmed_first_datetime,design_sla)<=0 and (rto_confirmed_first_attribution='Vendor' or rto_confirmed_second_attribution='Vendor' or rto_confirmed_last_attribution='Vendor'),1,0) as rto_before_lp_flag,
datediff(delivered_first_datetime,dtv) as d1_d2_delivered_flag,
datediff(first_attempt,dtv) as d1_d2_attempt_flag,
if(datediff(undelivered_attempted_first_datetime,design_sla)<=0 and (undelivered_attempted_first_attribution='Vendor' or undelivered_attempted_first_attribution='Vendor' or undelivered_attempted_first_attribution='Vendor'),1,0) as  CD_LP_flag,
if(datediff(delivered_first_datetime,order_item_approve_date_time)>8,'8+',datediff(delivered_first_datetime,order_item_approve_date_time)) as d1_d2_o2d,
if(datediff(design_sla,order_item_approve_date_time)>8,'8+',datediff(design_sla,order_item_approve_date_time)) as d1_d2_design_sla,
if(datediff(delivered_first_datetime,dtv)>8,'8+',datediff(delivered_first_datetime,dtv)) as d1_d2_dd_o2d,
if(datediff(design_sla,dtv)>8,'8+',datediff(design_sla,dtv)) as d1_d2_design_dd_sla,
dtv,
order_item_service_profile,
if(datediff(customer_sla,order_item_approve_date_time)>8,'8+',datediff(design_sla,order_item_approve_date_time)) as d1_d2_sla,
if(datediff(customer_sla,dtv)>8,'8+',datediff(design_sla,dtv)) as d1_d2_dd_sla

from
(
select 
status.sr_id,
max(rvp_out_for_pickup_last_datetime) as rvp_out_for_pickup_last_datetime,
max(status.shipment_reference_ids) as shipment_reference_ids,
max(shipment_current_status) as shipment_current_status,
max(shipment_current_secondary_status) as shipment_current_secondary_status,
max(shipment_current_status_date_time) as shipment_current_status_date_time,
max(shipment_current_location) as shipment_current_location,
max(shipment_current_remarks) as shipment_current_remarks,
max(pickup_created_first_datetime) as pickup_created_first_datetime,
max(out_for_pickup_first_datetime) as out_for_pickup_first_datetime,
max(pickup_completed_first_datetime) as pickup_completed_first_datetime,
max(pickup_failed_first_datetime) as pickup_failed_first_datetime,
max(pickup_cancelled_first_datetime) as pickup_cancelled_first_datetime,
max(in_scan_at_hub_first_datetime) as in_scan_at_hub_first_datetime,
max(in_transit_first_datetime) as in_transit_first_datetime,
max(received_at_delivery_hub_first_datetime) as received_at_delivery_hub_first_datetime,
max(out_for_delivery_first_datetime) as out_for_delivery_first_datetime,
max(undelivered_attempted_first_datetime) as undelivered_attempted_first_datetime,
max(delivered_first_datetime) as delivered_first_datetime,
max(damaged_first_datetime) as damaged_first_datetime,
max(lost_first_datetime) as lost_first_datetime,
max(update_lpd_first_datetime) as update_lpd_first_datetime,
max(ready_to_dispatch_first_datetime) as ready_to_dispatch_first_datetime,
max(rvp_scheduled_first_datetime) as rvp_scheduled_first_datetime,
max(rvp_out_for_pickup_first_datetime) as rvp_out_for_pickup_first_datetime,
max(rvp_pickup_completed_first_datetime) as rvp_pickup_completed_first_datetime,
max(rvp_pickup_failed_first_datetime) as rvp_pickup_failed_first_datetime,
max(rvp_cancelled_first_datetime) as rvp_cancelled_first_datetime,
max(rvp_in_scan_at_hub_first_datetime) as rvp_in_scan_at_hub_first_datetime,
max(rvp_in_transit_first_datetime) as rvp_in_transit_first_datetime,
max(rvp_received_at_delivery_hub_first_datetime) as rvp_received_at_delivery_hub_first_datetime,
max(rvp_out_for_delivery_first_datetime) as rvp_out_for_delivery_first_datetime,
max(rvp_undelivered_attempted_first_datetime) as rvp_undelivered_attempted_first_datetime,
max(rvp_completed_first_datetime) as rvp_completed_first_datetime,
max(rto_confirmed_first_datetime) as rto_confirmed_first_datetime,
max(rto_received_at_delivery_hub_first_datetime) as rto_received_at_delivery_hub_first_datetime,
max(rto_out_for_delivery_first_datetime) as rto_out_for_delivery_first_datetime,
max(rto_undelivered_attempted_first_datetime) as rto_undelivered_attempted_first_datetime,
max(rto_completed_first_datetime) as rto_completed_first_datetime,
max(pickup_failed_first_secondary_status) as pickup_failed_first_secondary_status,
max(undelivered_attempted_first_secondary_status) as undelivered_attempted_first_secondary_status,
max(rvp_undelivered_attempted_first_secondary_status) as rvp_undelivered_attempted_first_secondary_status,
max(pickup_failed_first_remarks) as pickup_failed_first_remarks,
max(undelivered_attempted_first_remarks) as undelivered_attempted_first_remarks,
max(rvp_undelivered_attempted_first_remarks) as rvp_undelivered_attempted_first_remarks,
max(pickup_failed_second_datetime) as pickup_failed_second_datetime,
max(pickup_failed_second_secondary_status) as pickup_failed_second_secondary_status,
max(pickup_failed_second_remarks) as pickup_failed_second_remarks,
max(vendor_tag) as vendor_tag,
max(design_sla) as design_sla,
max(customer_sla) as customer_sla,
max(service.created_at) as created_at,
max(transit_type) as transit_type,
max(shipment_movement_type) as shipment_movement_type,
max(payment_type) as payment_type,
max(updated_at) as updated_at,
max(size) as size,
max(vendor_tracking_id) as vendor_tracking_id,
max(destination_pincode) as destination_pincode,
max(destination_type) as destination_type,
max(shipment_currency) as shipment_currency,
max(shipment_value) as shipment_value,
max(amount_to_collect_currency) as amount_to_collect_currency,
max(amount_to_collect_value) as amount_to_collect_value,
max(source_pincode) as source_pincode,
max(source_type) as source_type,
max(order_item_unit_tracking_id) as order_item_unit_tracking_id,
max(order_item_unit_rts_breach) as order_item_unit_rts_breach,
max(order_item_unit_deliver_date_key) as order_item_unit_deliver_date_key,
max(order_item_unit_deliver_time_key) as order_item_unit_deliver_time_key,
max(order_item_unit_ready_to_ship_time_key) as order_item_unit_ready_to_ship_time_key,
max(order_item_unit_ready_to_ship_date_key) as order_item_unit_ready_to_ship_date_key,
max(item.order_id) as order_id,
max(T3.order_external_id) as order_external_id,
max(T3.order_item_unit_id) as order_item_unit_id,
max(item.order_item_id) as order_item_id,
max(T3.order_item_unit_shipment_id) as order_item_unit_shipment_id,

max(if(to_date(delivered_first_datetime)<=to_date(customer_sla),'Delivered_within_promise', 
if(to_date(out_for_delivery_first_datetime)<=to_date(customer_sla) or to_date(undelivered_attempted_first_datetime)<=to_date(customer_sla),'Customer_Dependency-Attempted Undelivered',
if(T3.order_item_unit_rts_breach = 'Breach' , 'RTS Breach',
if( unit_hive.fulfill_item_unit_dispatch_service_tier='EXPRESS', 
if(((status.out_for_pickup_first_datetime='' or status.out_for_pickup_first_datetime is null) and (status.pickup_completed_first_datetime='' or status.pickup_completed_first_datetime is null)) or ((lookup_time(update_lpd_first_datetime)<=1000) and (lookup_date(status.out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR
((lookup_time(update_lpd_first_datetime)>1400) and ((lookup_date(status.out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(status.out_for_pickup_first_datetime)<> lookup_date(date_add(update_lpd_first_datetime,1)))))
,'3PL First Mile Breach',
if(((lookup_time(update_lpd_first_datetime)<=1400) and (lookup_date(status.pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR (status.pickup_completed_first_datetime='' or status.pickup_completed_first_datetime is null) OR
((lookup_time(update_lpd_first_datetime)>1400) and ((lookup_date(status.pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(status.pickup_completed_first_datetime) <> lookup_date(date_add(update_lpd_first_datetime,1))) ))
,'Pick Up Breach',
if( (status.in_transit_first_datetime='' or status.in_transit_first_datetime is null) OR (lookup_date(status.pickup_completed_first_datetime) <> lookup_date(status.in_transit_first_datetime)),'3PL Connection breach',
if(((lookup_time(status.received_at_delivery_hub_first_datetime) < 1430) and (lookup_date(status.out_for_delivery_first_datetime) > lookup_date(status.received_at_delivery_hub_first_datetime))),'3PL Last Mile breach','3PL Line Haul Breach')))),
if(((status.out_for_pickup_first_datetime='' or status.out_for_pickup_first_datetime is null) and (status.pickup_completed_first_datetime='' or status.pickup_completed_first_datetime is null)) or ((lookup_time(update_lpd_first_datetime)<=1000) and (lookup_date(status.out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime))) OR
((lookup_time(update_lpd_first_datetime)>1000) and ((lookup_date(status.out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(status.out_for_pickup_first_datetime)<> lookup_date(date_add(update_lpd_first_datetime,1)))))
,'3PL First Mile Breach',
if(((lookup_time(update_lpd_first_datetime)<=1000) and (lookup_date(status.pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR (status.pickup_completed_first_datetime='' or status.pickup_completed_first_datetime is null) OR
((lookup_time(update_lpd_first_datetime)>1000) and ((lookup_date(status.pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(status.pickup_completed_first_datetime) <> lookup_date(date_add(update_lpd_first_datetime,1))) ))
,'Pick Up Breach',
if( (status.in_transit_first_datetime='' or status.in_transit_first_datetime is null) OR (lookup_date(status.pickup_completed_first_datetime) <> lookup_date(status.in_transit_first_datetime)),'3PL Connection breach',
if(((lookup_time(status.received_at_delivery_hub_first_datetime) < 1430) and (lookup_date(status.out_for_delivery_first_datetime) > lookup_date(status.received_at_delivery_hub_first_datetime))) ,'3PL Last Mile breach','3PL Line Haul Breach'))))))))) as e2e_cp_breach_bucket,
-- update_lpd
-- duplicates e2e and 
max(if(to_date(delivered_first_datetime)<=to_date(design_sla),'Delivered_within_promise', 
if(to_date(out_for_delivery_first_datetime)<=to_date(design_sla) or to_date(undelivered_attempted_first_datetime)<=to_date(design_sla),'Customer_Dependency-Attempted Undelivered',	
if(unit_hive.fulfill_item_unit_dispatch_service_tier='EXPRESS',
if(((status.out_for_pickup_first_datetime='' or status.out_for_pickup_first_datetime is null) and (status.pickup_completed_first_datetime='' or status.pickup_completed_first_datetime is null)) or ((lookup_time(update_lpd_first_datetime)<=1400) and (lookup_date(status.out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime))) OR
((lookup_time(update_lpd_first_datetime)>1400) and ((lookup_date(status.out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(status.out_for_pickup_first_datetime)<> lookup_date(date_add(update_lpd_first_datetime,1))) ))
,'3PL First Mile Breach-2',
if(((lookup_time(update_lpd_first_datetime)<=1400) and (lookup_date(status.pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR (status.pickup_completed_first_datetime='' or status.pickup_completed_first_datetime is null) OR
((lookup_time(update_lpd_first_datetime)>1400) and ((lookup_date(status.pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(status.pickup_completed_first_datetime) <> lookup_date(date_add(update_lpd_first_datetime,1))) ))
,'Pick Up Breach',
if((status.in_transit_first_datetime='' or status.in_transit_first_datetime is null) OR (lookup_date(status.pickup_completed_first_datetime) <> lookup_date(status.in_transit_first_datetime)),'3PL Connection breach',
if(((lookup_time(status.received_at_delivery_hub_first_datetime) < 1430) and (lookup_date(status.out_for_delivery_first_datetime) > lookup_date(status.received_at_delivery_hub_first_datetime))) ,'3PL Last Mile breach','3PL Line Haul Breach')))),

if(((status.out_for_pickup_first_datetime='' or status.out_for_pickup_first_datetime is null) and (status.pickup_completed_first_datetime='' or status.pickup_completed_first_datetime is null)) or ((lookup_time(update_lpd_first_datetime)<=1000) and (lookup_date(status.out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime))) OR
((lookup_time(update_lpd_first_datetime)>1000) and ((lookup_date(status.out_for_pickup_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(status.out_for_pickup_first_datetime)<> lookup_date(date_add(update_lpd_first_datetime,1))) ))
,'3PL First Mile Breach-2',
if(((lookup_time(update_lpd_first_datetime)<=1000) and (lookup_date(status.pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) ) OR (status.pickup_completed_first_datetime='' or status.pickup_completed_first_datetime is null) OR
((lookup_time(update_lpd_first_datetime)>1000) and ((lookup_date(status.pickup_completed_first_datetime) <> lookup_date(update_lpd_first_datetime)) and (lookup_date(status.pickup_completed_first_datetime) <> lookup_date(date_add(update_lpd_first_datetime,1))) ))
,'Pick Up Breach',
if((status.in_transit_first_datetime='' or status.in_transit_first_datetime is null) OR (lookup_date(status.pickup_completed_first_datetime) <> lookup_date(status.in_transit_first_datetime)),'3PL Connection breach',
if(((lookup_time(status.received_at_delivery_hub_first_datetime) < 1430) and (lookup_date(status.out_for_delivery_first_datetime) > lookup_date(status.received_at_delivery_hub_first_datetime))) ,'3PL Last Mile breach','3PL Line Haul Breach')))))))) as e2e_lp_breach_bucket,



min(if(out_for_pickup_first_datetime <= pickup_failed_first_datetime,pickup_failed_first_datetime, pickup_failed_second_datetime)) as pickup_failed_datetime,
min(if(out_for_pickup_first_datetime <= pickup_failed_first_datetime, pickup_failed_first_secondary_status, pickup_failed_second_secondary_status)) as pickup_failed_status,
min(if(out_for_pickup_first_datetime <= pickup_failed_first_datetime,pickup_failed_first_remarks,pickup_failed_second_remarks)) as pickup_failed_remarks,


min(if(out_for_pickup_first_datetime <= pickup_failed_first_datetime,
if(pickup_failed_first_secondary_status is null,'Not_defined',if(lower(trim(pickup_failed_first_secondary_status)) in ('seller did not handover','seller premises closed','seller shifted','package substandard','seller forms unavailable','label of substandard quality','seller rejection','seller unavailable','seller no response','request to reschedule','address pincode mismatch','incomplete address','request for address change','address related issue','shipment damaged'),'Seller',if(lower(trim(pickup_failed_first_secondary_status)) in ('vendor did not pickup','pickup area not accessible','non serviceable','out of delivery area','vendor delay','misrouted','capacity issues'),'Vendor',
if(lower(trim(pickup_failed_first_secondary_status)) in ('shipment data unavailable','reattempt','dispute','delay beyond control'),'Flipkart',
if(lower(trim(pickup_failed_first_secondary_status)) in ('duplicate pickup request','delivery area not accessible'),'None','RCA_needed'))
)
)),
if(pickup_failed_second_secondary_status is null,'Not_defined',if(lower(trim(pickup_failed_second_secondary_status)) in ('seller did not handover','seller premises closed','seller shifted','package substandard','seller forms unavailable','label of substandard quality','seller rejection','seller unavailable','seller no response','request to reschedule','address pincode mismatch','incomplete address','request for address change','address related issue','shipment damaged'),'Seller',if(lower(trim(pickup_failed_second_secondary_status)) in ('vendor did not pickup','pickup area not accessible','non serviceable','out of delivery area','vendor delay','misrouted','capacity issues'),'Vendor',
if(lower(trim(pickup_failed_second_secondary_status)) in ('shipment data unavailable','reattempt','dispute','delay beyond control'),'Flipkart',
if(lower(trim(pickup_failed_second_secondary_status)) in ('duplicate pickup request','delivery area not accessible'),'None','RCA_needed'))
)
))
)) as pickup_failed_attribution,
min(out_for_pickup_second_datetime) as out_for_pickup_second_datetime,
min(out_for_pickup_third_datetime) as out_for_pickup_third_datetime,
min(out_for_pickup_fourth_datetime) as out_for_pickup_fourth_datetime,
min(out_for_pickup_fifth_datetime) as out_for_pickup_fifth_datetime,
min(out_for_pickup_sixth_datetime) as out_for_pickup_sixth_datetime,
min(pickup_failed_third_datetime) as pickup_failed_third_datetime,
min(pickup_failed_fourth_datetime) as pickup_failed_fourth_datetime,
min(pickup_failed_fifth_datetime) as pickup_failed_fifth_datetime,
min(pickup_failed_sixth_datetime) as pickup_failed_sixth_datetime,
min(pickup_failed_last_datetime) as pickup_failed_last_datetime,

min(if(pickup_failed_first_secondary_status is null,'Not_defined',if(lower(trim(pickup_failed_first_secondary_status)) in ('seller did not handover','seller premises closed','seller shifted','package substandard','seller forms unavailable','label of substandard quality','seller rejection','seller unavailable','seller no response','request to reschedule','address pincode mismatch','incomplete address','request for address change','address related issue','shipment damaged'),'Seller',if(lower(trim(pickup_failed_first_secondary_status)) in ('vendor did not pickup','pickup area not accessible','non serviceable','out of delivery area','vendor delay','misrouted','capacity issues'),'Vendor',
if(lower(trim(pickup_failed_first_secondary_status)) in ('shipment data unavailable','reattempt','dispute','delay beyond control'),'Flipkart',
if(lower(trim(pickup_failed_first_secondary_status)) in ('duplicate pickup request','delivery area not accessible'),'None','RCA_needed'))
)
))) as pickup_failed_first_attribution,
min(if(pickup_failed_fourth_secondary_status is null,'Not_defined',if(lower(trim(pickup_failed_fourth_secondary_status)) in ('seller did not handover','seller premises closed','seller shifted','package substandard','seller forms unavailable','label of substandard quality','seller rejection','seller unavailable','seller no response','request to reschedule','address pincode mismatch','incomplete address','request for address change','address related issue','shipment damaged'),'Seller',if(lower(trim(pickup_failed_fourth_secondary_status)) in ('vendor did not pickup','pickup area not accessible','non serviceable','out of delivery area','vendor delay','misrouted','capacity issues'),'Vendor',
if(lower(trim(pickup_failed_fourth_secondary_status)) in ('shipment data unavailable','reattempt','dispute','delay beyond control'),'Flipkart',
if(lower(trim(pickup_failed_fourth_secondary_status)) in ('duplicate pickup request','delivery area not accessible'),'None','RCA_needed'))
)
))) as pickup_failed_fourth_attribution,

min(if(pickup_failed_second_secondary_status is null,'Not_defined',if(lower(trim(pickup_failed_second_secondary_status)) in ('seller did not handover','seller premises closed','seller shifted','package substandard','seller forms unavailable','label of substandard quality','seller rejection','seller unavailable','seller no response','request to reschedule','address pincode mismatch','incomplete address','request for address change','address related issue','shipment damaged'),'Seller',if(lower(trim(pickup_failed_second_secondary_status)) in ('vendor did not pickup','pickup area not accessible','non serviceable','out of delivery area','vendor delay','misrouted','capacity issues'),'Vendor',
if(lower(trim(pickup_failed_second_secondary_status)) in ('shipment data unavailable','reattempt','dispute','delay beyond control'),'Flipkart',
if(lower(trim(pickup_failed_second_secondary_status)) in ('duplicate pickup request','delivery area not accessible'),'None','RCA_needed'))
)
))) as pickup_failed_second_attribution,

min(if(pickup_failed_third_secondary_status is null,'Not_defined',if(lower(trim(pickup_failed_third_secondary_status)) in ('seller did not handover','seller premises closed','seller shifted','package substandard','seller forms unavailable','label of substandard quality','seller rejection','seller unavailable','seller no response','request to reschedule','address pincode mismatch','incomplete address','request for address change','address related issue','shipment damaged'),'Seller',if(lower(trim(pickup_failed_third_secondary_status)) in ('vendor did not pickup','pickup area not accessible','non serviceable','out of delivery area','vendor delay','misrouted','capacity issues'),'Vendor',
if(lower(trim(pickup_failed_third_secondary_status)) in ('shipment data unavailable','reattempt','dispute','delay beyond control'),'Flipkart',
if(lower(trim(pickup_failed_third_secondary_status)) in ('duplicate pickup request','delivery area not accessible'),'None','RCA_needed'))
)
))) as pickup_failed_third_attribution,

min(if(pickup_failed_fifth_secondary_status is null,'Not_defined',if(lower(trim(pickup_failed_fifth_secondary_status)) in ('seller did not handover','seller premises closed','seller shifted','package substandard','seller forms unavailable','label of substandard quality','seller rejection','seller unavailable','seller no response','request to reschedule','address pincode mismatch','incomplete address','request for address change','address related issue','shipment damaged'),'Seller',if(lower(trim(pickup_failed_fifth_secondary_status)) in ('vendor did not pickup','pickup area not accessible','non serviceable','out of delivery area','vendor delay','misrouted','capacity issues'),'Vendor',
if(lower(trim(pickup_failed_fifth_secondary_status)) in ('shipment data unavailable','reattempt','dispute','delay beyond control'),'Flipkart',
if(lower(trim(pickup_failed_fifth_secondary_status)) in ('duplicate pickup request','delivery area not accessible'),'None','RCA_needed'))
)
))) as pickup_failed_fifth_attribution,

min(if(pickup_failed_sixth_secondary_status is null,'Not_defined',if(lower(trim(pickup_failed_sixth_secondary_status)) in ('seller did not handover','seller premises closed','seller shifted','package substandard','seller forms unavailable','label of substandard quality','seller rejection','seller unavailable','seller no response','request to reschedule','address pincode mismatch','incomplete address','request for address change','address related issue','shipment damaged'),'Seller',if(lower(trim(pickup_failed_sixth_secondary_status)) in ('vendor did not pickup','pickup area not accessible','non serviceable','out of delivery area','vendor delay','misrouted','capacity issues'),'Vendor',
if(lower(trim(pickup_failed_sixth_secondary_status)) in ('shipment data unavailable','reattempt','dispute','delay beyond control'),'Flipkart',
if(lower(trim(pickup_failed_sixth_secondary_status)) in ('duplicate pickup request','delivery area not accessible'),'None','RCA_needed'))
)
))) as pickup_failed_sixth_attribution,
 max(lookupkey('pincode',source_pincode)) as source_pincode_key,
 max(lookupkey('pincode',destination_pincode)) as destination_pincode_key,
 max(lookupkey('seller_id',seller_id)) as seller_id_key,
 min(business_date_diff(delivered_first_datetime,update_lpd_first_datetime)) as buss_delivered_rts_diff,
 min(datediff(delivered_first_datetime,update_lpd_first_datetime)) as delivered_rts_diff,
 max(IF(Upper(geo_src.src_lt) = Upper(geo_dest.dest_lt), "INTRACITY", IF(Upper(geo_src.src_zone) = Upper(geo_dest.dest_zone), "INTRAZONE", IF(Upper(geo_src.src_zone) <> Upper(geo_dest.dest_zone), "INTERZONE", "Missing")))) as ekl_fin_zone,
 max(IF(Upper(geo_src.src_lt) = Upper(geo_dest.dest_lt), "Local", IF(Upper(geo_src.src_zone) = Upper(geo_dest.dest_zone),"Zonal", IF(Upper(geo_src.src_zone) <> Upper(geo_dest.dest_zone), "National", "National")))) as ekl_fin_zone_tmp,
 
 max(item.shipping_category) as shipping_category,
 min(if(lower(shipment_current_status) in ('lost','marked_for_merchant_dispatch','dispatched_to_merchant','received_by_merchant','marked_for_seller_return','dispatched_to_seller','returned_to_seller') or lower(shipment_current_status) like '%rto%' or lower(shipment_current_status) like '%rt:in%' or (lower(shipment_current_status) like '%rvp%' and shipment_movement_type='Outgoing'),1,0)) as rto_flag,
 max(wdecision_length) as wms_length,
 max(wdecision_breadth) as wms_breadth,
 max(wdecision_height) as wms_height,
 max(wdecision_length*wdecision_breadth*wdecision_height/5000) as volumetric_weight,
 max(wdecision_weight) as wms_dead_weight,
 max(unit_hive.fulfill_item_unit_dispatch_service_tier) as fulfill_item_unit_dispatch_service_tier,
max(item.cms_category) as item_cms_category,
max(item.cms_vertical) as item_cms_vertical,	
max(item.invoice_number) as item_invoice_number,
max(item.item_content) as item_item_content,
max(item.listing_breadth) as item_listing_breadth,
max(item.listing_height) as item_listing_height,
max(item.listing_length) as item_listing_length,
max(item.listing_weight) as item_listing_weight,
max(item.lpe_tier) as item_lpe_tier,
max(item.product_currency) as item_product_currency,
max(item.product_value) as item_product_value,
max(item.product_breadth) as item_product_breadth,
max(item.product_height) as item_product_height,
max(item.product_length) as item_product_length,
max(item.product_weight) as item_product_weight,
max(item.product_id) as item_product_id,
max(item.product_title) as item_product_title,
max(item.rolled_quantity) as item_quantity,
max(item.seller_pincode) as item_seller_pincode,
max(item.seller_id) as item_seller_id,
max(item.seller_type) as item_seller_type,
max(item.tax_per_unit_currency) as item_tax_per_unit_currency,
max(item.tax_per_unit_value) as item_tax_per_unit_value,
max(item.total_tax_currency) as item_total_tax_currency,
max(item.total_tax_value) as item_total_tax_value,
max(item.is_dangerous) as item_is_dangerous,
min(out_for_delivery_third_datetime) as  out_for_delivery_third_datetime,
min(dispatched_to_facility_first_datetime) as  dispatched_to_facility_first_datetime,
min(dispatched_to_facility_first_status_location) as  dispatched_to_facility_first_status_location,
min(dispatched_to_merchant_first_datetime) as  dispatched_to_merchant_first_datetime,
min(dispatched_to_merchant_first_status_location) as  dispatched_to_merchant_first_status_location,
min(dispatched_to_seller_first_datetime) as  dispatched_to_seller_first_datetime,
min(dispatched_to_seller_first_status_location) as  dispatched_to_seller_first_status_location,
min(dispatched_to_tc_first_datetime) as  dispatched_to_tc_first_datetime,
min(dispatched_to_tc_first_status_location) as  dispatched_to_tc_first_status_location,
min(dispatched_to_vendor_first_datetime) as  dispatched_to_vendor_first_datetime,
min(dispatched_to_vendor_first_status_location) as  dispatched_to_vendor_first_status_location,
min(dispatch_failed_first_datetime) as  dispatch_failed_first_datetime,
min(dispatch_failed_first_status_location) as  dispatch_failed_first_status_location,
min(expected_first_datetime) as  expected_first_datetime,
min(expected_first_status_location) as  expected_first_status_location,
min(marked_for_merchant_dispatch_first_datetime) as  marked_for_merchant_dispatch_first_datetime,
min(marked_for_merchant_dispatch_first_status_location) as  marked_for_merchant_dispatch_first_status_location,
min(marked_for_reshipment_first_datetime) as  marked_for_reshipment_first_datetime,
min(marked_for_reshipment_first_status_location) as  marked_for_reshipment_first_status_location,
min(marked_for_seller_return_first_datetime) as  marked_for_seller_return_first_datetime,
min(marked_for_seller_return_first_status_location) as  marked_for_seller_return_first_status_location,
min(marked_reshipment_approved_first_datetime) as  marked_reshipment_approved_first_datetime,
min(marked_reshipment_approved_first_status_location) as  marked_reshipment_approved_first_status_location,
min(not_received_first_datetime) as  not_received_first_datetime,
min(not_received_first_status_location) as  not_received_first_status_location,
min(pickup_complete_first_datetime) as  pickup_complete_first_datetime,
min(pickup_complete_first_status_location) as  pickup_complete_first_status_location,
min(pickup_leg_completed_first_datetime) as  pickup_leg_completed_first_datetime,
min(pickup_leg_completed_first_status_location) as  pickup_leg_completed_first_status_location,
min(pickup_out_for_pickup_first_datetime) as  pickup_out_for_pickup_first_datetime,
min(pickup_out_for_pickup_first_status_location) as  pickup_out_for_pickup_first_status_location,
min(pickup_reattempt_first_datetime) as  pickup_reattempt_first_datetime,
min(pickup_reattempt_first_status_location) as  pickup_reattempt_first_status_location,
min(pickup_scheduled_first_datetime) as  pickup_scheduled_first_datetime,
min(pickup_scheduled_first_status_location) as  pickup_scheduled_first_status_location,
min(ready_for_pickup_first_datetime) as  ready_for_pickup_first_datetime,
min(ready_for_pickup_first_status_location) as  ready_for_pickup_first_status_location,
min(received_first_datetime) as  received_first_datetime,
min(received_first_status_location) as  received_first_status_location,
min(received_by_merchant_first_datetime) as  received_by_merchant_first_datetime,
min(received_by_merchant_first_status_location) as  received_by_merchant_first_status_location,
min(received_by_seller_first_datetime) as  received_by_seller_first_datetime,
min(received_by_seller_first_status_location) as  received_by_seller_first_status_location,
min(received_with_error_first_datetime) as  received_with_error_first_datetime,
min(received_with_error_first_status_location) as  received_with_error_first_status_location,
min(request_for_cancellation_first_datetime) as  request_for_cancellation_first_datetime,
min(request_for_cancellation_first_status_location) as  request_for_cancellation_first_status_location,
min(request_for_reschedule_first_datetime) as  request_for_reschedule_first_datetime,
min(request_for_reschedule_first_status_location) as  request_for_reschedule_first_status_location,
min(reshipped_first_datetime) as  reshipped_first_datetime,
min(reshipped_first_status_location) as  reshipped_first_status_location,
min(returned_first_datetime) as  returned_first_datetime,
min(returned_first_status_location) as  returned_first_status_location,
min(returned_to_seller_first_datetime) as  returned_to_seller_first_datetime,
min(returned_to_seller_first_status_location) as  returned_to_seller_first_status_location,
min(reverse_pickup_delivered_first_datetime) as  reverse_pickup_delivered_first_datetime,
min(reverse_pickup_delivered_first_status_location) as  reverse_pickup_delivered_first_status_location,
min(reverse_pickup_scheduled_first_datetime) as  reverse_pickup_scheduled_first_datetime,
min(reverse_pickup_scheduled_first_status_location) as  reverse_pickup_scheduled_first_status_location,
min(rto_delivered_first_datetime) as  rto_delivered_first_datetime,
min(rto_delivered_first_status_location) as  rto_delivered_first_status_location,
min(scheduled_first_datetime) as  scheduled_first_datetime,
min(scheduled_first_status_location) as  scheduled_first_status_location,
min(undelivered_first_datetime) as  undelivered_first_datetime,
min(undelivered_first_status_location) as  undelivered_first_status_location,
min(rto_request_first_datetime) as  rto_request_first_datetime,
min(rto_request_first_status_location) as  rto_request_first_status_location,
min(undelivered_unattempted_first_datetime) as  undelivered_unattempted_first_datetime,
min(undelivered_unattempted_first_status_location) as  undelivered_unattempted_first_status_location,
min(vendor_received_first_datetime) as  vendor_received_first_datetime,
min(vendor_received_first_status_location) as  vendor_received_first_status_location,
min(pickup_reattempt_last_datetime) as  pickup_reattempt_last_datetime,
min(pickup_reattempt_second_datetime) as  pickup_reattempt_second_datetime,
min(pickup_reattempt_third_datetime) as  pickup_reattempt_third_datetime,
min(pickup_reattempt_first_secondary_status) as  pickup_reattempt_first_secondary_status,
min(pickup_reattempt_first_remarks) as  pickup_reattempt_first_remarks,
min(service.source_id) as facility_name,
min(lookupkey('facility_id',source_id)) as facility_id_key,
min(If(ekl_service.facility_id IS NOT NULL, 'shared','independent')) AS vendor_service_type,
max(geo_dest.destination_zone) as destination_zone,
max(delivered_received_datetime) as delivered_received_datetime,
max(pickup_completed_received_datetime) as pickup_completed_received_datetime,
max(case when lower(vendor_tag) like 'delhi%' then 'delhivery'
when lower(vendor_tag) like 'blue%' then 'bluedart'
when lower(vendor_tag) like 'ecom%' then 'ecom'  
when lower(vendor_tag) like 'fed%' and lower(vendor_tag) like '%cod%' then 'fedex_large_cod' 
when lower(vendor_tag) like 'fed%' then 'fedex_large_prepaid' 
when lower(vendor_tag) like 'speed%' then 'speedpost' 
when lower(vendor_tag) like 'afl%' then 'afl' end) as vendor_tag_tmp,
max(case when lower(vendor_tag) like 'delhi%' then 'delhivery'
when lower(vendor_tag) like 'blue%' then 'bluedart'
when lower(vendor_tag) like 'ecom%' then 'ecom'  
when lower(vendor_tag) like 'fed%' then 'fedex large' 
when lower(vendor_tag) like 'speed%' then 'speedpost' 
when lower(vendor_tag) like 'afl%' then 'afl' end) as vendor_tag_tmp_2,
max(case when lower(vendor_tag) like '%speed%' then 'Speed Post'
when lower(vendor_tag) like '%safexpress_surface%' then 'SafeExpress Surface'
when lower(vendor_tag) like '%safexpress%' then 'SafeExpress'  
when lower(vendor_tag) like '%road_%' then 'Road Runnr' 
when lower(vendor_tag) like '%gojavas%' then 'GoJava' 
when lower(vendor_tag) like '%gati%' then 'Gati' 
when lower(vendor_tag) like '%fsd_large%' then 'FSD Large'
when lower(vendor_tag) like '%first%' then 'FirstFlight'
when lower(vendor_tag) like '%fsd_large%' then 'FSD Large'
when lower(vendor_tag) like '%fedex_large%' then 'Fedex Large'
when lower(vendor_tag) like '%fedex%' then 'AFL'
when lower(vendor_tag) like '%ecom%' and lower(vendor_tag) like '%reverse%' then 'Ecom Reverse'
when lower(vendor_tag) like '%ecom%' and lower(vendor_tag) like '%e2e%' then 'Ecom E2E'
when lower(vendor_tag) like '%ecom%' then 'Ecom'
when lower(vendor_tag) like '%delhi%' and lower(vendor_tag) like '%lastmile%' then 'Delhivery Volumetric Last Mile'
when lower(vendor_tag) like '%delhi%' and lower(vendor_tag) like '%e2e%' then 'Delhivery E2E'
when lower(vendor_tag) like '%delhi%' and lower(vendor_tag) like '%reverse%' then 'Delhivery Reverse'
when lower(vendor_tag) like '%delhi%' and lower(vendor_tag) like '%volumetric%' then 'Delhivery Volumetric'
when lower(vendor_tag) like '%delhi%' and lower(vendor_tag) like '%surface%' then 'Delhivery Surface'
when lower(vendor_tag) like '%delhi%' then 'Delhivery'
when lower(vendor_tag) like '%bvc%' then 'BVC'
when lower(vendor_tag) like '%blue%' and lower(vendor_tag) like '%reverse%' then 'Bluedart Reverse'
when lower(vendor_tag) like '%blue%' then 'Bluedart'
when lower(vendor_tag) like '%afl%' then 'AFL'
end) as vendor_tag_tmp_1,

max(ekl_service.facility_id) as ekl_service_facility_id,
max(fulfill_item_unit_new_shipment_movement_type) as fulfill_item_unit_new_shipment_movement_type,
min(if(rvp_pickup_failed_first_secondary_status is null,'Not_defined',if(lower(trim(rvp_pickup_failed_first_secondary_status)) in ('customer no response','customer rejection','reattempt','dispute','customer unavailable','self pickup','request for address change','request to reschedule','consignee shifted','improper packaging','address pincode mismatch','incomplete address','qc failed'),'Customer',if(lower(trim(rvp_pickup_failed_first_secondary_status)) in ('out of pickup area','vendor delay','pickup area not accessible','non serviceable','misrouted'),'Vendor',
if( lower(trim(rvp_pickup_failed_first_secondary_status)) in ('duplicate pickup request','delay beyond control'),'Flipkart','RCA_needed')
)
))) as rvp_pickup_failed_first_attribution,
min(if(rvp_pickup_failed_second_secondary_status is null,'Not_defined',if(lower(trim(rvp_pickup_failed_second_secondary_status)) in ('customer no response','customer rejection','reattempt','dispute','customer unavailable','self pickup','request for address change','request to reschedule','consignee shifted','improper packaging','address pincode mismatch','incomplete address','qc failed'),'Customer',if(lower(trim(rvp_pickup_failed_second_secondary_status)) in ('out of pickup area','vendor delay','pickup area not accessible','non serviceable','misrouted'),'Vendor',
if( lower(trim(rvp_pickup_failed_second_secondary_status)) in ('duplicate pickup request','delay beyond control'),'Flipkart','RCA_needed')
)
))) as rvp_pickup_failed_second_attribution,
min(if(rvp_pickup_failed_last_secondary_status is null,'Not_defined',if(lower(trim(rvp_pickup_failed_last_secondary_status)) in ('customer no response','customer rejection','reattempt','dispute','customer unavailable','self pickup','request for address change','request to reschedule','consignee shifted','improper packaging','address pincode mismatch','incomplete address','qc failed'),'Customer',if(lower(trim(rvp_pickup_failed_last_secondary_status)) in ('out of pickup area','vendor delay','pickup area not accessible','non serviceable','misrouted'),'Vendor',
if( lower(trim(rvp_pickup_failed_last_secondary_status)) in ('duplicate pickup request','delay beyond control'),'Flipkart','RCA_needed')
)
))) as rvp_pickup_failed_last_attribution,

min(if(undelivered_attempted_first_secondary_status is null,'Not_defined',if(lower(trim(undelivered_attempted_first_secondary_status)) in ('customer unavailable','customer no response','cod not ready','customer rejection','reattempt','dispute','self collect','request to reschedule','consignee shifted','address pincode mismatch','incomplete address','qc failed - replacement'),'Customer',if(lower(trim(undelivered_attempted_first_secondary_status)) in ('vendor delay','out of delivery area','delivery area not accessible','delay beyond control','non serviceable','misrouted'),'Vendor','RCA_needed'
)
))) as undelivered_attempted_first_attribution,
min(if(undelivered_attempted_second_secondary_status is null,'Not_defined',if(lower(trim(undelivered_attempted_second_secondary_status)) in ('customer unavailable','customer no response','cod not ready','customer rejection','reattempt','dispute','self collect','request to reschedule','consignee shifted','address pincode mismatch','incomplete address','qc failed - replacement'),'Customer',if(lower(trim(undelivered_attempted_second_secondary_status)) in ('vendor delay','out of delivery area','delivery area not accessible','delay beyond control','non serviceable','misrouted'),'Vendor','RCA_needed'
)
))) as undelivered_attempted_second_attribution,
min(if(undelivered_attempted_last_secondary_status is null,'Not_defined',if(lower(trim(undelivered_attempted_last_secondary_status)) in ('customer unavailable','customer no response','cod not ready','customer rejection','reattempt','dispute','self collect','request to reschedule','consignee shifted','address pincode mismatch','incomplete address','qc failed - replacement'),'Customer',if(lower(trim(undelivered_attempted_last_secondary_status)) in ('vendor delay','out of delivery area','delivery area not accessible','delay beyond control','non serviceable','misrouted'),'Vendor','RCA_needed'
)
))) as undelivered_attempted_last_attribution,

min(if(rvp_undelivered_attempted_first_secondary_status is null,'Not_defined',if(lower(trim(rvp_undelivered_attempted_first_secondary_status)) in ('seller did not handover','seller premises closed','seller shifted','package substandard','seller forms unavailable','label of substandard quality','seller rejection','seller unavailable','seller no response','request to reschedule','address pincode mismatch','incomplete address','request for address change','address related issue','shipment damaged'),'Seller',if(lower(trim(rvp_undelivered_attempted_first_secondary_status)) in ('vendor did not pickup','pickup area not accessible','non serviceable','out of delivery area','vendor delay','misrouted','capacity issues'),'Vendor',
if(lower(trim(rvp_undelivered_attempted_first_secondary_status)) in ('shipment data unavailable','reattempt','dispute','delay beyond control'),'Flipkart',
if(lower(trim(rvp_undelivered_attempted_first_secondary_status)) in ('duplicate pickup request','delivery area not accessible'),'None','RCA_needed'))
)
))) as rvp_undelivered_attempted_first_attribution,
min(if(rvp_undelivered_attempted_second_secondary_status is null,'Not_defined',if(lower(trim(rvp_undelivered_attempted_second_secondary_status)) in ('seller did not handover','seller premises closed','seller shifted','package substandard','seller forms unavailable','label of substandard quality','seller rejection','seller unavailable','seller no response','request to reschedule','address pincode mismatch','incomplete address','request for address change','address related issue','shipment damaged'),'Seller',if(lower(trim(rvp_undelivered_attempted_second_secondary_status)) in ('vendor did not pickup','pickup area not accessible','non serviceable','out of delivery area','vendor delay','misrouted','capacity issues'),'Vendor',
if(lower(trim(rvp_undelivered_attempted_second_secondary_status)) in ('shipment data unavailable','reattempt','dispute','delay beyond control'),'Flipkart',
if(lower(trim(rvp_undelivered_attempted_second_secondary_status)) in ('duplicate pickup request','delivery area not accessible'),'None','RCA_needed'))
)
))) as rvp_undelivered_attempted_second_attribution,
min(if(rvp_undelivered_attempted_last_secondary_status is null,'Not_defined',if(lower(trim(rvp_undelivered_attempted_last_secondary_status)) in ('seller did not handover','seller premises closed','seller shifted','package substandard','seller forms unavailable','label of substandard quality','seller rejection','seller unavailable','seller no response','request to reschedule','address pincode mismatch','incomplete address','request for address change','address related issue','shipment damaged'),'Seller',if(lower(trim(rvp_undelivered_attempted_last_secondary_status)) in ('vendor did not pickup','pickup area not accessible','non serviceable','out of delivery area','vendor delay','misrouted','capacity issues'),'Vendor',
if(lower(trim(rvp_undelivered_attempted_last_secondary_status)) in ('shipment data unavailable','reattempt','dispute','delay beyond control'),'Flipkart',
if(lower(trim(rvp_undelivered_attempted_last_secondary_status)) in ('duplicate pickup request','delivery area not accessible'),'None','RCA_needed'))
)
))) as rvp_undelivered_attempted_last_attribution,

min(if(rto_confirmed_first_secondary_status is null,'Not_defined',if(lower(trim(rto_confirmed_first_secondary_status)) in ('customer unavailable','customer no response','cod not ready','customer rejection','reattempt','dispute','self collect','request to reschedule','consignee shifted','address pincode mismatch','incomplete address','qc failed - replacement'),'Customer',if(lower(trim(rto_confirmed_first_secondary_status)) in ('vendor delay','out of delivery area','delivery area not accessible','delay beyond control','non serviceable','misrouted'),'Vendor','RCA_needed'
)
))) as rto_confirmed_first_attribution,
min(if(rto_confirmed_second_secondary_status is null,'Not_defined',if(lower(trim(rto_confirmed_second_secondary_status)) in ('customer unavailable','customer no response','cod not ready','customer rejection','reattempt','dispute','self collect','request to reschedule','consignee shifted','address pincode mismatch','incomplete address','qc failed - replacement'),'Customer',if(lower(trim(rto_confirmed_second_secondary_status)) in ('vendor delay','out of delivery area','delivery area not accessible','delay beyond control','non serviceable','misrouted'),'Vendor','RCA_needed'
)
))) as rto_confirmed_second_attribution,
min(if(rto_confirmed_last_secondary_status is null,'Not_defined',if(lower(trim(rto_confirmed_last_secondary_status)) in ('customer unavailable','customer no response','cod not ready','customer rejection','reattempt','dispute','self collect','request to reschedule','consignee shifted','address pincode mismatch','incomplete address','qc failed - replacement'),'Customer',if(lower(trim(rto_confirmed_last_secondary_status)) in ('vendor delay','out of delivery area','delivery area not accessible','delay beyond control','non serviceable','misrouted'),'Vendor','RCA_needed'
)
))) as rto_confirmed_last_attribution,

min(if(rto_undelivered_attempted_first_secondary_status is null,'Not_defined',if(lower(trim(rto_undelivered_attempted_first_secondary_status)) in ('seller did not handover','seller premises closed','seller shifted','package substandard','seller forms unavailable','label of substandard quality','seller rejection','seller unavailable','seller no response','request to reschedule','address pincode mismatch','incomplete address','request for address change','address related issue','shipment damaged'),'Seller',if(lower(trim(rto_undelivered_attempted_first_secondary_status)) in ('vendor did not pickup','pickup area not accessible','non serviceable','out of delivery area','vendor delay','misrouted','capacity issues'),'Vendor',
if(lower(trim(rto_undelivered_attempted_first_secondary_status)) in ('shipment data unavailable','reattempt','dispute','delay beyond control'),'Flipkart',
if(lower(trim(rto_undelivered_attempted_first_secondary_status)) in ('duplicate pickup request','delivery area not accessible'),'None','RCA_needed'))
)
))) as rto_undelivered_attempted_first_attribution,
min(if(rto_undelivered_attempted_second_secondary_status is null,'Not_defined',if(lower(trim(rto_undelivered_attempted_second_secondary_status)) in ('seller did not handover','seller premises closed','seller shifted','package substandard','seller forms unavailable','label of substandard quality','seller rejection','seller unavailable','seller no response','request to reschedule','address pincode mismatch','incomplete address','request for address change','address related issue','shipment damaged'),'Seller',if(lower(trim(rto_undelivered_attempted_second_secondary_status)) in ('vendor did not pickup','pickup area not accessible','non serviceable','out of delivery area','vendor delay','misrouted','capacity issues'),'Vendor',
if(lower(trim(rto_undelivered_attempted_second_secondary_status)) in ('shipment data unavailable','reattempt','dispute','delay beyond control'),'Flipkart',
if(lower(trim(rto_undelivered_attempted_second_secondary_status)) in ('duplicate pickup request','delivery area not accessible'),'None','RCA_needed'))
)
))) as rto_undelivered_attempted_second_attribution,
min(if(rto_undelivered_attempted_last_secondary_status is null,'Not_defined',if(lower(trim(rto_undelivered_attempted_last_secondary_status)) in ('seller did not handover','seller premises closed','seller shifted','package substandard','seller forms unavailable','label of substandard quality','seller rejection','seller unavailable','seller no response','request to reschedule','address pincode mismatch','incomplete address','request for address change','address related issue','shipment damaged'),'Seller',if(lower(trim(rto_undelivered_attempted_last_secondary_status)) in ('vendor did not pickup','pickup area not accessible','non serviceable','out of delivery area','vendor delay','misrouted','capacity issues'),'Vendor',
if(lower(trim(rto_undelivered_attempted_last_secondary_status)) in ('shipment data unavailable','reattempt','dispute','delay beyond control'),'Flipkart',
if(lower(trim(rto_undelivered_attempted_last_secondary_status)) in ('duplicate pickup request','delivery area not accessible'),'None','RCA_needed'))
)
))) as rto_undelivered_attempted_last_attribution,

min(rvp_pickup_failed_first_secondary_status) as rvp_pickup_failed_first_secondary_status,
min(rvp_pickup_failed_second_secondary_status) as rvp_pickup_failed_second_secondary_status,
min(rvp_pickup_failed_last_secondary_status) as rvp_pickup_failed_last_secondary_status,
min(undelivered_attempted_second_secondary_status) as undelivered_attempted_second_secondary_status,
min(undelivered_attempted_last_secondary_status) as undelivered_attempted_last_secondary_status,
min(rvp_undelivered_attempted_second_secondary_status) as rvp_undelivered_attempted_second_secondary_status,
min(rvp_undelivered_attempted_last_secondary_status) as rvp_undelivered_attempted_last_secondary_status,
min(rto_undelivered_attempted_first_secondary_status) as rto_undelivered_attempted_first_secondary_status,
min(rto_undelivered_attempted_second_secondary_status) as rto_undelivered_attempted_second_secondary_status,
min(rto_undelivered_attempted_last_secondary_status) as rto_undelivered_attempted_last_secondary_status,
min(rto_confirmed_first_secondary_status) as rto_confirmed_first_secondary_status,
min(rto_confirmed_second_secondary_status) as rto_confirmed_second_secondary_status,
min(rto_confirmed_last_secondary_status) as rto_confirmed_last_secondary_status,
min(pickup_failed_third_secondary_status) as pickup_failed_third_secondary_status,
min(pickup_failed_fourth_secondary_status) as pickup_failed_fourth_secondary_status,
min(pickup_failed_fifth_secondary_status) as pickup_failed_fifth_secondary_status,
min(pickup_failed_sixth_secondary_status) as pickup_failed_sixth_secondary_status,
min(rto_handover_completed_datetime) as rto_handover_completed_datetime,
min(rvp_handover_completed_datetime) as rvp_handover_completed_datetime,
min(rto_handover_initiated_datetime) as rto_handover_initiated_datetime,
min(rvp_handover_initiated_datetime) as rvp_handover_initiated_datetime,
min(unit_hive.fulfill_item_unit_lpe_tier) as fulfill_item_unit_lpe_tier,
min(unit_hive.fulfill_item_unit_reserve_actual_time) as fulfill_item_unit_reserve_actual_time,
min(T3.order_item_approve_date_time) as order_item_approve_date_time,
min(if(dispatched_to_vendor_first_datetime>rto_handover_completed_datetime,1,0)) as no_dispatch_flag,
min(T3.order_item_service_profile) as order_item_service_profile,
min(IF(geo_src.source_city = 'NEW DELHI' and geo_dest.destination_city = 'NEW DELHI',"Local",(IF(geo_src.source_city = 'NEW DELHI' and geo_dest.destination_city = 'NEW DELHI',"Local",
(IF(geo_src.source_city = 'BENGALURU' and geo_dest.destination_city = 'BENGALURU',"Local",(IF(geo_src.source_city = 'BANGALORE' and geo_dest.destination_city = 'BANGALORE',"Local",
(IF(geo_src.source_city = 'KOLKATA' and geo_dest.destination_city = 'KOLKATA',"Local",(IF(geo_src.source_city = 'AHMEDABAD' and geo_dest.destination_city = 'AHMEDABAD',"Local",(IF(geo_src.source_city = 'CHENNAI' and geo_dest.destination_city = 'CHENNAI',"Local",(IF(geo_src.source_city = 'MUMBAI' and geo_dest.destination_city = 'MUMBAI',"Local",(IF(geo_src.source_city = 'HYDERABAD' and geo_dest.destination_city = 'HYDERABAD',"Local",(IF(geo_dest.destination_city IN ('NEW DELHI','BENGALURU','BANGALORE','KOLKATA','AHMEDABAD','CHENNAI','MUMBAI','HYDERABAD'),"Metro",(IF(geo_src.source_city = geo_dest.destination_city, "Local", (IF(geo_src.src_zone = geo_dest.dest_zone, "Zonal", "National")))))))))))))))))))))))) AS geo_zone_flag,

min(out_for_delivery_fourth_datetime) as out_for_delivery_fourth_datetime,
min(out_for_delivery_fifth_datetime) as out_for_delivery_fifth_datetime,
min(out_for_delivery_sixth_datetime) as out_for_delivery_sixth_datetime,
min(out_for_delivery_seventh_datetime) as out_for_delivery_seventh_datetime,
min(undelivered_attempted_second_datetime) as undelivered_attempted_second_datetime,
min(undelivered_attempted_third_datetime) as undelivered_attempted_third_datetime,
min(undelivered_attempted_fourth_datetime) as undelivered_attempted_fourth_datetime,
min(undelivered_attempted_fifth_datetime) as undelivered_attempted_fifth_datetime,
min(undelivered_attempted_sixth_datetime) as undelivered_attempted_sixth_datetime,
min(undelivered_attempted_seventh_datetime) as undelivered_attempted_seventh_datetime,
min(undelivered_attempted_third_secondary_status) as undelivered_attempted_third_secondary_status,
min(undelivered_attempted_fourth_secondary_status) as undelivered_attempted_fourth_secondary_status,
min(undelivered_attempted_fifth_secondary_status) as undelivered_attempted_fifth_secondary_status,
min(undelivered_attempted_sixth_secondary_status) as undelivered_attempted_sixth_secondary_status,
min(undelivered_attempted_seventh_secondary_status) as undelivered_attempted_seventh_secondary_status,
min(rto_confirmed_second_datetime) as rto_confirmed_second_datetime,
min(rto_confirmed_last_datetime) as rto_confirmed_last_datetime,
min(rto_cancelled_first_datetime) as rto_cancelled_first_datetime,
min(rto_cancelled_second_datetime) as rto_cancelled_second_datetime,
min(rto_cancelled_last_datetime) as rto_cancelled_last_datetime,
min(if(update_lpd_first_datetime is null, dispatched_to_vendor_first_datetime,update_lpd_first_datetime)) as dtv,
min(least(out_for_delivery_first_datetime,undelivered_attempted_first_datetime)) as first_attempt
from
 bigfoot_external_neo.scp_fulfillment__fulfillment_liteshipmentstatusevent_base_fact status
left join
 bigfoot_external_neo.scp_fulfillment__fulfillment_liteshipmentservicerequest_base_fact service
 on status.sr_id=service.sr_id
 LEFT JOIN (SELECT `data`.pincode as src_pincode, `data`.zone as src_zone, `data`.local_territory as src_lt,`data`.city as source_city,
`data`.state as source_state,`data`.zone as source_zone from bigfoot_snapshot.dart_fki_scp_ekl_geo_1_1_view_total) geo_src ON source_pincode = geo_src.src_pincode
LEFT JOIN (SELECT `data`.pincode as dest_pincode, `data`.zone as dest_zone, `data`.local_territory as dest_lt,`data`.city as destination_city,
`data`.state as destination_state,`data`.zone as destination_zone from bigfoot_snapshot.dart_fki_scp_ekl_geo_1_1_view_total) geo_dest ON (destination_pincode = geo_dest.dest_pincode)
left Join
(select
order_item_unit_tracking_id,
order_item_unit_rts_breach,
min(order_item_unit_deliver_date_key) as order_item_unit_deliver_date_key,
min(order_item_unit_deliver_time_key) as order_item_unit_deliver_time_key,
min(order_item_unit_ready_to_ship_time_key) as order_item_unit_ready_to_ship_time_key,
min(order_item_unit_ready_to_ship_date_key) as order_item_unit_ready_to_ship_date_key,
min(order_id) as order_id,
min(order_external_id) as order_external_id,
min(order_item_unit_id) as order_item_unit_id,
min(order_item_id) as order_item_id,
min(order_item_unit_shipment_id) as order_item_unit_shipment_id,
min(order_item_approve_date_time) as order_item_approve_date_time,
min(order_item_service_profile) as order_item_service_profile
from bigfoot_external_neo.scp_oms__order_item_unit_s1_fact where (order_item_unit_tracking_id<>0 or order_item_unit_tracking_id<>'' or order_item_unit_tracking_id is not null)
group by order_item_unit_tracking_id, order_item_unit_rts_breach) T3
on 
service.vendor_tracking_id=T3.order_item_unit_tracking_id
left join
bigfoot_external_neo.scp_fulfillment__fulfillment_tpl_shipment_item_intermediate_fact item
on status.sr_id=item.sr_id
left join
bigfoot_external_neo.scp_fulfillment__fulfillment_cartman_weight_fact weight
on vendor_tracking_id=weight.vendortrackingid
left join 
(select shipment_merchant_reference_id,max(fulfill_item_unit_dispatch_service_tier) as fulfill_item_unit_dispatch_service_tier,max(fulfill_item_unit_new_shipment_movement_type) as fulfill_item_unit_new_shipment_movement_type,max(fulfill_item_unit_lpe_tier) as fulfill_item_unit_lpe_tier,max(fulfill_item_unit_reserve_actual_time) as fulfill_item_unit_reserve_actual_time from bigfoot_external_neo.scp_fulfillment__fulfillment_unit_hive_fact group by shipment_merchant_reference_id) unit_hive
on service.shipment_reference_ids=unit_hive.shipment_merchant_reference_id
Left Outer Join
(
SELECT
distinct entityid as facility_id,
pincode,
`data`.type as hub_type
FROM
bigfoot_snapshot.dart_wsr_scp_ekl_facility_0_11_view_total lateral view explode(`data`.facility_routes.pin_codes) exploded_table as pincode where `data`.type in ('BULK_HUB','DELIVERY_HUB') and `data`.active_flag = 1) ekl_service
ON (destination_pincode = ekl_service.pincode AND ekl_service.hub_type <> 'BULK_HUB'
AND If(size = 'bulk','BULK_HUB','DELIVERY_HUB') = ekl_service.hub_type)

 where (item_flag=1 or item_flag is null) and vendor_tag not in ('flipkartlogistics','flipkartlogistics-cod','FSD','FSD_COD') group by status.sr_id) a
 
  left join bigfoot_common.fa_cutoffs_v1 T22 on lower(a.ekl_fin_zone_tmp)=lower(T22.destination_zone) and lower(vendor_tag_tmp)=lower(T22.vendor_name) and lower(a.facility_name)=lower(T22.origin_facility)
 left join bigfoot_common.non_fa_cutoffs_v1 T33 on lower(a.ekl_fin_zone_tmp)=lower(T33.destination_zone) and lower(vendor_tag_tmp)=lower(T33.vendor_name) and lower(a.facility_name)=lower(T33.origin_facility) left join 
(select vendor_name,
case when lower(facility_name) ='ekl-bhiwandi' then 'fkl-bhiwandi'
when lower(facility_name) ='ekl-jaipur' then 'fkl-Jaipur'
when  lower(facility_name) ='ekl-kolkata' then 'fkl-kolkata'
when  lower(facility_name) ='ekl-ghaziabad' then 'fkl-ghaziabad'
when  lower(facility_name) ='ekl-pataudi' then 'fkl-Pataudi'
when  lower(facility_name) ='ekl-jigani1' then 'fkl-Jigani1'
when  lower(facility_name) ='ekl_samalkha' then 'fKL_Samalkha'
when  lower(facility_name) ='ekl-kolkatamp' then 'fkl-KolkataMP'
when  lower(facility_name) ='ekl-chennai' then 'fkl-chennai'
when  lower(facility_name) ='ekl-mumbai' then 'fkl-mumbai'
when  lower(facility_name) ='ekl-pune' then 'fkl-pune' else facility_name end as facility_name_v1,destination_zone,max(in_scan_cut_off) as in_scan_cut_off,max(in_transit_cut_off) as in_transit_cut_off from bigfoot_common.mh_based_shipments_cutoffs group by vendor_name,
case when lower(facility_name) ='ekl-bhiwandi' then 'fkl-bhiwandi'
when lower(facility_name) ='ekl-jaipur' then 'fkl-Jaipur'
when  lower(facility_name) ='ekl-kolkata' then 'fkl-kolkata'
when  lower(facility_name) ='ekl-ghaziabad' then 'fkl-ghaziabad'
when  lower(facility_name) ='ekl-pataudi' then 'fkl-Pataudi'
when  lower(facility_name) ='ekl-jigani1' then 'fkl-Jigani1'
when  lower(facility_name) ='ekl_samalkha' then 'fKL_Samalkha'
when  lower(facility_name) ='ekl-kolkatamp' then 'fkl-KolkataMP'
when  lower(facility_name) ='ekl-chennai' then 'fkl-chennai'
when  lower(facility_name) ='ekl-mumbai' then 'fkl-mumbai'
when  lower(facility_name) ='ekl-pune' then 'fkl-pune' else facility_name end,destination_zone) mh_cut_offs
on lower(a.ekl_fin_zone_tmp)=lower(mh_cut_offs.destination_zone) and lower(vendor_tag_tmp)=lower(mh_cut_offs.vendor_name) and lower(a.facility_name)=lower(mh_cut_offs.facility_name_v1)
left join bigfoot_common.Pincode_Dh_Mapping dh_mapping on dh_mapping.pincode=a.destination_pincode and lower(dh_mapping.vendor)=lower(a.vendor_tag_tmp_2);
