INSERT overwrite TABLE la_e2e_hive_fact 
SELECT
			order_external_id,
			order_id,
			order_item_id,
			order_item_unit_tracking_id,
			order_item_unit_shipment_id,
			tracking_id_type,
			order_item_unit_id,
			order_item_unit_quantity,
			order_item_quantity,
			order_item_selling_price,
			order_item_type,
			order_item_title,
			order_item_unit_status,
			order_item_unit_is_promised_date_updated,
			order_item_unit_new_promised_date,
			order_item_sub_type,
			order_item_sku,
			order_item_category_id,
			order_item_sub_status,
			order_item_ship_group_id,
			order_item_status,
			order_item_product_id_key, 
			order_item_unit_init_promised_date_key,
			order_item_date_key,
			order_item_unit_final_promised_date_key,
			order_item_seller_id_key,
			order_item_time_key,
			order_item_unit_source_facility,
			order_item_listing_id_key,
			order_item_unit_final_promised_date_use,
			order_item_unit_init_promised_datetime,
			order_item_last_update,
			order_item_date,
			order_item_created_at,
			order_shipping_address_id_key,
			call_verification_reason,
			order_item_max_approved_time,
			order_item_max_on_hold_time,
			order_last_payment_method,
			call_verification_type,
			is_replacement,
			is_exchange,
			is_duplicate,
			account_id,
			account_id_key,
			fulfill_item_unit_dispatch_actual_time,
			fulfill_item_unit_ship_actual_time,
			fulfill_item_unit_dispatch_expected_time,
			fulfill_item_unit_ship_expected_time,
			fulfillment_order_item_id,
			fulfill_item_unit_id,
			fulfill_item_unit_status,
			shipment_merchant_reference_id,
			fulfill_item_unit_updated_at,
			fulfill_item_unit_region,
			fulfill_item_unit_dispatch_actual_date_key,
			fulfill_item_unit_ship_actual_date_key,
			fulfill_item_unit_dispatch_expected_date_key,
			fulfill_item_unit_order_date_key,
			fulfill_item_unit_dispatch_actual_time_key,
			fulfill_item_unit_ship_actual_time_key,
			fulfill_item_unit_dispatch_expected_time_key,
			fulfill_item_unit_order_time_key,
			fulfill_item_unit_region_type,
			fulfill_item_id,
			fulfill_item_type,
			fulfill_item_unit_deliver_after_time,
			fulfill_item_unit_dispatch_after_time,
			fulfill_item_unit_deliver_after_date_key,
			fulfill_item_unit_deliver_after_time_key,
			fulfill_item_unit_dispatch_after_date_key,
			fulfill_item_unit_dispatch_after_time_key,
			fulfill_item_unit_is_for_slotted_delivery,
			fulfill_item_unit_reserved_status_b2c_actual_time,
			fulfill_item_unit_reserved_status_expected_time,
			fulfill_item_unit_reserved_status_b2c_actual_date_key,
			fulfill_item_unit_reserved_status_b2c_actual_time_key,
			fulfill_item_unit_reserved_status_expected_date_key,
			fulfill_item_unit_reserved_status_expected_time_key,
			fulfill_item_unit_delivered_status_expected_time,
			fulfill_item_unit_delivered_status_expected_date_key,
			fulfill_item_unit_delivered_status_expected_time_key,
			fulfill_item_unit_delivered_status_id,
			fulfill_item_unit_size,
			fulfill_item_unit_reserved_status_b2b_actual_date_key,
			fulfill_item_unit_reserved_status_b2b_actual_time_key,
			fulfill_item_unit_reserved_status_b2b_actual_time,
			slot_changed_created_at,
			slot_changed_by,
			product_categorization_hive_dim_key,
			product_id,
			analytic_super_category,
			brand,
			vertical,
			analytic_category,
			analytic_sub_category,
			shipment_id,
			vendor_tracking_id,
			shipment_current_status,
			shipment_current_status_date_key,
			shipment_current_status_time_key,
			destination_geo_id_key,
			ekl_shipment_type,
			customer_promise_date_key,
			customer_promise_time_key,
			logistics_promise_date_key,
			logistics_promise_time_key,
			shipment_created_at_date_key,
			shipment_created_at_time_key,
			vendor_id_key,
			shipment_carrier,
			fsd_assigned_hub_id_key,
			fsd_last_current_hub_id_key,
			fsd_last_current_hub_type,
			shipment_inscan_time,
			shipment_rto_create_time,
			fsd_last_received_time,
			fsd_number_of_ofd_attempts,
			fsd_assignedhub_expected_time,
			fsd_assignedhub_received_time,
			fsd_returnedtoekl_time,
			fsd_receivedbyekl_time,
			merchant_reference_id,
			ekl_delivery_datetime,
			ekl_delivery_date_key,
			ekl_delivery_time_key,
			shipment_first_ofd_datetime,
			shipment_last_ofd_datetime,
			vendor_dispatch_datetime,
			ekl_billable_weight,
			fsd_assigned_hub_sent_datetime,
			shipment_first_consignment_id,
			shipment_last_consignment_id,
			openbox_reject_datetime,
			merchant_id,
			fsd_first_dh_received_datetime,
			shipment_first_consignment_create_date_key,
			shipment_first_consignment_create_time_key,
			shipment_origin_facility_id_key,
			fsd_firstundeliverystatus,
			rvp_pickup_complete_datetime,
			shipment_rvp_pk_number_of_attempts,
			end_state_datetime,
			fsd_last_dhhub_sent_datetime,
			rvp_hub_id,
			rvp_hub_id_key,
			shipment_value,
			cod_amount_to_collect,
			rvp_origin_geo_id_key,
			rvp_destination_facility_key,
			exchange_fsn,
			replacement_order_item_id,
			shipment_dh_fwd_promised_date,
			shipment_dh_rvp_promised_date,
			shipment_dh_rto_promised_date,
			shipment_dh_pending_flag,
			ekl_pending_flag,
			present_day,
			order_final_unit_type,
			fsd_first_undel_customer_dependency_flag,
			fsd_current_customer_dependency_flag,
			logistics_promise_datetime,
			shipment_created_at_datetime,
			shipment_first_consignment_create_datetime,
			fc_pending_flag,
			fc_pending_age,
			fc_breach_flag,
			fc_breach_bucket,
			perfect_order_final_promised_date,
			shipment_dh_fwd_breach_flag,
			shipment_dh_pending_age,
			first_attempt_delivery_flag,
			shipment_dh_days_for_delivery,
			shipment_pending_location,
			first_attempt_pickup_flag,
			shipment_dh_fwd_promised_date_key,
			shipment_dh_rvp_promised_date_key,
			shipment_dh_rto_promised_date_key,
			rvp_pickup_complete_date_key,
			end_state_date_key,
			fsd_last_dhhub_sent_date_key,
			order_item_unit_final_promised_date_use_date_key,
			order_item_created_at_date_key,
			fulfill_item_unit_ship_expected_date_key,
			shipment_inscan_date_key,
			shipment_rto_create_date_key,
			fsd_assignedhub_received_date_key,
			shipment_first_ofd_date_key,
			shipment_last_ofd_date_key,
			vendor_dispatch_date_key,
			fsd_assigned_hub_sent_date_key,
			openbox_reject_date_key,
			present_day_key,
			openbox_reject_flag,
			order_pending_flag,
			order_final_status,
			order_pending_location,
			order_pending_age,
			shipment_breach_bucket,
			shipment_breach_flag,
			shipment_pending_age,
			rvp_pickup_breach_flag,
			customer_breach_breakup,
			rvp_rto_reconciliation_days,
			shipment_dh_rvp_breach_flag,
			shipment_dh_rto_breach_flag,
			forward_dh_reconciliation_bucket,
			reverse_dh_reconciliation_bucket,
			rvp_cancelled_flag,
			existing_sla_days,
			performance_sla_days,
			pending_beyond_customer_promise_age,
			customer_promise_proactive_age,
			reverse_breach_flag,
			rev_promise_date_key,
			perfect_order_breach_bucket,
			return_type,
			return_reason,
			return_sub_reason,
			return_comments,
			qc_reason,
			runsheet_close_datetime,
			runsheet_close_date_key,
			return_derived_reason,
			mh_breach_flag,
			cancellation_reason,
			cancellation_sub_reason,
			cancellation_derived_reason,
			customer_contact_flag,
			customer_contact_issue_type,
			fk_triggered_customer_breach_flag,
			fk_triggered_promise_change_flag,
			fk_triggered_cancellation_flag,
			fk_triggered_return_flag,
			IF(
				fk_triggered_customer_breach_flag = 1
				OR fk_triggered_promise_change_flag = 1
				OR customer_contact_flag = 1
				OR fk_triggered_cancellation_flag = 1
				OR fk_triggered_return_flag = 1,
				0,
				1
			) AS perfect_delivery_flag,
			slot_adherence_flag,
			slot_change_reason,
			asset,
			(
				CASE
					WHEN asset IN(
						'12_Returned',
						'11_Delivered',
						'13_Cancelled'
					) THEN 0
					WHEN asset IN('01_OMS')
					AND unix_timestamp() > unix_timestamp(order_item_date) + 86400 THEN 1
					WHEN asset IN('02_Fulfillment')
					AND unix_timestamp() > unix_timestamp(order_item_max_approved_time) + 14400 THEN 1
					WHEN asset IN('03_Warehouse')
					AND unix_timestamp() > unix_timestamp(fulfill_item_unit_dispatch_expected_time) THEN 1
					WHEN asset IN('10_3PL')
					AND unix_timestamp() > unix_timestamp(order_item_unit_final_promised_date_use) THEN 1
					WHEN asset IN('04_MotherHub')
					AND unix_timestamp() > unix_timestamp(mh_cutoff_datetime) THEN 1
					WHEN asset IN('05_Transport')
					AND unix_timestamp() > unix_timestamp(shipment_last_consignment_create_datetime) + tpt_time_diff_in_sec + shipment_last_consignment_eta_in_sec THEN 1
					WHEN asset IN('06_DeliveryHub')
					AND unix_timestamp() > unix_timestamp(shipment_dh_fwd_promised_date) THEN 1
					ELSE 0
				END
			) AS asset_breach_flag,
			shipment_last_consignment_create_datetime,
			lookup_date(
				to_Date(perfect_order_final_promised_date)
			) AS perfect_order_final_promised_date_key,
			IF(
				calculated_delivery_hub LIKE '%3PL%'
				OR shipment_carrier = '3PL',
				'3PL',
				'FSD'
			) AS calculated_carrier,
			calculated_delivery_hub,
			order_pincode_key,
			slot_booking_ref_id,
			cu_sub_issue_type,
			incident_creation_time,
			0 as incident_creation_date_key,
			DATEDIFF(
				to_date(order_item_unit_init_promised_datetime),
				to_date(order_item_max_approved_time)
			) AS ndd_flag,
			fulfill_item_unit_final_reserved_datetime,
			fulfill_item_unit_final_reserved_date_key,
			existing_d2d_days,
			performance_d2d_days,
			tasklist_tracking_id,
			ekl_facility_id,
			last_tasklist_agent_id,
			last_tasklist_updated_at,
			tasklist_type,
			last_tasklist_id,
			tasklist_status,
			runsheet_id,
			last_tasklist_agent_id_key,
			ekl_facility_id_key,
			customer_contact_sub_sub_issue_type,
			seller_id,
			payment_mode,
			pos_id,
			transaction_id,
			agent_id,
			amount_collected,
			seller_type,
			shipment_dg_flag,
			shipment_fragile_flag,
			seller_id_key,
			pos_id_key,
			shipment_agent_id_key,
			pincode_location_type AS pincode_location_type,
			is_cpu_vendor AS is_cpu_vendor,
			'dummy3' AS dummy3,
			mh_promise_datetime,
			shipment_tpt_dh_eta_datetime,
			lzn_classification,
			lzn_tat_target,
			pd_new_customer_breach,
			pd_new_fk_triggered_rto,
			pe_shipment_condition_breach,
			pe_sla_breach,
			pe_delivery_attempt_breach,
			pe_delivery_precision_breach,
			IF(
			
				pd_new_customer_breach = 1
				OR fk_triggered_promise_change_flag = 1
				OR pd_fk_triggered_rto_after_lpd = 1,
				0,
				1
			) AS pd_new_flag,
			IF(
				customer_contact_flag = 1
				OR pe_shipment_condition_breach = 1
				OR pe_sla_breach = 1
				OR pe_delivery_attempt_breach = 1
				OR pe_delivery_precision_breach = 1,
				0,
				1
			) AS pe_flag,
			return_date,
			vendor_name,
			order_create_to_delivery_sla_days,
			picklist_display_id,
			picklist_created_at,
			picklist_item_confirmed_at,
			pd_fk_triggered_rto_after_lpd,
			CASE 
			WHEN fk_triggered_promise_change_flag = 1 THEN 'Promise Changed By EKL'
			WHEN fk_triggered_promise_change_flag = 0 AND pd_fk_triggered_rto_after_lpd = 1 THEN 'EKL Triggered RTO'
			WHEN fk_triggered_promise_change_flag = 0 AND pd_fk_triggered_rto_after_lpd = 0 AND pd_new_customer_breach = 1 THEN shipment_breach_bucket END
			AS pd_imperfection_bucket,
			--rvp_promise_date_key,
			rvp_complete_date_key,
			IF(IF(rvp_complete_date_key is null or rvp_complete_date_key = 0, current_day_key,rvp_complete_date_key) > rev_promise_date_key,1,0)  as rvp_breach_flag,
			rvp_tat,
			source_pincode,			
			destination_pincode,
			product_length,
			product_breadth,
			product_height,
			product_shipping_weight,
			is_synergy_hub,
			rvp_complete_datetime,
			reverse_shipment_type,
			shipment_tpt_dh_eta_date_key,
			tpt_compliance_flag,
			actual_cons_created_datetime,
			conn_eta_in_sec
FROM
			(
				SELECT
					order_external_id,
					order_id,
					IF(
						isnull(shipment_last_consignment_create_datetime),
						0,
						IF(
							(
								last_conn_cutoff_in_sec - HOUR(shipment_last_consignment_create_datetime) * 3600 - MINUTE(shipment_last_consignment_create_datetime) * 60 - SECOND(shipment_last_consignment_create_datetime)
							) > 0,
							(
								last_conn_cutoff_in_sec - HOUR(shipment_last_consignment_create_datetime) * 3600 - MINUTE(shipment_last_consignment_create_datetime) * 60 - SECOND(shipment_last_consignment_create_datetime)
							),
							0
						)
					) AS tpt_time_diff_in_sec,
					(
						CASE
							WHEN order_item_status IN(
								'returned',
								'return_requested'
							)
							OR ekl_shipment_type IN(
								'approved_rto',
								'unapproved_rto'
							) THEN '12_Returned'
							WHEN order_item_status IN('delivered')
							OR shipment_current_status IN(
								'delivered',
								'Delivered'
							)
							OR ekl_delivery_datetime IS NOT NULL THEN '11_Delivered'
							WHEN order_item_status IN('cancelled')
							OR shipment_current_status IN('cancelled') THEN '13_Cancelled'
							WHEN order_item_status IN(
								'created',
								'on_hold'
							) THEN '01_OMS'
							WHEN isnull(fulfill_item_unit_reserved_status_b2c_actual_time)
							AND isnull(fulfill_item_unit_reserved_status_b2b_actual_time)
							AND isnull(shipment_created_at_datetime) THEN '02_Fulfillment'
							WHEN isnull(fulfill_item_unit_dispatch_actual_time)
							AND isnull(shipment_created_at_datetime) THEN '03_Warehouse'
							WHEN shipment_carrier = '3PL' THEN '10_3PL'
							WHEN isnull(shipment_first_consignment_create_datetime) THEN '04_MotherHub'
							WHEN isnull(fsd_assignedhub_received_time) THEN '05_Transport'
							WHEN isnull(shipment_first_ofd_datetime) THEN '06_DeliveryHub'
							ELSE '00_Catch_in_system_error'
						END
					) AS asset,
					from_unixtime(
						unix_timestamp(
							IF(
								fulfill_item_unit_dispatch_expected_time > fulfill_item_unit_dispatch_actual_time
								OR isnull(fulfill_item_unit_dispatch_actual_time),
								fulfill_item_unit_dispatch_expected_time,
								fulfill_item_unit_dispatch_actual_time
							)
						) + 7200
					) AS mh_cutoff_datetime,
					DA.order_item_id,
					order_item_unit_tracking_id,
					order_item_unit_shipment_id,
					tracking_id_type,
					order_item_unit_id,
					order_pincode_key,
					cu_sub_issue_type,
					incident_creation_time,
					calculated_delivery_hub,
					order_item_unit_quantity,
					order_item_quantity,
					order_item_selling_price,
					order_item_type,
					order_item_title,
					order_item_unit_status,
					order_item_unit_is_promised_date_updated,
					order_item_unit_new_promised_date,
					order_item_sub_type,
					order_item_sku,
					order_item_category_id,
					order_item_sub_status,
					order_item_ship_group_id,
					order_item_status,
					order_item_product_id_key,
					order_item_unit_init_promised_date_key,
					order_item_date_key,
					order_item_unit_final_promised_date_key,
					order_item_seller_id_key,
					order_item_time_key,
					order_item_unit_source_facility,
					order_item_listing_id_key,
					order_item_unit_final_promised_date_use,
					order_item_unit_init_promised_datetime,
					order_item_last_update,
					order_item_date,
					order_item_created_at,
					order_shipping_address_id_key,
					call_verification_reason,
					order_item_max_approved_time,
					order_item_max_on_hold_time,
					order_last_payment_method,
					call_verification_type,
					is_replacement,
					is_exchange,
					is_duplicate,
					account_id,
					account_id_key,
					fulfill_item_unit_dispatch_actual_time,
					fulfill_item_unit_ship_actual_time,
					fulfill_item_unit_dispatch_expected_time,
					fulfill_item_unit_ship_expected_time,
					fulfillment_order_item_id,
					slot_booking_ref_id,
					fulfill_item_unit_id,
					fulfill_item_unit_status,
					shipment_merchant_reference_id,
					fulfill_item_unit_updated_at,
					fulfill_item_unit_region,
					fulfill_item_unit_dispatch_actual_date_key,
					fulfill_item_unit_ship_actual_date_key,
					fulfill_item_unit_dispatch_expected_date_key,
					fulfill_item_unit_order_date_key,
					fulfill_item_unit_dispatch_actual_time_key,
					fulfill_item_unit_ship_actual_time_key,
					fulfill_item_unit_dispatch_expected_time_key,
					fulfill_item_unit_order_time_key,
					fulfill_item_unit_region_type,
					fulfill_item_id,
					fulfill_item_type,
					fulfill_item_unit_deliver_after_time,
					fulfill_item_unit_dispatch_after_time,
					fulfill_item_unit_deliver_after_date_key,
					fulfill_item_unit_deliver_after_time_key,
					fulfill_item_unit_dispatch_after_date_key,
					fulfill_item_unit_dispatch_after_time_key,
					fulfill_item_unit_is_for_slotted_delivery,
					fulfill_item_unit_reserved_status_b2c_actual_time,
					fulfill_item_unit_reserved_status_expected_time,
					fulfill_item_unit_reserved_status_b2c_actual_date_key,
					fulfill_item_unit_reserved_status_b2c_actual_time_key,
					fulfill_item_unit_reserved_status_expected_date_key,
					fulfill_item_unit_reserved_status_expected_time_key,
					fulfill_item_unit_delivered_status_expected_time,
					fulfill_item_unit_delivered_status_expected_date_key,
					fulfill_item_unit_delivered_status_expected_time_key,
					fulfill_item_unit_delivered_status_id,
					fulfill_item_unit_size,
					fulfill_item_unit_reserved_status_b2b_actual_date_key,
					fulfill_item_unit_reserved_status_b2b_actual_time_key,
					fulfill_item_unit_reserved_status_b2b_actual_time,
					slot_changed_created_at,
					slot_changed_by,
					product_categorization_hive_dim_key,
					product_id,
					analytic_super_category,
					brand,
					analytic_vertical AS vertical,
					analytic_category,
					analytic_sub_category,
					shipment_id,
					vendor_tracking_id,
					shipment_current_status,
					shipment_current_status_date_key,
					shipment_current_status_time_key,
					destination_geo_id_key,
					ekl_shipment_type,
					customer_promise_date_key,
					customer_promise_time_key,
					logistics_promise_date_key,
					logistics_promise_time_key,
					shipment_created_at_date_key,
					shipment_created_at_time_key,
					vendor_id_key,
					shipment_carrier,
					fsd_assigned_hub_id_key,
					fsd_last_current_hub_id_key,
					fsd_last_current_hub_type,
					shipment_inscan_time,
					shipment_rto_create_time,
					fsd_last_received_time,
					fsd_number_of_ofd_attempts,
					fsd_assignedhub_expected_time,
					fsd_assignedhub_received_time,
					fsd_returnedtoekl_time,
					fsd_receivedbyekl_time,
					merchant_reference_id,
					ekl_delivery_datetime,
					ekl_delivery_date_key,
					ekl_delivery_time_key,
					shipment_first_ofd_datetime,
					shipment_last_ofd_datetime,
					vendor_dispatch_datetime,
					ekl_billable_weight,
					fsd_assigned_hub_sent_datetime,
					shipment_first_consignment_id,
					shipment_last_consignment_id,
					openbox_reject_datetime,
					merchant_id,
					fsd_first_dh_received_datetime,
					shipment_first_consignment_create_date_key,
					shipment_first_consignment_create_time_key,
					shipment_origin_facility_id_key,
					fsd_firstundeliverystatus,
					rvp_pickup_complete_datetime,
					shipment_rvp_pk_number_of_attempts,
					end_state_datetime,
					fsd_last_dhhub_sent_datetime,
					rvp_hub_id,
					rvp_hub_id_key,
					shipment_value,
					cod_amount_to_collect,
					ekl_facility_id,
					last_tasklist_agent_id,
					last_tasklist_agent_id_key,
					last_tasklist_updated_at,
					tasklist_type,
					last_tasklist_id,
					tasklist_status,
					runsheet_id,
					tasklist_tracking_id,
					ekl_facility_id_key,
					rvp_origin_geo_id_key,
					rvp_destination_facility_key,
					exchange_fsn,
					replacement_order_item_id,
					shipment_dh_fwd_promised_date,
					shipment_dh_rvp_promised_date,
					shipment_dh_rto_promised_date,
					shipment_dh_pending_flag,
					ekl_pending_flag,
					present_day,
					order_final_unit_type,
					fsd_first_undel_customer_dependency_flag,
					fsd_current_customer_dependency_flag,
					logistics_promise_datetime,
					shipment_created_at_datetime,
					shipment_first_consignment_create_datetime,
					fc_pending_flag,
					fc_pending_age,
					fc_breach_flag,
					fc_breach_bucket,
					perfect_order_final_promised_date,
					shipment_dh_fwd_breach_flag,
					shipment_dh_pending_age,
					first_attempt_delivery_flag,
					shipment_dh_days_for_delivery,
					shipment_pending_location,
					first_attempt_pickup_flag,
					shipment_dh_fwd_promised_date_key,
					shipment_dh_rvp_promised_date_key,
					shipment_dh_rto_promised_date_key,
					rvp_pickup_complete_date_key,
					end_state_date_key,
					fsd_last_dhhub_sent_date_key,
					order_item_unit_final_promised_date_use_date_key,
					order_item_created_at_date_key,
					fulfill_item_unit_ship_expected_date_key,
					shipment_inscan_date_key,
					shipment_rto_create_date_key,
					fsd_assignedhub_received_date_key,
					shipment_first_ofd_date_key,
					shipment_last_ofd_date_key,
					vendor_dispatch_date_key,
					fsd_assigned_hub_sent_date_key,
					openbox_reject_date_key,
					present_day_key,
					openbox_reject_flag,
					order_pending_flag,
					order_final_status,
					order_pending_location,
					order_pending_age,
					shipment_breach_bucket,
					shipment_breach_flag,
					shipment_pending_age,
					rvp_pickup_breach_flag,
					IF(
						to_date(ekl_delivery_datetime) <= to_date(order_item_unit_final_promised_date_use),
						'Delivered by promise',
						IF(
							to_date(shipment_first_ofd_datetime) <= to_date(order_item_unit_final_promised_date_use),
							'No Breach-OFD Before LPD-Customer Dependency Attempted',
							IF(
								fulfill_item_unit_dispatch_actual_time > fulfill_item_unit_dispatch_expected_time,
								fc_breach_bucket,
								IF(
									to_date(logistics_promise_datetime) > to_date(order_item_unit_final_promised_date_use),
									'LP greater than CP',
									IF(
										NOT isnull(fulfill_item_unit_dispatch_actual_time),
										shipment_breach_bucket,
										IF(
											order_item_status = 'on_hold',
											'on_hold',
											'rca'
										)
									)
								)
							)
						)
					) AS customer_breach_breakup,
					IF(
						ekl_shipment_type IN(
							'approved_rto',
							'unapproved_rto'
						)
						AND NOT isnull(end_state_datetime)
						AND NOT isnull(shipment_rto_create_time),
						datediff(
							to_date(end_state_datetime),
							to_date(shipment_rto_create_time)
						),
						IF(
							ekl_shipment_type = 'rvp'
							AND NOT isnull(end_state_datetime)
							AND NOT isnull(shipment_created_at_datetime),
							datediff(
								to_date(end_state_datetime),
								to_date(shipment_created_at_datetime)
							),
							NULL
						)
					) AS rvp_rto_reconciliation_days,
					IF(
						ekl_shipment_type = 'rvp',
						IF(
							NOT isnull(fsd_last_dhhub_sent_datetime),
							IF(
								to_date(fsd_last_dhhub_sent_datetime) <= shipment_dh_rvp_promised_date,
								0,
								1
							),
							IF(
								datediff(
									present_day,
									shipment_dh_rvp_promised_date
								) > 0,
								1,
								0
							)
						),
						NULL
					) AS shipment_dh_rvp_breach_flag,
					IF(
						ekl_shipment_type IN(
							'approved_rto',
							'unapproved_rto'
						)
						AND NOT isnull(fsd_assignedhub_received_time),
						IF(
							NOT isnull(fsd_last_dhhub_sent_datetime),
							IF(
								to_date(fsd_last_dhhub_sent_datetime) <= shipment_dh_rto_promised_date,
								0,
								1
							),
							IF(
								datediff(
									present_day,
									shipment_dh_rto_promised_date
								) > 0,
								1,
								0
							)
						),
						NULL
					) AS shipment_dh_rto_breach_flag,
					IF(
						ekl_shipment_type IN(
							'forward',
							'unapproved_rto',
							'approved_rto'
						)
						AND NOT isnull(fsd_assignedhub_received_time),
						IF(
							ekl_shipment_type = 'forward',
							IF(
								isnull(ekl_delivery_datetime),
								'Forward_Intransit',
								'Forward_Delivered'
							),
							IF(
								isnull(fsd_last_dhhub_sent_datetime),
								'RTO_In_Hub',
								'RTO_Intransit_To_DC'
							)
						),
						NULL
					) AS forward_dh_reconciliation_bucket,
					IF(
						ekl_shipment_type = 'rvp'
						AND shipment_carrier = 'FSD',
						IF(
							shipment_current_status IN(
								'not_received',
								'Not_Received'
							),
							'RVP_cancelled',
							IF(
								isnull(rvp_pickup_complete_datetime),
								'RVP_Pickup_Pending',
								IF(
									isnull(fsd_last_dhhub_sent_datetime),
									'RVP_In_Hub',
									'Intransit_To_DC'
								)
							)
						),
						NULL
					) AS reverse_dh_reconciliation_bucket,
					IF(
						ekl_shipment_type = 'rvp'
						AND shipment_current_status IN(
							'not_received',
							'Not_Received'
						),
						1,
						0
					) AS rvp_cancelled_flag,
					datediff(
						to_date(order_item_unit_init_promised_datetime),
						to_date(order_item_created_at)
					) AS existing_sla_days,
					IF(
						ekl_shipment_type = 'forward'
						AND NOT isnull(ekl_delivery_datetime),
						datediff(
							to_date(ekl_delivery_datetime),
							to_date(order_item_created_at)
						),
						NULL
					) AS performance_sla_days,
					datediff(
						to_date(order_item_unit_final_promised_date_use),
						to_date(fulfill_item_unit_dispatch_expected_time)
					) AS existing_d2d_days,
					IF(
						ekl_shipment_type = 'forward'
						AND NOT isnull(ekl_delivery_datetime),
						IF(
							shipment_carrier = '3PL',
							datediff(
								to_date(ekl_delivery_datetime),
								to_date(vendor_dispatch_datetime)
							),
							datediff(
								to_date(ekl_delivery_datetime),
								to_date(fulfill_item_unit_dispatch_expected_time)
							)
						),
						NULL
					) AS performance_d2d_days,
					IF(
						ekl_shipment_type = 'forward'
						AND isnull(ekl_delivery_datetime),
						datediff(
							present_day,
							to_Date(order_item_unit_final_promised_date_use)
						),
						NULL
					) AS pending_beyond_customer_promise_age,
					datediff(
						to_date(order_item_unit_final_promised_date_use),
						present_day
					) AS customer_promise_proactive_age,
					IF(
						ekl_shipment_type = 'rvp',
						IF(
							pincode_location_type IN(
								'ESS',
								'Fran'
							),
							IF(
								NOT isnull(end_state_datetime),
								IF(
									to_date(end_state_datetime) <= date_add(
										to_date(shipment_created_at_datetime),
										14
									),
									0,
									1
								),
								IF(
									datediff(
										present_day,
										date_add(
											to_date(shipment_created_at_datetime),
											14
										)
									) > 0,
									1,
									0
								)
							),
							IF(
								NOT isnull(end_state_datetime),
								IF(
									to_date(end_state_datetime) <= date_add(
										to_date(shipment_created_at_datetime),
										10
									),
									0,
									1
								),
								IF(
									datediff(
										present_day,
										date_add(
											to_date(shipment_created_at_datetime),
											10
										)
									) > 0,
									1,
									0
								)
							)
						),
						IF(
							ekl_shipment_type IN(
								'approved_rto',
								'unapproved_rto'
							),
							IF(
								pincode_location_type IN(
									'ESS',
									'Fran'
								),
								IF(
									NOT isnull(end_state_datetime),
									IF(
										to_date(end_state_datetime) <= date_add(
											to_date(shipment_rto_create_time),
											10
										),
										0,
										1
									),
									IF(
										datediff(
											present_day,
											date_add(
												to_date(shipment_rto_create_time),
												10
											)
										) > 0,
										1,
										0
									)
								),
								IF(
									NOT isnull(end_state_datetime),
									IF(
										to_date(end_state_datetime) <= date_add(
											to_date(shipment_rto_create_time),
											7
										),
										0,
										1
									),
									IF(
										datediff(
											present_day,
											date_add(
												to_date(shipment_rto_create_time),
												7
											)
										) > 0,
										1,
										0
									)
								)
							),
							NULL
						)
					) AS reverse_breach_flag,
					IF(
						ekl_shipment_type = 'rvp',
						if(shipment_carrier = '3PL',lookup_date(date_add(to_date(shipment_created_at_datetime),rvp_tat)),
						lookup_date(
							date_add(
								to_date(shipment_created_at_datetime),
								10
							)
						)),
						IF(
							ekl_shipment_type IN(
								'approved_rto',
								'unapproved_rto'
							),
							lookup_date(
								date_add(
									to_date(shipment_rto_create_time),
									7
								)
							),
							NULL
						)
					) AS rev_promise_date_key,
					perfect_order_breach_bucket,
					return_type,
					return_date,
					return_reason,
					return_sub_reason,
					return_comments,
					qc_reason,
					runsheet_close_datetime,
					runsheet_close_date_key,
					mh_breach_flag,
					mh_promise_datetime,
					shipment_tpt_dh_eta_datetime,
					shipment_tpt_dh_eta_date_key,
					tpt_compliance_flag,
					actual_cons_created_datetime,
					conn_eta_in_sec,
					return_derived_reason,
					cancellation_reason,
					cancellation_sub_reason,
					cancellation_derived_reason,
					customer_contact_flag,
					customer_contact_issue_type,
					customer_contact_sub_sub_issue_type,
					IF(
						ekl_shipment_type = 'forward',
						IF(
							ekl_delivery_datetime IS NOT NULL,
							IF(
								to_date(ekl_delivery_datetime) <= to_date(perfect_order_final_promised_date),
								0,
								IF(
									to_date(shipment_first_ofd_datetime) <= to_date(perfect_order_final_promised_date),
									0,
									IF(
										shipment_breach_bucket IN(
											'No Breach-OFD Before LPD-Customer Dependency Attempted',
											'Customer_Dependency-Attempted Undelivered-3PL'
										),
										0,
										1
									)
								)
							),
							IF(
								to_date(shipment_first_ofd_datetime) <= to_date(perfect_order_final_promised_date),
								0,
								IF(
									to_date(perfect_order_final_promised_date) < present_day,
									1,
									0
								)
							)
						),
						0
					) AS fk_triggered_customer_breach_flag,
					CASE WHEN shipment_breach_bucket != 'No Breach-OFD Before LPD-Customer Dependency Attempted' THEN fk_triggered_promise_change_flag ELSE 0 END AS fk_triggered_promise_change_flag,
					fk_triggered_cancellation_flag,
					fk_triggered_return_flag,
					IF(
						(
							ekl_delivery_datetime <= fulfill_item_unit_delivered_status_expected_time
							AND ekl_delivery_datetime >= fulfill_item_unit_deliver_after_time
						),
						1,
						0
					) AS slot_adherence_flag,
					slot_change_reason,
					last_conn_cutoff_in_sec,
					shipment_last_consignment_create_datetime,
					shipment_last_consignment_eta_in_sec,
					fulfill_item_unit_final_reserved_datetime,
					fulfill_item_unit_final_reserved_date_key,
					seller_id,
					payment_mode,
					pos_id,
					transaction_id,
					agent_id,
					amount_collected,
					seller_type,
					shipment_dg_flag,
					shipment_fragile_flag,
					seller_id_key,
					pos_id_key,
					shipment_agent_id_key,
					pincode_location_type,
					is_cpu_vendor,
					lzn_classification,
					lzn_tat_target,
					CASE
					WHEN ekl_shipment_type = 'forward'
					AND shipment_breach_bucket NOT IN(
						'Future Pending',
						'delivered by logistics promise date',
						'No Breach-OFD Before LPD-Customer Dependency Attempted',
						'Promise Change by Customer',
						'Customer_Dependency-Attempted Undelivered-3PL'
					) THEN 1
					WHEN shipment_rto_create_date_key > logistics_promise_date_key
					AND shipment_breach_bucket NOT IN(
						'Future Pending',
						'delivered by logistics promise date',
						'No Breach-OFD Before LPD-Customer Dependency Attempted',
						'Promise Change by Customer',
						'Customer_Dependency-Attempted Undelivered-3PL'
					) THEN 1
					ELSE 0
					END AS pd_new_customer_breach,
						CASE
						WHEN ekl_shipment_type IN(
							'approved_rto',
							'unapproved_rto'
						)
						AND return_reason IN(
							'Damaged Shipment',
							'DAMAGED_MISSING_PRODUCT',
							'DAMAGED_PRODUCT',
							'delivery_delayed',
							'lost_in_transit',
							'mis_route',
							'mis_shipment',
							'MISSHIPMENT',
							'Mis-shipped Item',
							'MISSING_ITEM',
							'Non Serviceable Pincode',
							'non_serviceable',
							'not_serviceable',
							'Out of Delivery Area',
							'shipment_delayed',
							'shipment_lost',
							'Untraceable'
						) THEN 1
						ELSE 0
					END AS pd_new_fk_triggered_rto,
					CASE
						WHEN shipment_rto_create_date_key > logistics_promise_date_key
						AND ekl_shipment_type IN(
							'approved_rto',
							'unapproved_rto'
						)
						AND return_reason IN(
							'Damaged Shipment',
							'DAMAGED_MISSING_PRODUCT',
							'DAMAGED_PRODUCT',
							'delivery_delayed',
							'lost_in_transit',
							'mis_route',
							'mis_shipment',
							'MISSHIPMENT',
							'Mis-shipped Item',
							'MISSING_ITEM',
							'Non Serviceable Pincode',
							'non_serviceable',
							'not_serviceable',
							'Out of Delivery Area',
							'shipment_delayed',
							'shipment_lost',
							'Untraceable'
						) 
						AND shipment_breach_bucket != 'No Breach-OFD Before LPD-Customer Dependency Attempted' THEN 1
						ELSE 0
					END AS pd_fk_triggered_rto_after_lpd,
					CASE
						WHEN return_reason IN(
							'Damaged Shipment',
							'DAMAGED_MISSING_PRODUCT',
							'DAMAGED_PRODUCT',
							'MISSHIPMENT',
							'MISSING_ITEM'
						) THEN 1
						ELSE 0
					END AS pe_shipment_condition_breach,
					CASE
						WHEN ekl_shipment_type = 'forward'
						AND datediff(
							to_date(ekl_delivery_datetime),
							to_date(order_item_created_at)
						) > 4 THEN 1
						ELSE 0
					END AS pe_sla_breach,
					CASE
						WHEN fsd_number_of_ofd_attempts > 1 THEN 1
						ELSE 0
					END AS pe_delivery_attempt_breach,
					CASE
						WHEN ekl_shipment_type = 'forward'
						AND datediff(
							to_date(order_item_unit_final_promised_date_use),
							to_date(ekl_delivery_datetime)
						) > 2 THEN 1
						ELSE 0
					END AS pe_delivery_precision_breach,
					vendor_name,
					IF(
						ekl_shipment_type = 'forward'
						AND NOT isnull(ekl_delivery_datetime),
						datediff(
							to_date(ekl_delivery_datetime),
							to_date(order_item_created_at)
						),
						NULL
					) AS order_create_to_delivery_sla_days,
					picklist_display_id,
					picklist_created_at,
					picklist_item_confirmed_at,
					--rvp_promise_date,
					--rvp_promise_date_key,
					rvp_complete_date_key,
					rvp_tat,
					source_pincode,
					destination_pincode,
					current_day_key,
					product_length,
					product_breadth,
					product_height,
					product_shipping_weight,
					is_synergy_hub,
					rvp_complete_datetime,
					reverse_shipment_type
				FROM
					(
						SELECT
							C.runsheet_close_datetime,
							c.runsheet_close_date_key,
							C.tracking_id,
							C.return_reason,
							C.return_id,
							C.qc_reason,
							C.return_sub_reason,
							C.return_comments,
							C.return_type,
							C.return_date,
							C.shipment_dh_fwd_breach_flag,
							C.shipment_dh_pending_age,
							C.first_attempt_delivery_flag,
							C.shipment_dh_days_for_delivery,
							C.shipment_pending_location,
							C.first_attempt_pickup_flag,
							C.shipment_dh_fwd_promised_date_key,
							C.shipment_dh_rvp_promised_date_key,
							C.shipment_dh_rto_promised_date_key,
							C.rvp_pickup_complete_date_key,
							C.end_state_date_key,
							C.fsd_last_dhhub_sent_date_key,
							C.order_item_unit_final_promised_date_use_date_key,
							C.order_item_created_at_date_key,
							C.fulfill_item_unit_ship_expected_date_key,
							C.shipment_inscan_date_key,
							C.shipment_rto_create_date_key,
							C.fsd_assignedhub_received_date_key,
							C.shipment_first_ofd_date_key,
							C.shipment_last_ofd_date_key,
							C.vendor_dispatch_date_key,
							C.fsd_assigned_hub_sent_date_key,
							C.openbox_reject_date_key,
							C.present_day_key,
							C.openbox_reject_flag,
							C.shipment_dh_fwd_promised_date,
							C.shipment_dh_rvp_promised_date,
							C.shipment_dh_rto_promised_date,
							C.shipment_dh_pending_flag,
							C.ekl_pending_flag,
							C.present_day,
							C.order_final_unit_type,
							C.fsd_first_undel_customer_dependency_flag,
							C.fsd_current_customer_dependency_flag,
							C.logistics_promise_datetime,
							C.shipment_created_at_datetime,
							C.shipment_first_consignment_create_datetime,
							C.fc_pending_flag,
							C.fc_pending_age,
							C.fc_breach_flag,
							C.fc_breach_bucket,
							C.perfect_order_final_promised_date,
							C.calculated_delivery_hub,
							C.order_item_unit_id,
							c.cu_sub_issue_type,
							C.incident_creation_time,
							C.order_pincode_key,
							C.order_item_unit_quantity,
							C.order_item_quantity,
							C.order_item_selling_price,
							C.order_item_type,
							C.order_item_title,
							C.order_item_unit_status,
							C.order_item_unit_is_promised_date_updated,
							C.order_item_unit_new_promised_date,
							C.order_item_sub_type,
							C.order_item_sku,
							C.order_item_category_id,
							C.order_item_sub_status,
							C.order_item_ship_group_id,
							C.order_item_status,
							C.order_item_product_id_key,
							C.order_item_unit_init_promised_date_key,
							C.order_item_date_key,
							C.order_item_unit_final_promised_date_key,
							C.order_item_seller_id_key,
							C.order_item_time_key,
							C.order_item_unit_source_facility,
							C.order_item_listing_id_key,
							C.order_item_unit_final_promised_date_use,
							C.order_item_unit_init_promised_datetime,
							C.order_item_last_update,
							C.order_item_date,
							C.order_item_created_at,
							C.order_shipping_address_id_key,
							C.call_verification_reason,
							C.order_item_max_approved_time,
							C.order_item_max_on_hold_time,
							C.order_last_payment_method,
							C.call_verification_type,
							C.is_replacement,
							C.is_exchange,
							C.is_duplicate,
							C.account_id,
							C.account_id_key,
							C.exchange_fsn,
							C.replacement_order_item_id,
							C.tpt_intransit_breach_flag,
							C.mh_breach_flag,
							C.mh_promise_datetime,	
							C.order_external_id,
							C.order_id,
							C.order_item_id,
							C.order_item_unit_tracking_id,
							C.order_item_unit_shipment_id,
							C.tracking_id_type,
							C.fulfill_item_unit_dispatch_actual_time,
							C.fulfill_item_unit_ship_actual_time,
							C.fulfill_item_unit_dispatch_expected_time,
							C.fulfill_item_unit_ship_expected_time,
							C.fulfillment_order_item_id,
							C.slot_booking_ref_id,
							C.fulfill_item_unit_id,
							C.fulfill_item_unit_status,
							C.shipment_merchant_reference_id,
							C.fulfill_item_unit_updated_at,
							C.fulfill_item_unit_region,
							C.fulfill_item_unit_dispatch_actual_date_key,
							C.fulfill_item_unit_ship_actual_date_key,
							C.fulfill_item_unit_dispatch_expected_date_key,
							C.fulfill_item_unit_order_date_key,
							C.fulfill_item_unit_dispatch_actual_time_key,
							C.fulfill_item_unit_ship_actual_time_key,
							C.fulfill_item_unit_dispatch_expected_time_key,
							C.fulfill_item_unit_order_time_key,
							C.fulfill_item_unit_region_type,
							C.fulfill_item_id,
							C.fulfill_item_type,
							C.fulfill_item_unit_deliver_after_time,
							C.fulfill_item_unit_dispatch_after_time,
							C.fulfill_item_unit_deliver_after_date_key,
							C.fulfill_item_unit_deliver_after_time_key,
							C.fulfill_item_unit_dispatch_after_date_key,
							C.fulfill_item_unit_dispatch_after_time_key,
							C.fulfill_item_unit_is_for_slotted_delivery,
							C.fulfill_item_unit_reserved_status_b2c_actual_time,
							C.fulfill_item_unit_reserved_status_expected_time,
							C.fulfill_item_unit_reserved_status_b2c_actual_date_key,
							C.fulfill_item_unit_reserved_status_b2c_actual_time_key,
							C.fulfill_item_unit_reserved_status_expected_date_key,
							C.fulfill_item_unit_reserved_status_expected_time_key,
							C.fulfill_item_unit_delivered_status_expected_time,
							C.fulfill_item_unit_delivered_status_expected_date_key,
							C.fulfill_item_unit_delivered_status_expected_time_key,
							C.fulfill_item_unit_delivered_status_id,
							C.fulfill_item_unit_size,
							C.fulfill_item_unit_reserved_status_b2b_actual_date_key,
							C.fulfill_item_unit_reserved_status_b2b_actual_time_key,
							C.fulfill_item_unit_reserved_status_b2b_actual_time,
							C.slot_changed_created_at,
							C.slot_changed_by,
							C.product_categorization_hive_dim_key,
							C.product_id,
							C.analytic_super_category,
							C.brand,
							C.analytic_vertical,
							C.analytic_category,
							C.analytic_sub_category,
							C.shipment_id,
							C.vendor_tracking_id,
							C.shipment_current_status,
							C.shipment_current_status_date_key,
							C.shipment_current_status_time_key,
							C.destination_geo_id_key,
							C.ekl_shipment_type,
							C.customer_promise_date_key,
							C.customer_promise_time_key,
							C.logistics_promise_date_key,
							C.logistics_promise_time_key,
							C.shipment_created_at_date_key,
							C.shipment_created_at_time_key,
							C.vendor_id_key,
							C.shipment_carrier,
							C.fsd_assigned_hub_id_key,
							C.fsd_last_current_hub_id_key,
							C.fsd_last_current_hub_type,
							C.shipment_inscan_time,
							C.shipment_rto_create_time,
							C.fsd_last_received_time,
							C.fsd_number_of_ofd_attempts,
							C.fsd_assignedhub_expected_time,
							C.fsd_assignedhub_received_time,
							C.fsd_returnedtoekl_time,
							C.fsd_receivedbyekl_time,
							C.merchant_reference_id,
							C.ekl_delivery_datetime,
							C.ekl_delivery_date_key,
							C.ekl_delivery_time_key,
							C.shipment_first_ofd_datetime,
							C.shipment_last_ofd_datetime,
							C.vendor_dispatch_datetime,
							C.ekl_billable_weight,
							C.fsd_assigned_hub_sent_datetime,
							C.shipment_first_consignment_id,
							C.shipment_last_consignment_id,
							C.openbox_reject_datetime,
							C.merchant_id,
							C.fsd_first_dh_received_datetime,
							C.shipment_first_consignment_create_date_key,
							C.shipment_first_consignment_create_time_key,
							C.shipment_origin_facility_id_key,
							C.fsd_firstundeliverystatus,
							C.rvp_pickup_complete_datetime,
							C.shipment_rvp_pk_number_of_attempts,
							C.end_state_datetime,
							C.fsd_last_dhhub_sent_datetime,
							C.rvp_hub_id,
							C.rvp_hub_id_key,
							C.rvp_destination_facility_key,
							C.shipment_value,
							C.cod_amount_to_collect,
							C.rvp_origin_geo_id_key,
							C.ekl_facility_id,
							C.last_tasklist_agent_id,
							C.last_tasklist_agent_id_key,
							C.last_tasklist_updated_at,
							C.tasklist_type,
							C.last_tasklist_id,
							C.tasklist_status,
							C.runsheet_id,
							C.tasklist_tracking_id,
							C.ekl_facility_id_key,
							C.shipment_tpt_dh_eta_datetime,
							C.shipment_tpt_dh_eta_date_key,
							C.tpt_compliance_flag,
							C.actual_cons_created_datetime,
							C.conn_eta_in_sec,
							C.return_derived_reason,
							C.cancellation_reason,
							C.cancellation_sub_reason,
							C.cancellation_derived_reason,
							C.customer_contact_flag,
							C.customer_contact_issue_type,
							C.customer_contact_sub_sub_issue_type,
							IF(
								ekl_delivery_datetime IS NOT NULL,
								IF(
									to_date(ekl_delivery_datetime) <= to_date(perfect_order_final_promised_date),
									'PO_Delivered_by_promise',
									'PO_breached_delivered'
								),
								IF(
									to_date(perfect_order_final_promised_date) < present_day,
									'PO_pending_breached',
									'PO_pending_not_breached'
								)
							) AS perfect_order_breach_bucket,
							IF(
							    order_item_unit_is_promised_date_updated = 1
								AND slot_changed_by = 'ekl',
								1,
								0
							) AS fk_triggered_promise_change_flag,
							(
								CASE
									WHEN cancellation_derived_reason IN(
										'Seller',
										'MarketPlace'
									)
									AND cancellation_reason <> 'auto_decline_verification' THEN 1
									ELSE 0
								END
							) AS fk_triggered_cancellation_flag,
							IF(
								return_derived_reason = 'Flipkart',
								1,
								0
							) AS fk_triggered_return_flag,
							IF(
								order_item_status IN(
									'cancelled',
									'created',
									'returned',
									'retuned'
								)
								OR shipment_pending_location = 'not_pending',
								0,
								1
							) AS order_pending_flag,
							IF(
								order_item_status IN(
									'cancelled',
									'created',
									'returned',
									'retuned'
								),
								order_item_status,
								IF(
									order_item_status = 'on_hold',
									CONCAT( 'on_hold_', IF( NOT( isnull( call_verification_reason ) ), call_verification_reason, 'no_reason' ) ),
									IF(
										shipment_pending_location = 'not_pending',
										'not_pending',
										IF(
											isnull(fulfill_item_unit_reserved_status_b2c_actual_time)
											AND tracking_id_type = 'forward',
											'reservation_pending',
											IF(
												IsNull(order_item_unit_shipment_id)
												AND tracking_id_type = 'forward',
												'dispatch_pending',
												shipment_pending_location
											)
										)
									)
								)
							) AS order_final_status,
							IF(
								ekl_shipment_type = 'forward'
								OR isnull(ekl_shipment_type),
								IF(
									order_item_status IN(
										'on_hold',
										'created'
									),
									'CS',
									IF(
										order_item_status = 'approved'
										AND isnull(fulfill_item_unit_reserved_status_b2c_actual_time),
										'TECH',
										IF(
											order_item_status = 'approved'
											AND NOT(
												isnull(fulfill_item_unit_reserved_status_b2c_actual_time)
											),
											'WareHouse',
											shipment_pending_location
										)
									)
								),
								IF(
									ekl_shipment_type IN(
										'approved_rto',
										'unapproved_rto',
										'rvp'
									),
									shipment_pending_location,
									'RCA'
								)
							) AS order_pending_location,
							IF(
								order_item_status IN(
									'cancelled',
									'created',
									'returned',
									'retuned'
								)
								OR shipment_pending_location = 'not_pending',
								'not_pending',
								datediff(
									present_day,
									to_date(order_item_created_at)
								)
							) AS order_pending_age,
							IF(
	isnull(ekl_delivery_datetime) and isnull(shipment_first_ofd_datetime) and from_unixtime(unix_timestamp(),'yyyy-MM-dd') <= to_date(logistics_promise_datetime),'Future Pending',
		IF(
			NOT(isnull(ekl_delivery_datetime)) and to_date(ekl_delivery_datetime) <= to_date(logistics_promise_datetime),'delivered by logistics promise date',
			IF(shipment_carrier = 'FSD',
				IF(
					NOT(isnull(shipment_first_ofd_datetime)) and to_date(shipment_first_ofd_datetime) <= to_date(logistics_promise_datetime) and fsd_first_undel_customer_dependency_flag=1,'No Breach-OFD Before LPD-Customer Dependency Attempted',
						IF(
							to_date(logistics_promise_datetime)>to_date(order_item_unit_final_promised_date_use),'LPD>CPD - Tech_Issue',
								IF(
									from_unixtime(unix_timestamp(fulfill_item_unit_reserved_status_b2c_actual_time)+(150 * 60))> from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_time)),'Fulfilment_Breach',
										IF(
											from_unixtime(unix_timestamp(order_item_max_on_hold_time)+(150 * 60))> from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_time)),'CS_Breach',
												IF(
													isnull(fulfill_item_unit_dispatch_actual_time) AND from_unixtime(unix_timestamp())> from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_time)),'DC_Breach',
														IF(
															NOT(isnull(fulfill_item_unit_dispatch_actual_time))	AND from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_actual_time)) > from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_time)),'DC_Breach',
																IF(
																	isnull(shipment_first_consignment_create_datetime) AND from_unixtime(unix_timestamp())> from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_time)+(120 * 60)),'MotherHub_Breach',
																		IF(
																			NOT(isnull(shipment_first_consignment_create_datetime)) AND from_unixtime(unix_timestamp(shipment_first_consignment_create_datetime)) > from_unixtime(unix_timestamp(fulfill_item_unit_dispatch_expected_time)+(120 * 60)),'MotherHub_Breach',
																			IF(
																				tpt_intransit_breach_flag = 1,'Transport_Breach',
																						IF(
																							order_item_unit_is_promised_date_updated = 1 AND slot_changed_by = 'ekl','Promise Change by EKL',
																								IF(
																									order_item_unit_is_promised_date_updated = 1 AND slot_changed_by = 'cust',
																										IF
																										( COALESCE(shipment_first_ofd_date_key,current_day_key) <= 	order_item_unit_final_promised_date_key,'Promise Change by Customer','Breached after Promise Change by Customer'
																										),
																										IF(
																										UPPER( ekl_shipment_type ) 	IN('APPROVED_RTO','UNAPPROVED_RTO')
																										AND to_date(shipment_rto_create_time) > to_date(order_item_unit_final_promised_date_use),'Ops.Issues','Breach-EKL_DH'
																										)
																								)
																						)																		
																				)
																		)
																)
																
														)
												)
										)
								)
						)				
			    ),
				IF(to_date(shipment_first_ofd_datetime) <= to_date(order_item_unit_final_promised_date_use) and fsd_first_undel_customer_dependency_flag = 1,'Customer_Dependency-Attempted Undelivered-3PL','Breach-3PL')
			)	
		)
) AS shipment_breach_bucket
,
							IF(
								shipment_carrier = 'FSD',
								IF(
									to_date(ekl_delivery_datetime) <= to_date(order_item_unit_final_promised_date_use),
									0,
									1
								),
								IF(
									to_date(ekl_delivery_datetime) <= to_date(order_item_unit_final_promised_date_use),
									0,
									1
								)
							) AS shipment_breach_flag,
							IF(
								ekl_pending_flag = 'pending',
								IF(
									ekl_shipment_type IN(
										'unapproved_rto',
										'approved_rto'
									),
									datediff(
										present_day,
										to_date(shipment_rto_create_time)
									),
									datediff(
										present_day,
										to_date(shipment_created_at_datetime)
									)
								),
								0
							) AS shipment_pending_age,
							IF(
								ekl_shipment_type = 'rvp',
								IF(
									to_date(rvp_pickup_complete_datetime) <= date_add(
										to_date(shipment_created_at_datetime),
										2
									),
									0,
									1
								),
								NULL
							) AS rvp_pickup_breach_flag,
							slot_change_reason,
							last_conn_cutoff_in_sec,
							shipment_last_consignment_create_datetime,
							shipment_last_consignment_eta_in_sec,
							fulfill_item_unit_final_reserved_datetime,
							fulfill_item_unit_final_reserved_date_key,
							C.seller_id,
							C.payment_mode,
							C.pos_id,
							C.transaction_id,
							C.agent_id,
							C.amount_collected,
							C.seller_type,
							C.shipment_dg_flag,
							C.shipment_fragile_flag,
							C.seller_id_key,
							C.pos_id_key,
							C.shipment_agent_id_key,
							C.pincode_location_type,
							C.is_cpu_vendor,
							C.lzn_classification,
							C.lzn_tat_target,
							C.vendor_name,
							C.picklist_display_id,
							C.picklist_created_at,
							C.picklist_item_confirmed_at,
							--C.rvp_promise_date,
							--C.rvp_promise_date_key,
							C.rvp_complete_date_key,
							current_day_key,
							C.rvp_tat,
							C.source_pincode,
							C.destination_pincode,
							C.product_length,
							C.product_breadth,
							C.product_height,
							C.product_shipping_weight,
							C.is_synergy_hub,
							C.rvp_complete_datetime,
							C.reverse_shipment_type
						FROM
							(
								SELECT
									B.shipment_dh_fwd_promised_date,
									B.runsheet_close_datetime,
									B.runsheet_close_date_key,
									B.tracking_id,
									B.return_reason,
									B.return_id,
									B.qc_reason,
									B.return_sub_reason,
									B.return_comments,
									B.shipment_last_consignment_eta_in_sec,
									B.return_type,
									B.return_date,
									B.shipment_dh_rvp_promised_date,
									B.shipment_dh_rto_promised_date,
									B.shipment_dh_pending_flag,
									B.ekl_pending_flag,
									B.present_day,
									B.order_final_unit_type,
									B.fsd_first_undel_customer_dependency_flag,
									B.fsd_current_customer_dependency_flag,
									B.logistics_promise_datetime,
									B.shipment_created_at_datetime,
									B.shipment_last_consignment_create_datetime,
									B.shipment_first_consignment_create_datetime,
									B.fc_pending_flag,
									B.fc_pending_age,
									B.fc_breach_flag,
									B.fc_breach_bucket,
									B.perfect_order_final_promised_date,
									B.order_item_unit_id,
									B.cu_sub_issue_type,
									B.incident_creation_time,
									B.order_pincode_key,
									B.calculated_delivery_hub,
									B.order_item_unit_quantity,
									B.order_item_quantity,
									B.order_item_selling_price,
									B.order_item_type,
									B.order_item_title,
									B.order_item_unit_status,
									B.order_item_unit_is_promised_date_updated,
									B.order_item_unit_new_promised_date,
									B.order_item_sub_type,
									B.order_item_sku,
									B.order_item_category_id,
									B.order_item_sub_status,
									B.order_item_ship_group_id,
									B.order_item_status,
									B.order_item_product_id_key,
									B.order_item_unit_init_promised_date_key,
									B.order_item_date_key,
									B.order_item_unit_final_promised_date_key,
									B.order_item_seller_id_key,
									B.order_item_time_key,
									B.order_item_unit_source_facility,
									B.order_item_listing_id_key,
									B.order_item_unit_final_promised_date_use,
									B.order_item_unit_init_promised_datetime,
									B.order_item_last_update,
									B.order_item_date,
									B.order_item_created_at,
									B.order_shipping_address_id_key,
									B.call_verification_reason,
									B.order_item_max_approved_time,
									B.order_item_max_on_hold_time,
									B.order_last_payment_method,
									B.call_verification_type,
									B.is_replacement,
									B.is_exchange,
									B.is_duplicate,
									B.account_id,
									B.account_id_key,
									B.exchange_fsn,
									B.replacement_order_item_id,
									B.tpt_intransit_breach_flag,
									B.mh_breach_flag,
									B.mh_promise_datetime,
									B.order_external_id,
									B.order_id,
									B.order_item_id,
									B.order_item_unit_tracking_id,
									B.order_item_unit_shipment_id,
									B.tracking_id_type,
									B.fulfill_item_unit_dispatch_actual_time,
									B.fulfill_item_unit_ship_actual_time,
									B.fulfill_item_unit_dispatch_expected_time,
									B.fulfill_item_unit_ship_expected_time,
									B.slot_booking_ref_id,
									B.fulfillment_order_item_id,
									B.fulfill_item_unit_id,
									B.fulfill_item_unit_status,
									B.shipment_merchant_reference_id,
									B.fulfill_item_unit_updated_at,
									B.fulfill_item_unit_region,
									B.fulfill_item_unit_dispatch_actual_date_key,
									B.fulfill_item_unit_ship_actual_date_key,
									B.fulfill_item_unit_dispatch_expected_date_key,
									B.fulfill_item_unit_order_date_key,
									B.fulfill_item_unit_dispatch_actual_time_key,
									B.fulfill_item_unit_ship_actual_time_key,
									B.fulfill_item_unit_dispatch_expected_time_key,
									B.fulfill_item_unit_order_time_key,
									B.fulfill_item_unit_region_type,
									B.fulfill_item_id,
									B.fulfill_item_type,
									B.fulfill_item_unit_deliver_after_time,
									B.fulfill_item_unit_dispatch_after_time,
									B.fulfill_item_unit_deliver_after_date_key,
									B.fulfill_item_unit_deliver_after_time_key,
									B.fulfill_item_unit_dispatch_after_date_key,
									B.fulfill_item_unit_dispatch_after_time_key,
									B.fulfill_item_unit_is_for_slotted_delivery,
									B.fulfill_item_unit_reserved_status_b2c_actual_time,
									B.fulfill_item_unit_reserved_status_expected_time,
									B.fulfill_item_unit_reserved_status_b2c_actual_date_key,
									B.fulfill_item_unit_reserved_status_b2c_actual_time_key,
									B.fulfill_item_unit_reserved_status_expected_date_key,
									B.fulfill_item_unit_reserved_status_expected_time_key,
									B.fulfill_item_unit_delivered_status_expected_time,
									B.fulfill_item_unit_delivered_status_expected_date_key,
									B.fulfill_item_unit_delivered_status_expected_time_key,
									B.fulfill_item_unit_delivered_status_id,
									B.fulfill_item_unit_size,
									B.fulfill_item_unit_reserved_status_b2b_actual_date_key,
									B.fulfill_item_unit_reserved_status_b2b_actual_time_key,
									B.fulfill_item_unit_reserved_status_b2b_actual_time,
									B.slot_changed_created_at,
									B.slot_changed_by,
									B.product_categorization_hive_dim_key,
									B.product_id,
									B.analytic_super_category,
									B.brand,
									B.analytic_vertical,
									B.analytic_category,
									B.analytic_sub_category,
									B.shipment_id,
									B.vendor_tracking_id,
									B.shipment_current_status,
									B.shipment_current_status_date_key,
									B.shipment_current_status_time_key,
									B.destination_geo_id_key,
									B.ekl_shipment_type,
									B.customer_promise_date_key,
									B.customer_promise_time_key,
									B.logistics_promise_date_key,
									B.logistics_promise_time_key,
									B.shipment_created_at_date_key,
									B.shipment_created_at_time_key,
									B.vendor_id_key,
									B.shipment_carrier,
									B.fsd_assigned_hub_id_key,
									B.fsd_last_current_hub_id_key,
									B.fsd_last_current_hub_type,
									B.shipment_inscan_time,
									B.shipment_rto_create_time,
									B.fsd_last_received_time,
									B.fsd_number_of_ofd_attempts,
									B.fsd_assignedhub_expected_time,
									B.fsd_assignedhub_received_time,
									B.fsd_returnedtoekl_time,
									B.fsd_receivedbyekl_time,
									B.merchant_reference_id,
									B.ekl_delivery_datetime,
									B.ekl_delivery_date_key,
									B.ekl_delivery_time_key,
									B.shipment_first_ofd_datetime,
									B.shipment_last_ofd_datetime,
									B.vendor_dispatch_datetime,
									B.ekl_billable_weight,
									B.fsd_assigned_hub_sent_datetime,
									B.shipment_first_consignment_id,
									B.shipment_last_consignment_id,
									B.openbox_reject_datetime,
									B.merchant_id,
									B.fsd_first_dh_received_datetime,
									B.shipment_first_consignment_create_date_key,
									B.shipment_first_consignment_create_time_key,
									B.shipment_origin_facility_id_key,
									B.fsd_firstundeliverystatus,
									B.rvp_pickup_complete_datetime,
									B.shipment_rvp_pk_number_of_attempts,
									B.end_state_datetime,
									B.fsd_last_dhhub_sent_datetime,
									B.rvp_hub_id,
									B.rvp_hub_id_key,
									B.rvp_destination_facility_key,
									B.shipment_value,
									B.cod_amount_to_collect,
									B.rvp_origin_geo_id_key,
									B.ekl_facility_id,
									B.last_tasklist_agent_id,
									B.last_tasklist_agent_id_key,
									B.last_tasklist_updated_at,
									B.tasklist_type,
									B.last_tasklist_id,
									B.tasklist_status,
									B.runsheet_id,
									B.tasklist_tracking_id,
									B.ekl_facility_id_key,
									B.shipment_tpt_dh_eta_datetime,
									B.shipment_tpt_dh_eta_date_key,
									B.tpt_compliance_flag,
									B.actual_cons_created_datetime,
									B.conn_eta_in_sec,
									B.return_derived_reason,
									B.cancellation_reason,
									B.cancellation_sub_reason,
									B.cancellation_derived_reason,
									B.customer_contact_flag,
									B.customer_contact_issue_type,
									B.customer_contact_sub_sub_issue_type,
									IF(
										tracking_id_type = 'forward'
										AND ekl_shipment_type = 'forward',
										IF(
											to_date(ekl_delivery_datetime) <= shipment_dh_fwd_promised_date,
											0,
											IF(
												shipment_dh_pending_flag = 0,
												'not@DH',
												IF(
													fsd_first_undel_customer_dependency_flag = 1,
													'No Breach-OFD Before LPD-Customer Dependency Attempted',
													1
												)
											)
										),
										'not_fwd_shipment'
									) AS shipment_dh_fwd_breach_flag,
									IF(
										tracking_id_type = 'forward',
										IF(
											shipment_dh_pending_flag = 0,
											- 100,
											datediff(
												present_day,
												to_date(fsd_assignedhub_received_time)
											)
										),
										IF(
											shipment_dh_pending_flag = 0,
											- 100,
											datediff(
												present_day,
												to_date(rvp_pickup_complete_datetime)
											)
										)
									) AS shipment_dh_pending_age,
									IF(
										tracking_id_type = 'forward',
										IF(
											NOT isnull(ekl_delivery_datetime)
											AND fsd_number_of_ofd_attempts = 1,
											1,
											0
										),
										NULL
									) AS first_attempt_delivery_flag,
									IF(
										ekl_shipment_type = 'forward'
										AND NOT isnull(ekl_delivery_datetime)
										AND NOT isnull(fsd_assignedhub_received_time),
										datediff(
											to_date(ekl_delivery_datetime),
											to_date(fsd_assignedhub_received_time)
										),
										NULL
									) AS shipment_dh_days_for_delivery,
									IF(
										ekl_pending_flag = 'not_pending',
										'not_pending',
										IF(
											ekl_shipment_type = 'rvp'
											AND shipment_carrier = 'FSD'
											AND isnull(rvp_pickup_complete_datetime),
											'RVP_pickup_pending',
											IF(
												shipment_carrier = 'VNF',
												'VNF',
												IF(
													shipment_dh_pending_flag = 1,
													'DH_pending',
													IF(
														upper( shipment_current_status ) = 'OUT_FOR_DELIVERY',
														'Out_For_Delivery',
														IF(
															shipment_current_status = 'Expected',
															'Intransit',
															IF(
																ekl_shipment_type = 'forward',
																IF(
																	isnull(shipment_inscan_time),
																	'MH_inscan_pending',
																	IF(
																		shipment_carrier = '3PL',
																		IF(
																			shipment_current_status = 'received',
																			'3PL_Central_Hub_Pending',
																			'3PL_pending'
																		),
																		IF(
																			isnull(shipment_first_consignment_id),
																			'consignment_creation_pending',
																			fsd_last_current_hub_type
																		)
																	)
																),
																IF(
																	shipment_current_status IN(
																		'Received_By_Ekl',
																		'Returned_To_Ekl'
																	),
																	'Central_Hub_Pending',
																	IF(
																		shipment_carrier = '3PL',
																		IF(
																			shipment_current_status IN(
																				'dispatched_to_merchant',
																				'received_by_merchant',
																				'marked_for_merchant_dispatch',
																				'received_with_error'
																			),
																			fsd_last_current_hub_type,
																			'3PL_pending'
																		),
																		fsd_last_current_hub_type
																	)
																)
															)
														)
													)
												)
											)
										)
									) AS shipment_pending_location,
									IF(
										ekl_shipment_type = 'rvp',
										IF(
											NOT isnull(rvp_pickup_complete_datetime)
											AND shipment_rvp_pk_number_of_attempts = 1,
											1,
											0
										),
										NULL
									) AS first_attempt_pickup_flag,
									lookup_date(shipment_dh_fwd_promised_date) AS shipment_dh_fwd_promised_date_key,
									lookup_date(shipment_dh_rvp_promised_date) AS shipment_dh_rvp_promised_date_key,
									lookup_date(shipment_dh_rto_promised_date) AS shipment_dh_rto_promised_date_key,
									lookup_date(
										to_date(rvp_pickup_complete_datetime)
									) AS rvp_pickup_complete_date_key,
									lookup_date(
										to_date(end_state_datetime)
									) AS end_state_date_key,
									lookup_date(
										to_date(fsd_last_dhhub_sent_datetime)
									) AS fsd_last_dhhub_sent_date_key,
									lookup_date(order_item_unit_final_promised_date_use) AS order_item_unit_final_promised_date_use_date_key,
									lookup_date(
										to_date(order_item_created_at)
									) AS order_item_created_at_date_key,
									lookup_date(
										to_date(fulfill_item_unit_ship_expected_time)
									) AS fulfill_item_unit_ship_expected_date_key,
									lookup_date(
										to_date(shipment_inscan_time)
									) AS shipment_inscan_date_key,
									lookup_date(
										to_date(shipment_rto_create_time)
									) AS shipment_rto_create_date_key,
									lookup_date(
										to_date(fsd_assignedhub_received_time)
									) AS fsd_assignedhub_received_date_key,
									lookup_date(
										to_date(shipment_first_ofd_datetime)
									) AS shipment_first_ofd_date_key,
									lookup_date(
										to_date(shipment_last_ofd_datetime)
									) AS shipment_last_ofd_date_key,
									lookup_date(
										to_date(vendor_dispatch_datetime)
									) AS vendor_dispatch_date_key,
									lookup_date(
										to_date(fsd_assigned_hub_sent_datetime)
									) AS fsd_assigned_hub_sent_date_key,
									lookup_date(
										to_date(openbox_reject_datetime)
									) AS openbox_reject_date_key,
									lookup_date(present_day) AS present_day_key,
									IF(
										isnull(openbox_reject_datetime),
										0,
										1
									) AS openbox_reject_flag,
									slot_change_reason,
									last_conn_cutoff_in_sec,
									fulfill_item_unit_final_reserved_datetime,
									fulfill_item_unit_final_reserved_date_key,
									B.seller_id,
									B.payment_mode,
									B.pos_id,
									B.transaction_id,
									B.agent_id,
									B.amount_collected,
									B.seller_type,
									B.shipment_dg_flag,
									B.shipment_fragile_flag,
									B.seller_id_key,
									B.pos_id_key,
									B.shipment_agent_id_key,
									B.pincode_location_type,
									B.is_cpu_vendor,
									B.lzn_classification,
									B.lzn_tat_target,
									B.vendor_name,
									B.picklist_display_id,
									B.picklist_created_at,
									B.picklist_item_confirmed_at,
									--B.rvp_promise_date,
									--B.rvp_promise_date_key,
									B.rvp_complete_date_key,
									B.rvp_tat,
									B.source_pincode,
									B.destination_pincode,
									lookup_date(to_date(cast(current_timestamp() as string))) as current_day_key,
									B.product_length,
									B.product_breadth,
									B.product_height,
									B.product_shipping_weight,
									B.is_synergy_hub,
									B.rvp_complete_datetime,
									B.reverse_shipment_type
								FROM
									(
										SELECT
											A.order_item_unit_id,
											A.order_pincode_key,
											A.cu_sub_issue_type,
											A.incident_creation_time,
											A.calculated_delivery_hub,
											A.runsheet_close_datetime,
											A.runsheet_close_date_key,
											A.tracking_id,
											A.return_reason,
											A.return_id,
											A.qc_reason,
											A.return_sub_reason,
											A.return_comments,
											A.return_type,
											A.return_date,
											A.order_item_unit_quantity,
											A.order_item_quantity,
											A.order_item_selling_price,
											A.order_item_type,
											A.order_item_title,
											A.order_item_unit_status,
											A.order_item_unit_is_promised_date_updated,
											A.order_item_unit_new_promised_date,
											A.order_item_sub_type,
											A.order_item_sku,
											A.order_item_category_id,
											A.order_item_sub_status,
											A.order_item_ship_group_id,
											A.order_item_status,
											A.order_item_product_id_key,
											A.order_item_unit_init_promised_date_key,
											A.order_item_date_key,
											A.order_item_unit_final_promised_date_key,
											A.order_item_seller_id_key,
											A.order_item_time_key,
											A.order_item_unit_source_facility,
											A.order_item_listing_id_key,
											A.order_item_unit_final_promised_date_use,
											A.order_item_unit_init_promised_datetime,
											A.order_item_last_update,
											A.order_item_date,
											A.order_item_created_at,
											A.order_shipping_address_id_key,
											A.call_verification_reason,
											A.order_item_max_approved_time,
											A.shipment_last_consignment_eta_in_sec,
											A.order_item_max_on_hold_time,
											A.order_last_payment_method,
											A.call_verification_type,
											A.is_replacement,
											A.is_exchange,
											A.is_duplicate,
											A.account_id,
											A.account_id_key,
											A.exchange_fsn,
											A.replacement_order_item_id,
											A.tpt_intransit_breach_flag,
											A.mh_breach_flag,
											A.mh_promise_datetime,
											A.order_external_id,
											A.order_id,
											A.order_item_id,
											A.order_item_unit_tracking_id,
											A.order_item_unit_shipment_id,
											A.tracking_id_type,
											A.fulfill_item_unit_dispatch_actual_time,
											A.fulfill_item_unit_ship_actual_time,
											A.fulfill_item_unit_dispatch_expected_time,
											A.fulfill_item_unit_ship_expected_time,
											A.slot_booking_ref_id,
											A.fulfillment_order_item_id,
											A.fulfill_item_unit_id,
											A.fulfill_item_unit_status,
											A.shipment_merchant_reference_id,
											A.fulfill_item_unit_updated_at,
											A.fulfill_item_unit_region,
											A.fulfill_item_unit_dispatch_actual_date_key,
											A.fulfill_item_unit_ship_actual_date_key,
											A.fulfill_item_unit_dispatch_expected_date_key,
											A.fulfill_item_unit_order_date_key,
											A.fulfill_item_unit_dispatch_actual_time_key,
											A.fulfill_item_unit_ship_actual_time_key,
											A.fulfill_item_unit_dispatch_expected_time_key,
											A.fulfill_item_unit_order_time_key,
											A.fulfill_item_unit_region_type,
											A.fulfill_item_id,
											A.fulfill_item_type,
											A.fulfill_item_unit_deliver_after_time,
											A.fulfill_item_unit_dispatch_after_time,
											A.fulfill_item_unit_deliver_after_date_key,
											A.fulfill_item_unit_deliver_after_time_key,
											A.fulfill_item_unit_dispatch_after_date_key,
											A.fulfill_item_unit_dispatch_after_time_key,
											A.fulfill_item_unit_is_for_slotted_delivery,
											A.fulfill_item_unit_reserved_status_b2c_actual_time,
											A.fulfill_item_unit_reserved_status_expected_time,
											A.fulfill_item_unit_reserved_status_b2c_actual_date_key,
											A.fulfill_item_unit_reserved_status_b2c_actual_time_key,
											A.fulfill_item_unit_reserved_status_expected_date_key,
											A.fulfill_item_unit_reserved_status_expected_time_key,
											A.fulfill_item_unit_delivered_status_expected_time,
											A.fulfill_item_unit_delivered_status_expected_date_key,
											A.fulfill_item_unit_delivered_status_expected_time_key,
											A.fulfill_item_unit_delivered_status_id,
											A.fulfill_item_unit_size,
											A.fulfill_item_unit_reserved_status_b2b_actual_date_key,
											A.fulfill_item_unit_reserved_status_b2b_actual_time_key,
											A.fulfill_item_unit_reserved_status_b2b_actual_time,
											A.slot_changed_created_at,
											A.slot_changed_by,
											A.product_categorization_hive_dim_key,
											A.product_id,
											A.analytic_super_category,
											A.brand,
											A.analytic_vertical,
											A.analytic_category,
											A.analytic_sub_category,
											A.shipment_id,
											A.vendor_tracking_id,
											A.shipment_current_status,
											A.shipment_current_status_date_key,
											A.shipment_current_status_time_key,
											A.destination_geo_id_key,
											A.ekl_shipment_type,
											A.customer_promise_date_key,
											A.customer_promise_time_key,
											A.logistics_promise_date_key,
											A.logistics_promise_time_key,
											A.shipment_created_at_date_key,
											A.shipment_created_at_time_key,
											A.vendor_id_key,
											A.shipment_carrier,
											A.fsd_assigned_hub_id_key,
											A.fsd_last_current_hub_id_key,
											A.fsd_last_current_hub_type,
											A.shipment_inscan_time,
											A.shipment_rto_create_time,
											A.fsd_last_received_time,
											A.fsd_number_of_ofd_attempts,
											A.fsd_assignedhub_expected_time,
											A.fsd_assignedhub_received_time,
											A.fsd_returnedtoekl_time,
											A.fsd_receivedbyekl_time,
											A.merchant_reference_id,
											A.ekl_delivery_datetime,
											A.ekl_delivery_date_key,
											A.ekl_delivery_time_key,
											A.shipment_first_ofd_datetime,
											A.shipment_last_ofd_datetime,
											A.vendor_dispatch_datetime,
											A.ekl_billable_weight,
											A.fsd_assigned_hub_sent_datetime,
											A.shipment_first_consignment_id,
											A.shipment_last_consignment_id,
											A.openbox_reject_datetime,
											A.merchant_id,
											A.fsd_first_dh_received_datetime,
											A.shipment_first_consignment_create_date_key,
											A.shipment_first_consignment_create_time_key,
											A.shipment_origin_facility_id_key,
											A.fsd_firstundeliverystatus,
											A.rvp_pickup_complete_datetime,
											A.shipment_rvp_pk_number_of_attempts,
											A.end_state_datetime,
											A.fsd_last_dhhub_sent_datetime,
											A.rvp_hub_id,
											A.rvp_hub_id_key,
											A.rvp_destination_facility_key,
											A.shipment_value,
											A.cod_amount_to_collect,
											A.rvp_origin_geo_id_key,
											A.ekl_facility_id,
											A.last_tasklist_agent_id,
											A.last_tasklist_agent_id_key,
											A.last_tasklist_updated_at,
											A.tasklist_type,
											A.last_tasklist_id,
											A.tasklist_status,
											A.runsheet_id,
											A.tasklist_tracking_id,
											A.ekl_facility_id_key,
											A.shipment_tpt_dh_eta_datetime,
											A.shipment_tpt_dh_eta_date_key,
											A.tpt_compliance_flag,
											A.actual_cons_created_datetime,
											A.conn_eta_in_sec,
											A.return_derived_reason,
											A.cancellation_reason,
											A.cancellation_sub_reason,
											A.cancellation_derived_reason,
											A.customer_contact_flag,
											A.customer_contact_issue_type,
											A.customer_contact_sub_sub_issue_type,
											IF(
												tracking_id_type = 'forward',
												IF(
													to_date(fsd_assignedhub_received_time) <= to_date(order_item_unit_final_promised_date_use),
													to_date(order_item_unit_final_promised_date_use),
													to_date(fsd_assignedhub_received_time)
												),
												NULL
											) AS shipment_dh_fwd_promised_date,
											IF(
												ekl_shipment_type = 'rvp'
												AND NOT IsNull(rvp_pickup_complete_datetime),
												(
													CASE
														WHEN rvp_hub_id IN(
															592,
															497,
															498,
															590,
															591,
															500,
															593
														) THEN date_add(
															to_date(rvp_pickup_complete_datetime),
															2
														)
														WHEN rvp_hub_id IN(595) THEN date_add(
															to_date(rvp_pickup_complete_datetime),
															3
														)
														ELSE date_add(
															to_date(rvp_pickup_complete_datetime),
															1
														)
													END
												),
												NULL
											) AS shipment_dh_rvp_promised_date,
											IF(
												tracking_id_type = 'forward',
												IF(
													ekl_shipment_type IN(
														'approved_rto',
														'unapproved_rto'
													),
													date_add(
														to_date(shipment_rto_create_time),
														1
													),
													NULL
												),
												NULL
											) AS shipment_dh_rto_promised_date,
											IF(
												fsd_last_current_hub_type IN(
													'BULK_HUB',
													'DELIVERY_HUB'
												)
												AND shipment_current_status <> 'Expected',
												1,
												0
											) AS shipment_dh_pending_flag,
											IF(
												NOT(
													isnull(ekl_delivery_datetime)
												)
												OR NOT isnull(end_state_datetime)
												OR shipment_current_status IN(
													'not_received',
													'reverse_pickup_cancelled',
													'PICKUP_Cancelled',
													'NotPicked_Attempted_CustomerHappyWithProduct',
													'expected',
													'dispatch_failed',
													'Delivered',
													'delivered',
													'reshipped',
													'delivered',
													'returned_to_seller',
													'received_by_merchant',
													'damaged',
													'returned',
													'Undelivered_UntraceableFromHub',
													'lost',
													'Lost',
													'Not_Received',
													'dispatched_to_facility',
													'dispatched_to_merchant',
													'received',
													'rvp_completed',
													'rvp_handover_completed',
													'rvp_handover_initiated'
												),
												'not_pending',
												'pending'
											) AS ekl_pending_flag,
											to_date(
												from_unixtime(
													unix_timestamp()
												)
											) AS present_day,
											IF(
												order_item_status = 'created',
												'not_approved',
												IF(
													order_item_status = 'cancelled',
													'cancelled',
													IF(
														order_item_status IN(
															'returned',
															'retuned',
															'return_requested'
														)
														OR ekl_shipment_type IN(
															'approved_rto',
															'unapproved_rto',
															'rvp'
														),
														'reverse',
														'forward'
													)
												)
											) AS order_final_unit_type,
											IF(
												lower(trim(fsd_firstundeliverystatus)) IN(
													'undelivered_attempted',
													'undelivered_cod_not_ready',
													'undelivered_customer_not_available',
													'undelivered_door_lock',
													'undelivered_incomplete_address',
													'undelivered_no_response',
													'undelivered_nonserviceablepincode',
													'undelivered_order_rejected_by_customer',
													'undelivered_order_rejected_opendelivery',
													'undelivered_outofdeliveryarea',
													'undelivered_request_for_reschedule',
													'customer unavailable',
													'customer no response',
													'cod not ready',
													'customer rejection',
													'reattempt',
													'dispute',
													'self collect',
													'request to reschedule',
													'consignee shifted',
													'address pincode mismatch',
													'incomplete address',
													'qc failed - replacement'
												),
												1,
												0
											) AS fsd_first_undel_customer_dependency_flag,
											IF(
												shipment_current_status IN(
													'Undelivered_Attempted',
													'Undelivered_COD_Not_Ready',
													'Undelivered_Customer_Not_Available',
													'Undelivered_Door_Lock',
													'Undelivered_Incomplete_Address',
													'Undelivered_No_Response',
													'Undelivered_NonServiceablePincode',
													'Undelivered_Order_Rejected_By_Customer',
													'Undelivered_Order_Rejected_OpenDelivery',
													'Undelivered_OutOfDeliveryArea',
													'Undelivered_Request_For_Reschedule'
												),
												1,
												0
											) AS fsd_current_customer_dependency_flag,
											from_unixtime(
												unix_timestamp(
													CONCAT( logistics_promise_date_key, SUBSTR( LPAD( logistics_promise_time_key, 4, '0' ), 1, 4 ) ),
													'yyyyMMddHHmm'
												)
											) AS logistics_promise_datetime,
											shipment_created_at_datetime,
											shipment_last_consignment_create_datetime,
											from_unixtime(
												unix_timestamp(
													CONCAT( shipment_first_consignment_create_date_key, SUBSTR( LPAD( shipment_first_consignment_create_time_key, 4, '0' ), 1, 4 ) ),
													'yyyyMMddHHmm'
												)
											) AS shipment_first_consignment_create_datetime,
											IF(
												isnull(fulfill_item_unit_dispatch_actual_time),
												IF(
													from_unixtime(
														unix_timestamp(fulfill_item_unit_dispatch_expected_time),
														'yyyy-MM-dd'
													) < from_unixtime(
														unix_timestamp(),
														'yyyy-MM-dd'
													),
													'pending_without_breach',
													'pending_with_breach'
												),
												'not_pending'
											) AS fc_pending_flag,
											IF(
												from_unixtime(
													unix_timestamp(fulfill_item_unit_dispatch_expected_time),
													'yyyy-MM-dd'
												) < from_unixtime(
													unix_timestamp(),
													'yyyy-MM-dd'
												),
												IF(
													isnull(fulfill_item_unit_dispatch_actual_time),
													datediff(
														from_unixtime(
															unix_timestamp(),
															'yyyy-MM-dd'
														),
														from_unixtime(
															unix_timestamp(fulfill_item_unit_dispatch_expected_time),
															'yyyy-MM-dd'
														)
													) - 1,
													NULL
												),
												NULL
											) AS fc_pending_age,
											IF(
												isnull(fulfill_item_unit_dispatch_actual_time),
												IF(
													from_unixtime(
														unix_timestamp(fulfill_item_unit_dispatch_expected_time),
														'yyyy-MM-dd'
													) < from_unixtime(
														unix_timestamp(),
														'yyyy-MM-dd'
													),
													1,
													0
												),
												IF(
													fulfill_item_unit_dispatch_expected_time < fulfill_item_unit_dispatch_actual_time,
													1,
													0
												)
											) AS fc_breach_flag,
											IF(
												isnull(fulfill_item_unit_dispatch_actual_time)
												AND(
													from_unixtime(
														unix_timestamp(fulfill_item_unit_dispatch_expected_time),
														'yyyy-MM-dd'
													) >= from_unixtime(
														unix_timestamp(),
														'yyyy-MM-dd'
													)
												),
												'In-Progress',
												IF(
													fulfill_item_unit_dispatch_expected_time >= fulfill_item_unit_dispatch_actual_time,
													'FC_fulfilled',
													IF(
														from_unixtime(
															unix_timestamp(order_item_max_on_hold_time) +(
																5 * 60 * 30
															)
														) > from_unixtime(
															unix_timestamp(fulfill_item_unit_dispatch_expected_time)
														),
														'FC_CS_Breach',
														IF(
															from_unixtime(
																unix_timestamp(fulfill_item_unit_final_reserved_datetime) +(
																	5 * 60 * 30
																)
															) > from_unixtime(
																unix_timestamp(fulfill_item_unit_dispatch_expected_time)
															),
															'FC_Fulfilment_Tech_Breach',
															'FC_Operations_Breach'
														)
													)
												)
											) AS fc_breach_bucket,
											IF(
												slot_changed_by = 'cust',
												order_item_unit_final_promised_date_use,
												order_item_unit_init_promised_datetime
											) AS perfect_order_final_promised_date,
											last_conn_cutoff_in_sec,
											slot_change_reason,
											fulfill_item_unit_final_reserved_datetime,
											lookup_date(to_Date(fulfill_item_unit_final_reserved_datetime)) AS fulfill_item_unit_final_reserved_date_key,
											A.seller_id,
											A.payment_mode,
											A.pos_id,
											A.transaction_id,
											A.agent_id,
											A.amount_collected,
											A.seller_type,
											A.shipment_dg_flag,
											A.shipment_fragile_flag,
											A.seller_id_key,
											A.pos_id_key,
											A.shipment_agent_id_key,
											A.pincode_location_type,
											A.is_cpu_vendor,
											A.lzn_classification,
											A.lzn_tat_target,
											A.vendor_name,
											A.picklist_display_id,
											A.picklist_created_at,
											A.picklist_item_confirmed_at,
											-- A.rvp_promise_date,
											--  lookup_date(A.rvp_promise_date) as rvp_promise_date_key,
											A.rvp_complete_date_key,
											A.rvp_tat,
											A.source_pincode,
											A.destination_pincode,
											A.product_length,
											A.product_breadth,
											A.product_height,
											A.product_shipping_weight,
											A.is_synergy_hub,
											A.rvp_complete_datetime,
											A.reverse_shipment_type
										FROM
											(
												SELECT
													order_tracking.order_external_id,
													order_tracking.order_id,
													order_tracking.order_item_id,
													order_tracking.order_item_unit_tracking_id,
													order_tracking.order_item_unit_shipment_id,
													order_tracking.tracking_id_type,
													--order_tracking.created_on as rvp_created_on,
													--order_tracking.rvp_promise_date,
													order_tracking.rvp_tat,
													order_tracking.source_pincode,
													order_tracking.destination_pincode,
													oms.order_item_unit_id,
													oms.order_pincode_key,
													oms.cu_sub_issue_type,
													oms.incident_creation_time,
													oms.calculated_delivery_hub,
													oms.order_item_unit_quantity,
													oms.order_item_quantity,
													oms.order_item_selling_price,
													oms.order_item_type,
													oms.order_item_title,
													oms.order_item_unit_status,
													oms.order_item_unit_is_promised_date_updated,
													oms.order_item_unit_new_promised_date,
													oms.order_item_sub_type,
													oms.order_item_sku,
													oms.order_item_category_id,
													oms.order_item_sub_status,
													oms.order_item_ship_group_id,
													oms.order_item_status,
													oms.order_item_product_id_key,
													oms.order_item_unit_init_promised_date_key,
													oms.order_item_date_key,
													oms.order_item_unit_final_promised_date_key,
													oms.order_item_seller_id_key,
													oms.order_item_time_key,
													oms.order_item_unit_source_faciltiy AS order_item_unit_source_facility,
													oms.order_item_listing_id_key,
													oms.order_item_unit_final_promised_date_use,
													oms.order_item_unit_init_promised_datetime,
													oms.order_item_last_update,
													oms.order_item_date,
													oms.order_item_created_at,
													oms.order_shipping_address_id_key,
													oms.order_shipping_address_id,
													oms.call_verification_reason,
													oms.order_item_max_approved_time,
													oms.order_item_max_on_hold_time,
													oms.order_last_payment_method,
													oms.call_verification_type,
													oms.is_replacement,
													oms.is_exchange,
													oms.is_duplicate,
													oms.account_id,
													oms.account_id_key,
													cancel.cancellation_reason,
													cancel.cancellation_sub_reason,
													cancel_dim.cancellation_derived_reason,
													oms.customer_contact_flag,
													oms.issue_type AS customer_contact_issue_type,
													oms.DUMMY AS customer_contact_sub_sub_issue_type,
													prod_details.length * 2.5 as product_length,
													prod_details.breadth * 2.5 as product_breadth,
													prod_details.height * 2.5 as product_height,
													prod_details.shipping_weight as product_shipping_weight,
													ff.fulfill_item_unit_dispatch_actual_time,
													ff.slot_booking_ref_id,
													ff.slot_change_reason,
													ff.fulfill_item_unit_ship_actual_time,
													ff.fulfill_item_unit_dispatch_expected_time,
													ff.fulfill_item_unit_ship_expected_time,
													ff.fulfillment_order_item_id,
													ff.fulfill_item_unit_id,
													ff.fulfill_item_unit_status,
													ff.shipment_merchant_reference_id,
													ff.fulfill_item_unit_updated_at,
													ff.fulfill_item_unit_region,
													ff.fulfill_item_unit_dispatch_actual_date_key,
													ff.fulfill_item_unit_ship_actual_date_key,
													ff.fulfill_item_unit_dispatch_expected_date_key,
													ff.fulfill_item_unit_order_date_key,
													ff.fulfill_item_unit_dispatch_actual_time_key,
													ff.fulfill_item_unit_ship_actual_time_key,
													ff.fulfill_item_unit_dispatch_expected_time_key,
													ff.fulfill_item_unit_order_time_key,
													ff.fulfill_item_unit_region_type,
													ff.fulfill_item_id,
													ff.fulfill_item_type,
													ff.fulfill_item_unit_deliver_after_time,
													ff.fulfill_item_unit_dispatch_after_time,
													ff.fulfill_item_unit_deliver_after_date_key,
													ff.fulfill_item_unit_deliver_after_time_key,
													ff.fulfill_item_unit_dispatch_after_date_key,
													ff.fulfill_item_unit_dispatch_after_time_key,
													ff.fulfill_item_unit_is_for_slotted_delivery,
													ff.fulfill_item_unit_reserved_status_b2c_actual_time,
													ff.fulfill_item_unit_reserved_status_expected_time,
													ff.fulfill_item_unit_reserved_status_b2c_actual_date_key,
													ff.fulfill_item_unit_reserved_status_b2c_actual_time_key,
													ff.fulfill_item_unit_reserved_status_expected_date_key,
													ff.fulfill_item_unit_reserved_status_expected_time_key,
													ff.fulfill_item_unit_delivered_status_expected_time,
													ff.fulfill_item_unit_delivered_status_expected_date_key,
													ff.fulfill_item_unit_delivered_status_expected_time_key,
													ff.fulfill_item_unit_delivered_status_id,
													ff.fulfill_item_unit_size,
													ff.fulfill_item_unit_reserved_status_b2b_actual_date_key,
													ff.fulfill_item_unit_reserved_status_b2b_actual_time_key,
													ff.fulfill_item_unit_reserved_status_b2b_actual_time,
													pl.picklist_display_id,
													pl.picklist_created_timestamp as picklist_created_at,
													pl.picklist_item_picked_timestamp as picklist_item_confirmed_at,
													ff.slot_changed_created_at,
													ff.slot_changed_by,
													prod.product_categorization_hive_dim_key,
													prod.product_id,
													prod.analytic_super_category,
													prod.brand,
													prod.analytic_vertical,
													prod.analytic_category,
													prod.analytic_sub_category,
													sh.shipment_id,
													sh.shipment_last_consignment_eta_in_sec,
													sh.vendor_tracking_id,
													sh.shipment_current_status,
													sh.runsheet_close_datetime,
													sh.runsheet_close_date_key,
													sh.shipment_current_status_date_key,
													sh.shipment_current_status_time_key,
													sh.destination_geo_id_key,
													sh.ekl_shipment_type,
													sh.customer_promise_date_key,
													sh.customer_promise_time_key,
													sh.logistics_promise_date_key,
													sh.logistics_promise_time_key,
													sh.shipment_created_at_date_key,
													sh.shipment_created_at_time_key,
													sh.vendor_id_key,
													sh.shipment_carrier,
													sh.fsd_assigned_hub_id_key,
													sh.fsd_last_current_hub_id_key,
													sh.fsd_last_current_hub_type,
													sh.shipment_inscan_time,
													sh.shipment_rto_create_time,
													sh.fsd_last_received_time,
													sh.fsd_number_of_ofd_attempts,
													sh.fsd_assignedhub_expected_time,
													sh.fsd_assignedhub_received_time,
													sh.fsd_returnedtoekl_time,
													sh.fsd_receivedbyekl_time,
													sh.merchant_reference_id,
													sh.ekl_delivery_datetime,
													sh.ekl_delivery_date_key,
													sh.ekl_delivery_time_key,
													sh.shipment_first_ofd_datetime,
													sh.shipment_last_ofd_datetime,
													sh.vendor_dispatch_datetime,
													sh.billable_weight as ekl_billable_weight,
													sh.fsd_assigned_hub_sent_datetime,
													sh.shipment_first_consignment_id,
													sh.shipment_last_consignment_id,
													sh.openbox_reject_datetime,
													sh.merchant_id,
													sh.fsd_first_dh_received_datetime,
													sh.shipment_first_consignment_create_date_key,
													sh.shipment_first_consignment_create_time_key,
													sh.shipment_origin_facility_id_key,
													sh.fsd_firstundeliverystatus,
													sh.rvp_pickup_complete_datetime,
													sh.shipment_rvp_pk_number_of_attempts,
													sh.end_state_datetime,
													sh.fsd_last_dhhub_sent_datetime,
													sh.rvp_hub_id,
													sh.rvp_hub_id_key,
													sh.rvp_origin_geo_id_key,
													sh.rvp_destination_facility_key,
													sh.shipment_value,
													sh.cod_amount_to_collect,
													sh.shipment_created_at_datetime,
													sh.shipment_last_consignment_conn_id,
													sh.shipment_last_consignment_create_datetime,
													sh.ekl_facility_id,
													sh.last_tasklist_agent_id,
													sh.last_tasklist_agent_id_key,
													sh.last_tasklist_updated_at,
													sh.tasklist_type,
													sh.last_tasklist_id,
													sh.tasklist_status,
													sh.runsheet_id,
													sh.tasklist_tracking_id,
													sh.ekl_facility_id_key,
													sh.seller_id,
													sh.payment_mode,
													sh.pos_id,
													sh.transaction_id,
													sh.agent_id,
													sh.amount_collected,
													sh.seller_type,
													sh.shipment_dg_flag,
													sh.shipment_fragile_flag,
													sh.seller_id_key,
													sh.pos_id_key,
													sh.shipment_agent_id_key,
													sh.pincode_location_type AS pincode_location_type,
													sh.is_cpu_vendor AS is_cpu_vendor,
													sh.lzn_classification AS lzn_classification,
													sh.lzn_tat_target As lzn_tat_target,
													sh.vendor_name,
													sh.rvp_complete_date_key,
													sh.is_synergy_hub,
													sh.rvp_complete_datetime,
													sh.reverse_shipment_type,
													rrr_return.exchange_fsn,
													rrr_return.replacement_order_item_id,
													rrr_return.tracking_id,
													rrr_return.return_reason,
													qc_wms.qc_reason,
													rrr_return.return_derived_reason,
													rrr_return.return_id,
													rrr_return.return_sub_reason,
													rrr_return.return_comments,
													rrr_return.return_type,
													rrr_return.return_date,
													tpt_intransit_breach_flag,
													MH_breach.mh_breach_flag,
													MH_breach.mh_promise_datetime,
													TPT.shipment_tpt_dh_eta_datetime,
													TPT.shipment_tpt_dh_eta_date_key,
													TPT.tpt_compliance_flag,
													TPT.actual_cons_created_datetime,
													TPT.conn_eta_in_sec,
													last_conn.last_conn_cutoff_in_sec,
													IF(
														isnull(ff.fulfill_item_unit_reserved_status_b2b_actual_time),
														IF(
															isnull(ff.fulfill_item_unit_reserved_status_b2c_actual_time),
															NULL,
															ff.fulfill_item_unit_reserved_status_b2c_actual_time
														),
														IF(
															isnull(ff.fulfill_item_unit_reserved_status_b2c_actual_time),
															ff.fulfill_item_unit_reserved_status_b2b_actual_time,
															IF(
																ff.fulfill_item_unit_reserved_status_b2b_actual_time < ff.fulfill_item_unit_reserved_status_b2c_actual_time,
																ff.fulfill_item_unit_reserved_status_b2b_actual_time,
																ff.fulfill_item_unit_reserved_status_b2c_actual_time
															)
														)
													) AS fulfill_item_unit_final_reserved_datetime
												FROM
													(
														SELECT
															DISTINCT subset_1.order_external_id,
															subset_1.order_id,
															subset_1.order_item_unit_id AS order_item_unit_id,
															subset_1.order_item_id,
															subset_1.order_item_unit_tracking_id,
															subset_1.order_item_unit_shipment_id,
															subset_1.tracking_id_type,
															subset_1.product_categorization_hive_dim_key,
															--NULL as created_on,
															--NULL as rvp_promise_date,
															NULL as rvp_tat,
															NULL as source_pincode,
															NULL as destination_pincode	
														FROM
															(
																SELECT
																	prod_1.product_categorization_hive_dim_key,
																	oiu_1.order_external_id AS order_external_id,
																	oiu_1.order_item_unit_id AS order_item_unit_id,
																	oiu_1.order_id AS order_id,
																	oiu_1.order_item_id AS order_item_id,
																	oiu_1.order_item_unit_tracking_id AS order_item_unit_tracking_id,
																	oiu_1.order_item_unit_shipment_id AS order_item_unit_shipment_id,
																	oiu_1.tracking_id_type
																FROM
																	(
																		SELECT
																			is_large,
																			product_categorization_hive_dim_key
																		FROM
																			bigfoot_external_neo.sp_product__product_categorization_hive_dim
																		WHERE
																			is_large = 1
																	) prod_1
																LEFT OUTER JOIN(
																		SELECT
																			DISTINCT order_item_id,
																			order_external_id,
																			order_id,
																			order_item_unit_id,
																			order_item_unit_tracking_id,
																			order_item_unit_shipment_id,
																			order_item_product_id_key,
																			'forward' AS tracking_id_type
																		FROM
																			bigfoot_external_neo.scp_oms__la_oms_fact
																		WHERE
																			order_item_unit_status NOT IN('created')
																	) oiu_1 ON
																	prod_1.product_categorization_hive_dim_key = oiu_1.order_item_product_id_key
															) subset_1
													UNION ALL Select
															DISTINCT oms_fct.order_external_id,
															oms_fct.order_id,
															000 as order_item_unit_id,
															return_item. `data` . order_item_id_string AS order_item_id,
															return_item. `data` . tracking_id AS order_item_unit_tracking_id,
															return_item. `data` . shipment_id AS order_item_unit_shipment_id,
															'reverse' AS tracking_id_type,
															oms_fct.order_item_product_id_key AS product_categorization_hive_dim_key,
															--to_date(date_add(cast(return_item. `data` . created_at as string),rvp_tat.target)) as created_on,
															--to_date(date_add(cast(return_item. `data` . created_at as string),rvp_tat.target)) as --rvp_promise_date,
															rvp_tat.target as rvp_tat,
															rvp_tat.source_pincode,
															rvp_tat.destination_pincode												
														FROM
															bigfoot_snapshot.dart_fkint_scp_rrr_return_4_view_total RETURN
														LEFT OUTER JOIN bigfoot_snapshot.dart_fkint_scp_rrr_return_item_3_0_view_total return_item ON
															RETURN. `data` . return_id_string = return_item. `data` . return_id_string
														JOIN bigfoot_external_neo.scp_oms__la_oms_fact oms_fct ON
															oms_fct.order_item_id = return_item. `data` . order_item_id_string
														JOIN bigfoot_external_neo.sp_product__product_categorization_hive_dim prod_dim ON
															oms_fct.order_item_product_id_key = prod_dim.product_categorization_hive_dim_key
														LEFT OUTER JOIN bigfoot_common.la_lzn_classification rvp_tat
														ON rvp_tat.source_pincode = oms_fct.address_pincode
														AND rvp_tat.shipment_carrier = '3PL'
														AND UPPER(rvp_tat.tat) = 'REVERSE'	
														WHERE
															prod_dim.is_large = 1
															AND RETURN. `data` . return_type = 'customer_return'
															AND return_item. `data` . tracking_id IS NOT NULL
													UNION ALL SELECT
															DISTINCT subset_3.order_external_id,
															subset_3.order_id,
															111 AS order_item_unit_id,
															subset_3.order_item_id,
															subset_3.order_item_unit_tracking_id,
															subset_3.order_item_unit_shipment_id,
															subset_3.tracking_id_type,
															subset_3.product_categorization_hive_dim_key,
															--NULL as created_on,
															--NULL as rvp_promise_date,
															NULL as rvp_tat,
															NULL as source_pincode,
															NULL as destination_pincode	
														FROM
															(
																SELECT
																	prexo_ex.product_categorization_hive_dim_key AS product_categorization_hive_dim_key,
																	prexo_p.tracking_id AS order_item_unit_tracking_id,
																	prexo_p.shipment_id AS order_item_unit_shipment_id,
																	prexo_ex.order_item_id AS order_item_id,
																	'prexo' AS tracking_id_type,
																	prexo_o.order_id AS order_id,
																	prexo_o.order_external_id AS order_external_id
																FROM
																	(
																		SELECT
																			`data` . pickup_id,
																			`data` . shipment_id,
																			`data` . tracking_id,
																			`data` . latched_shipment_id
																		FROM
																			bigfoot_snapshot.dart_fkint_scp_ro_pickup_1_view_total
																		WHERE
																			`data` . pickup_to_address_id IN(
																				'blr_jig_01L',
																				'blr_jig_03L',
																				'del_lhr_03L',
																				'del_tik_01L',
																				'kol_dan_01L',
																				'kol_dki_02L',
																				'mum_bhi_01L',
																				'mum_bhi_02L',
																				'mum_chm_01L'
																			)
																	) prexo_p
																INNER JOIN(
																		SELECT
																			`data` . ref_entity_id,
																			`data` . ref_entity,
																			`data` . pickup_id
																		FROM
																			bigfoot_snapshot.dart_fkint_scp_ro_pickup_item_1_view_total
																		WHERE
																			`data` . ref_entity = 'product_exchange_item'
																	) prexo_pi ON
																	prexo_p.pickup_id = prexo_pi.pickup_id
																LEFT JOIN(
																		SELECT
																			`data` . order_item_id,
																			`data` . product_id,
																			`data` . product_exchange_item_id,
																			lookupkey(
																				'product_id',
																				`data` . product_id
																			) AS product_categorization_hive_dim_key
																		FROM
																			bigfoot_snapshot.dart_fkint_scp_rrr_product_exchange_item_1_view_total
																	) prexo_ex ON
																	prexo_pi.ref_entity_id = prexo_ex.product_exchange_item_id
																LEFT OUTER JOIN(
																		SELECT
																			order_external_id,
																			order_id,
																			order_item_unit_shipment_id AS unit_shipment_id
																		FROM
																			bigfoot_external_neo.scp_oms__la_oms_fact
																	) prexo_o ON
																	prexo_o.unit_shipment_id = prexo_p.latched_shipment_id
															) subset_3
														WHERE
															NOT ISNULL(subset_3.order_item_unit_tracking_id)
													) order_tracking
												LEFT OUTER JOIN bigfoot_external_neo.scp_oms__la_oms_fact oms ON
													order_tracking.order_item_unit_id = oms.order_item_unit_id
												LEFT OUTER JOIN bigfoot_external_neo.scp_fulfillment__la_fulfilment_fact ff ON
													(
														order_tracking.order_item_unit_shipment_id = ff.shipment_merchant_reference_id
													)
												LEFT OUTER JOIN(
														SELECT
															product_categorization_hive_dim_key,
															product_id,
															analytic_super_category,
															brand,
															analytic_vertical,
															analytic_category,
															analytic_sub_category
														FROM
															bigfoot_external_neo.sp_product__product_categorization_hive_dim
													) prod ON
													order_tracking.product_categorization_hive_dim_key = prod.product_categorization_hive_dim_key
												LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__la_shipment_l0_fact sh ON
													order_tracking.order_item_unit_shipment_id = sh.merchant_reference_id
												LEFT JOIN  bigfoot_external_neo.scp_ekl__la_product_details_fact prod_details
													ON prod_details.fsn = prod.product_id
													AND UPPER(prod_details.seller_flag) = 'FKI'
													AND UPPER(prod_details.seller_id) = 'FKI'
												LEFT OUTER JOIN(
														SELECT
															DISTINCT `data` . group_id AS last_group_id,
															`data` . cutoff AS last_conn_cutoff_in_sec
														FROM
															bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_view_total
													) last_conn ON
													last_conn.last_group_id = sh.shipment_last_consignment_conn_id
												LEFT OUTER JOIN(
														SELECT
															distinct vendor_tracking_id,
															tpt_intransit_breach_flag,
															shipment_tpt_dh_eta_datetime,
															shipment_tpt_dh_eta_date_key,
															tpt_compliance_flag,
															actual_cons_created_datetime,
															conn_eta_in_sec
														FROM
															bigfoot_external_neo.scp_ekl__la_tpt_fact
													) TPT ON
													sh.vendor_tracking_id = TPT.vendor_tracking_id
												LEFT OUTER JOIN(
														SELECT
															vendor_tracking_id,
															max(MH_breach_flag) AS mh_breach_flag,
															max(MH_promise_dateTime) AS mh_promise_datetime
														FROM
															bigfoot_external_neo.scp_ekl__mh_breach_large_fact
														GROUP BY
															vendor_tracking_id
													) MH_breach ON
													sh.vendor_tracking_id = MH_breach.vendor_tracking_id
												LEFT OUTER JOIN(
														SELECT
															DISTINCT `data` . cancellation_reason AS cancellation_reason,
															`data` . cancellation_sub_reason AS cancellation_sub_reason,
															cancel_order_item_id
														FROM
															bigfoot_snapshot.dart_fkint_scp_oms_cancellation_3_view_total LATERAL VIEW explode(
																`data` . cancellation_items.cancellation_item_order_item_id
															) cancel_order_item_id AS cancel_order_item_id
													) CANCEL ON
													order_tracking.order_item_id = cancel.cancel_order_item_id
												LEFT OUTER JOIN bigfoot_external_neo.scp_fulfillment__cancellation_reason_hive_dim cancel_Dim ON
													(
														cancel.cancellation_reason = cancel_Dim.cancellation_reason
														AND cancel.cancellation_sub_reason = cancel_Dim.cancellation_sub_reason
													)
												LEFT OUTER JOIN(
														SELECT
															tracking_id,
															return_reason,
															return_derived_reason,
															return_id,
															return_sub_reason,
															return_comments,
															return_type,
															return_date,
															exchange_fsn,
															replacement_order_item_id,
															order_item_id AS r_order_item_id,
															return_shipment_id
														FROM
															(
																SELECT
																	return_shipment_id,
																	(
																		LAST_VALUE(return_reason) OVER(
																			PARTITION BY return_shipment_id
																		ORDER BY
																			a.updateat ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
																		)
																	) AS return_reason,
																	(
																		LAST_VALUE(return_derived_reason) OVER(
																			PARTITION BY return_shipment_id
																		ORDER BY
																			a.updateat ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
																		)
																	) AS return_derived_reason,
																	(
																		LAST_VALUE(return_id) OVER(
																			PARTITION BY return_shipment_id
																		ORDER BY
																			a.updateat ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
																		)
																	) AS return_id,
																	(
																		LAST_VALUE(return_sub_reason) OVER(
																			PARTITION BY return_shipment_id
																		ORDER BY
																			a.updateat ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
																		)
																	) AS return_sub_reason,
																	(
																		LAST_VALUE(return_comments) OVER(
																			PARTITION BY return_shipment_id
																		ORDER BY
																			a.updateat ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
																		)
																	) AS return_comments,
																	(
																		LAST_VALUE(order_item_id) OVER(
																			PARTITION BY return_shipment_id
																		ORDER BY
																			a.updateat ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
																		)
																	) AS order_item_id,
																	(
																		LAST_VALUE(return_type) OVER(
																			PARTITION BY return_shipment_id
																		ORDER BY
																			a.updateat ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
																		)
																	) AS return_type,
																	(
																		LAST_VALUE(return_date) OVER(
																			PARTITION BY return_shipment_id
																		ORDER BY
																			a.updateat ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
																		)
																	) AS return_date,
																	(
																		LAST_VALUE(exchange_fsn) OVER(
																			PARTITION BY return_shipment_id
																		ORDER BY
																			a.updateat ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
																		)
																	) AS exchange_fsn,
																	(
																		LAST_VALUE(replacement_order_item_id) OVER(
																			PARTITION BY return_shipment_id
																		ORDER BY
																			a.updateat ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
																		)
																	) AS replacement_order_item_id,
																	(
																		LAST_VALUE(tracking_id) OVER(
																			PARTITION BY return_shipment_id
																		ORDER BY
																			a.updateat ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
																		)
																	) AS tracking_id,
																	ROW_NUMBER() OVER(
																		PARTITION BY return_shipment_id
																	ORDER BY
																		a.updateat ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
																	) row_num
																FROM
																	(
																		SELECT
																			return_item. `data` . reason AS return_reason,
																			return_item. `data` . tracking_id,
																			return_item. `data` . exchange_fsn AS exchange_fsn,
																			return_item. `data` . replacement_order_item_id_string AS replacement_order_item_id,
																			return_item. `data` . shipment_id AS return_shipment_id,
																			return_item. `data` . sub_reason AS return_sub_reason,
																			RETURN. `data` . comments AS return_comments,
																			return_item. `data` . order_item_id_string AS order_item_id,
																			return_item. `data` . return_id_string AS return_id,
																			RETURN. `data` . return_type AS return_type,
																			RETURN. `data` . return_date AS return_date,
																			return_item. `updatedat` AS updateat,
																			return_reason_dim.courier_return_item_derived_reason AS return_derived_reason
																		FROM
																			bigfoot_snapshot.dart_fkint_scp_rrr_return_item_3_view_total return_item
																		LEFT OUTER JOIN bigfoot_snapshot.dart_fkint_scp_rrr_return_4_view_total RETURN ON
																			return_item. `data` . return_id_string = RETURN. `data` . return_id_string
																		LEFT OUTER JOIN bigfoot_common.courier_return_reason_dim_alter return_reason_dim ON
																			return_item. `data` . reason = return_reason_dim.courier_return_item_reason
																		WHERE
																			return_item. `data` . return_item_status != 'cancelled'
																	) a
															) RETURN
														WHERE
															row_num = 1
													) rrr_return ON
													order_tracking.order_item_id = rrr_return.r_order_item_id
												LEFT OUTER JOIN(
														SELECT
															max( qc.qc_verification_reason ) AS qc_reason,
															shi.shipment_display_id AS sh_merchant_reference_id
														FROM
															bigfoot_external_neo.scp_warehouse__fc_shipment_item_l0_hive_fact shi
														LEFT JOIN bigfoot_external_neo.scp_warehouse__fc_qc_ticket_item_l0_hive_fact qc ON
															shi.shipment_item_id = qc.shipment_item_id
															AND shi.warehouse_company = qc.warehouse_company
														WHERE
															shipment_entity_type = 'shipment'
															AND shipment_is_large_warehouse = 1
														GROUP BY
															shi.shipment_display_id
													) qc_wms ON
													sh.merchant_reference_id = qc_wms.sh_merchant_reference_id
												LEFT OUTER JOIN bigfoot_external_neo.scp_warehouse__fc_reservation_l0_hive_fact pl ON
													SUBSTR(
														pl.reservation_fulfill_reference_id,
														5
													) = ff.fulfill_item_unit_id
													AND pl.reservation_inv_source_type in(
														'fki',
														'wsr',
														'fbf'
													)
													and pl.reservation_outbound_type = 'customer_reservation'
											) A
											
									) B
							) C
					) DA
			) E;
