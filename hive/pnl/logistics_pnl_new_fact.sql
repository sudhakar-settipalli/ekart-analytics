INSERT overwrite TABLE logistics_pnl_new_fact 
--added columns 
--lzn 
--shipment_received_at_origin_facility_date_key 
--shipment_received_at_origin_facility_time_key 
--priority_shipment_incremental_revenue 
--handover_revenue_3pl 
SELECT DISTINCT s.vendor_tracking_id, 
                s.merchant_id, 
                s.seller_id, 
                s.shipment_current_status, 
                s.payment_type, 
                s.seller_type AS seller_type, 
                s.shipment_dg_flag, 
                s.shipment_fragile_flag, 
                s.shipment_priority_flag, 
                s.shipment_size_flag, 
                s.ekl_shipment_type, 
                s.ekl_fin_zone, 
                --newly added  col ( 
                r.lzn, 
                --newly added ) 
                s.shipment_fa_flag, 
                s.shipment_carrier, 
                s.shipment_pending_flag, 
                s.shipment_weight, 
                r.billable_weight, 
                r.weight_bucket, 
                s.cost_of_breach, 
                s.shipment_value, 
                s.cod_amount_to_collect, 
                s.shipment_charge, 
                lookupkey('pincode',s.source_address_pincode)      AS source_pincode_key, 
                lookupkey('pincode',s.destination_address_pincode) AS destination_pincode_key,
                lookup_date(s.shipment_created_at_datetime)        AS shipment_created_at_date_key,
                lookup_time(s.shipment_created_at_datetime)        AS shipment_created_at_time_key,
                lookup_date(s.shipment_dispatch_datetime)          AS shipment_dispatch_date_key,
                lookup_time(s.shipment_dispatch_datetime)          AS shipment_dispatch_time_key,
                lookup_date(s.shipment_first_received_at_datetime) AS shipment_first_received_date_key,
                lookup_time(s.shipment_first_received_at_datetime) AS shipment_first_received_time_key,
                lookup_date(s.shipment_delivered_at_datetime)      AS shipment_delivered_at_date_key,
                lookup_time(s.shipment_delivered_at_datetime)      AS shipment_delivered_at_time_key,
                --newly added  col ( 
                lookup_date(s.received_at_origin_facility_datetime) AS shipment_received_at_origin_facility_date_key,
                lookup_time(s.received_at_origin_facility_datetime) AS shipment_received_at_origin_facility_time_key,
                --newly added ) 
                lookup_date(s.customer_promise_datetime)                    AS customer_promise_date_key,
                lookup_time(s.customer_promise_datetime)                    AS customer_promise_time_key,
                lookup_date(s.logistics_promise_datetime)                   AS logistics_promise_date_key,
                lookup_time(s.logistics_promise_datetime)                   AS logistics_promise_time_key,
                lookup_date(s.vendor_dispatch_datetime)                     AS ekl_vendor_dispatch_date_key,
                lookup_time(s.vendor_dispatch_datetime)                     AS ekl_vendor_dispatch_time_key,
                lookup_date(s.rto_create_datetime)                          AS rto_create_date_key,
                lookup_time(s.rto_create_datetime)                          AS rto_create_time_key,
                lookup_date(s.rto_complete_datetime)                        AS rto_complete_date_key,
                lookup_time(s.rto_complete_datetime)                        AS rto_complete_time_key,
                lookupkey('facility_id',s.shipment_origin_facility_id)      AS shipment_origin_facility_id_key,
                lookupkey('facility_id',s.shipment_origin_mh_facility_id)   AS shipment_origin_mh_facility_id_key,
                lookupkey('facility_id',s.shipment_destination_facility_id) AS shipment_destination_facility_id_key,
                lookupkey('facility_id',s.fsd_assigned_hub_id)              AS fsd_assigned_hub_id_key,
                r.delivery_revenue, 
                r.forward_revenue, 
                r.rto_revenue, 
                r.cod_revenue, 
                r.priority_shipment_revenue, 
                --newly added  col ( 
                r.priority_shipment_incremental_revenue, 
                --newly added ) 
                r.rvp_revenue, 
                r.first_mile_revenue, 
                (r.cod_revenue    +r.priority_shipment_revenue)                                                                    AS vas_revenue,
                (r.forward_revenue+r.rto_revenue+ r.priority_shipment_revenue+ r.cod_revenue+ r.rvp_revenue+ r.first_mile_revenue) AS shipment_revenue,
                --newly added  col ( 
                r.handover_revenue_3pl, 
                --newly added ) 
                o.octroi_cost AS octroi_cost, 
                o.octroi_cost AS logistics_taxes, 
                r.sla_bucket, 
                d.offroll_salary      AS last_mile_offroll_salary, 
                d.pan_india_other     AS last_mile_others, 
                d.delivery_conveyance AS last_mile_delivery_conveyance, 
                d.rvp_conveyance      AS last_mile_rvp_conveyance, 
                d.mp_conveyance       AS last_mile_mp_conveyance, 
                d.area_manager_sal    AS last_mile_manager_sal, 
                d.dh_security         AS last_mile_security, 
                d.van                 AS last_mile_van_cost, 
                --newly added  col ( 
                r.pos_revenue, 
                r.asp_bucket, 
                r.risk_surcharge, 
                r.rto_revenue_total, 
                r.first_mile_revenue_flag, 
                s.payment_mode, 
                lookup_date(IF(Lower(s.ekl_shipment_type) LIKE "%rto%", s.rto_create_datetime, If(s.shipment_fa_flag = 0 AND             s.received_at_origin_facility_datetime IS NOT NULL, s.received_at_origin_facility_datetime, s.shipment_first_received_at_datetime ) ) ) AS revenue_month_date_key,
                IF(service.facility_id IS NOT NULL,'shared','independent') AS vendor_service_type,
                r.air_flag, 
                r.reverse_shipment_type, 
                r.ekart_lzn_flag, 
                r.return_processing_charges, 
                r.replacement_discount, 
                r.rto_creation_place, 
                f.cms_weight_bucket, 
                f.cms_vertical, 
                --newly added ) 
				r.shipment_dead_weight,
		        r.shipment_dead_weight_bucket,
				r.profiled_volumetric_weight,
				r.profiled_volumetric_weight_bucket,
				r.seller_dead_weight,
				r.seller_dead_weight_bucket,
				r.seller_volumetric_weight,
				r.seller_volumetric_weight_bucket,
				r.non_fa_volumetric_estimate,
				r.non_fa_volumetric_estimate_source,
				r.profiled_billable_weight,
		        r.profiled_billable_weight_bucket,
				r.profiled_dead_weight_at_source,
		        r.profiled_volumetric_weight_at_source,
				if(r.shipment_dead_weight_bucket = r.seller_dead_weight_bucket,1,0) as seller_vs_profiled_dead_wt_flg,
				if(r.profiled_volumetric_weight_bucket = r.seller_volumetric_weight_bucket,1,0) as seller_vs_profiled_vol_wt_flg,
				r.prof2seller_dead_wt_bucket_jump,
				r.prof2seller_vol_wt_bucket_jump,
				r.max_profiled_weight_bucket,
				r.max_seller_weight_bucket,
				s.shipping_category,
				lookupkey('product_id',r.product_detail_fsn) as first_product_key,
		        r.median_fsn_vol_wt as fa_suggested_boc_vol_wt,
				r.order_id,
		        r.order_external_id,
                (CASE 
                      WHEN UPPER(s.seller_id) IN ('BD91DF39671142CA', 'F6D15B4A4F304426', 'FIPL', '89512FCA732441C5', 'F1D25968ECC94909', 'C82E1FB314F34969', 'WSR', 'D591418B408940A0') THEN 'Alpha' 
                   ELSE 'Non-Alpha' END) AS seller_party_type
FROM            bigfoot_external_neo.scp_ekl__shipment_l1_90_fact s 
LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__ekl_logistics_profiler_revenue_new_hive_fact r 
ON              s.vendor_tracking_id = r.vendor_tracking_id 
LEFT OUTER JOIN 
                ( 
                          SELECT    fc_item.box_tracking_id, 
                                    max(fc_item.product_cms_vertical)                                              AS cms_vertical,
                                    floor(sum(cms_est.median_cms_vol_wt * fc_item.shipment_item_quantity)/500)*500 AS cms_weight_bucket
                          FROM      bigfoot_external_neo.scp_warehouse__fc_shipment_item_l0_hive_fact fc_item
                          LEFT JOIN bigfoot_external_neo.scp_warehouse__fc_cms_vertical_estimate_l1_hive_fact cms_est
                          ON        fc_item.product_cms_vertical = cms_est.product_cms_vertical
                          GROUP BY  fc_item.box_tracking_id ) f 
ON              s.vendor_tracking_id = f.box_tracking_id 
LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__octroi_cost_fact as o 
ON              ( s.vendor_tracking_id = o.tracking_id) 
LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__dh_cost_hive_fact as d 
ON              ( s.vendor_tracking_id = d.vendor_tracking_id) 
LEFT OUTER JOIN bigfoot_external_neo.scp_ekl__shipment_facility_id_pincode_map_l1_fact service
ON              ( s.destination_address_pincode = service.pincode 
                AND             service.hub_type <> 'BULK_HUB' 
                AND             IF(s.shipment_size_flag = 'bulk','BULK_HUB','DELIVERY_HUB') = service.hub_type)
WHERE           s.vendor_tracking_id <> 'not_assigned' ;
