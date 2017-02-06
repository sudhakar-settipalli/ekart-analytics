INSERT OVERWRITE TABLE customer_dependency_90_fact
SELECT
`data`.vendor_tracking_id,
'Y' AS customer_dependency_flag,
'1' as profiler_flag,
concat_ws('-',collect_set(`data`.status)) as statuses
FROM
bigfoot_journal.dart_wsr_scp_ekl_shipment_4
WHERE
lower(`data`.status) IN
('undelivered_customer_not_available',
'undelivered_door_lock',
'undelivered_cod_not_ready',
'undelivered_order_rejected_by_customer',
'undelivered_no_response',
'undelivered_incomplete_address',
'undelivered_attempted',
'undelivered_address_not_found',
'undelivered_order_rejected_opendelivery',
'undelivered_request_for_reschedule',
'undelivered_corresponding_pickup_rejected'
)
and day  > date_format(date_sub(current_date,90),'yyyyMMdd')
group by 
`data`.vendor_tracking_id,
'Y',
'1';
