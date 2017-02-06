INSERT OVERWRITE TABLE customer_misroute_90_fact
SELECT
DISTINCT
vendor_tracking_id,
'Y' as customer_misroute_flag,
profiler_flag
FROM
(SELECT 
`data`.vendor_tracking_id,
exp.flag,
`data`.`shipment_weight`.updated_by as profiler_flag
FROM 
bigfoot_journal.dart_wsr_scp_ekl_shipment_4 lateral view explode(`data`.notes) exploded_table as exp
where day  > date_format(date_sub(current_date,90),'yyyyMMdd')) X
WHERE
X.flag LIKE '%incode%ismatch%';
