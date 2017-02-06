INSERT OVERWRITE TABLE misroute_90_fact
SELECT DISTINCT
Y.vendor_tracking_id,
'Y' AS misroute_flag,
profiler_flag
FROM
(SELECT
`data`.vendor_tracking_id,
`data`.`shipment_weight`.updated_by as profiler_flag
FROM
bigfoot_journal.dart_wsr_scp_ekl_shipment_4
WHERE
lower(`data`.status) IN ('error') and day  > date_format(date_sub(current_date,90),'yyyyMMdd')

UNION ALL
SELECT
vendor_tracking_id,
profiler_flag
FROM
(SELECT 
`data`.vendor_tracking_id,
exp.flag,
`data`.`shipment_weight`.updated_by as profiler_flag
FROM 
bigfoot_journal.dart_wsr_scp_ekl_shipment_4 lateral view explode(`data`.notes) exploded_table as exp
where  day  > date_format(date_sub(current_date,90),'yyyyMMdd')
) X
WHERE
(lower(X.flag) LIKE '%incode%ismatch%' OR lower(X.flag) LIKE '%misroute%')) Y;
