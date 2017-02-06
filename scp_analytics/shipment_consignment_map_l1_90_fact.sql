INSERT OVERWRITE TABLE shipment_consignment_map_l1_90_fact
SELECT entityid AS consignment_id,
       exp AS shipment_id,
       min(updatedat) AS first_time
FROM bigfoot_journal.dart_wsr_scp_ekl_shipmentgroup_3 LATERAL VIEW explode(`data`.shipments) exploded_table AS exp
WHERE `data`.type = 'consignment' and  day > date_format(date_sub(current_date,100),'yyyyMMdd') and lower(`data`.status) <> 'created'
GROUP BY entityid,
         exp;

