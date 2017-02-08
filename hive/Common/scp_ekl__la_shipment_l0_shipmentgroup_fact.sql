INSERT overwrite TABLE la_shipment_l0_shipmentgroup_fact
SELECT C.shipment_first_consignment_id AS shipment_first_consignment_id,
       C.shipment_last_consignment_id AS shipment_last_consignment_id,
       lookup_date(to_date(from_unixtime(shipment_first_consignment_create_datetime))) AS shipment_first_consignment_create_date_key,
       lookup_time(from_unixtime(shipment_first_consignment_create_datetime)) AS shipment_first_consignment_create_time_key,
       lookup_date(to_date(from_unixtime(shipment_last_consignment_create_datetime))) AS shipment_last_consignment_create_date_key,
       C.tracking_id AS vendor_tracking_id,
       from_unixtime(shipment_last_consignment_create_datetime) AS shipment_last_consignment_create_datetime,
       shipment_last_consignment_eta_in_sec,
	   shipment_last_consignment_eta_datetime,
       shipment_last_consignment_conn_id,
       from_unixtime(shipment_first_consignment_create_datetime) AS shipment_first_consignment_create_datetime
	FROM
    ( SELECT B.shipment_first_consignment_id,
            B.shipment_first_consignment_create_datetime,
            B.row_n,
            B.shipment_last_consignment_id,
            B.shipment_last_consignment_create_datetime,
            B.shipment_last_consignment_eta_in_sec,
			B.shipment_last_consignment_eta_datetime,
            B.shipment_last_consignment_conn_id,
            B.l_row_n,
            B.tracking_id
    FROM
      ( SELECT first_value(A.consignment_id) OVER ( PARTITION BY A.tracking_id
                                                   ORDER BY A.first_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS shipment_first_consignment_id,
               first_value(A.first_time) OVER ( PARTITION BY A.tracking_id
                                               ORDER BY A.first_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS shipment_first_consignment_create_datetime,
               row_number() OVER ( PARTITION BY A.tracking_id
                                  ORDER BY A.first_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS row_n,
               first_value(A.consignment_id) OVER ( PARTITION BY A.tracking_id
                                                   ORDER BY A.first_time DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS shipment_last_consignment_id,
               first_value(A.conn_id) OVER ( PARTITION BY A.tracking_id
                                            ORDER BY A.first_time DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS shipment_last_consignment_conn_id,
               first_value(A.first_time) OVER ( PARTITION BY A.tracking_id
                                               ORDER BY A.first_time DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS shipment_last_consignment_create_datetime,
               first_value(A.eta_in_sec) OVER ( PARTITION BY A.tracking_id
                                               ORDER BY A.first_time DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS shipment_last_consignment_eta_in_sec,
			   first_value(A.eta_datatime) OVER ( PARTITION BY A.tracking_id
											ORDER BY A.first_time DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS shipment_last_consignment_eta_datetime,
               row_number() OVER ( PARTITION BY A.tracking_id
                                  ORDER BY A.first_time DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS l_row_n,
               tracking_id
       FROM
         ( SELECT entityid,
cast(split(entityid, "-") [1] AS INT) AS consignment_id,
TRID AS tracking_id,
`data`.connection_id AS Conn_id,
min(`data`.connection_estimated_tat) AS eta_in_sec,
min(`data`.eta) as eta_datatime,
min(unix_timestamp(`data`.created_at)) AS first_time
FROM bigfoot_snapshot.dart_wsr_scp_ekl_shipmentgroup_3_view_total LATERAL VIEW explode(`data`.shipments) exploded_table AS TRID
WHERE `data`.type = 'consignment'
AND `data`.source_location.id IN ( 599,
600,
601,
602,
603,
1281,
1289,
1517,
1758,
1954,
2017,
3598,
4066,
4065,
4064,
4061,
4060,
465 )
GROUP BY entityid,
TRID,
`data`.connection_id
				   ) A ) B
    WHERE B.row_n = 1 ) C;
