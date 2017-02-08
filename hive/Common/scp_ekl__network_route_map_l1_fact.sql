INSERT OVERWRITE TABLE network_route_map_l1_fact
SELECT DISTINCT CONCAT(L4.SrcId,'-',L4.DestId,'-',L4.ShipmentType) AS route_map_id,
                L4.SrcId AS source_id,
                L4.DestId AS destination_id,
                L4.ShipmentType AS shipment_type,
                L4.Via1 AS via1_id,
                L4.Via2 AS via2_id,
                L4.Via3 AS via3_id,
                L4.Via4 AS via4_id,
                if(L4.DestId=NM.TransitFacilityId,NULL,NM.TransitFacilityId) AS via5_id,
                CASE
                    WHEN if(L4.DestId=NM.TransitFacilityId,NULL,NM.TransitFacilityId) IS NOT NULL THEN 6
                    WHEN L4.DestId=NM.TransitFacilityId THEN 5
                    WHEN (L4.Via4 IS NULL
                          AND L4.Via3 IS NOT NULL) THEN 4
                    WHEN (L4.Via3 IS NULL
                          AND L4.Via2 IS NOT NULL) THEN 3
                    WHEN (L4.Via2 IS NULL
                          AND L4.Via1 IS NOT NULL) THEN 2
                    WHEN (L4.Via1 IS NULL
                          AND L4.RouteExists = 'Y') THEN 1
                    ELSE 0
                END AS number_of_hops,
                if(L4.via1 IS NOT NULL, L4.via1,IF(L4.RouteExists = 'Y',L4.DestId,NULL)) AS first_dest_id,
                if(L4.via2 IS NOT NULL, L4.via2,if(L4.via1 IS NULL,NULL,L4.DestId)) AS second_dest_id,
                if(L4.via3 IS NOT NULL, L4.via3,if(L4.via2 IS NULL,NULL,L4.DestId)) AS third_dest_id,
                if(L4.via4 IS NOT NULL, L4.via4,if(L4.via3 IS NULL,NULL,L4.DestId)) AS fourth_dest_id,
                if(L4.via4 IS NULL,'NULL',IF(L4.DestId=NM.DestFacilityId,L4.DestId,NM.DestFacilityId)) AS fifth_dest_id,
                if(L4.via4 IS NULL,'NULL',if(L4.DestId=NM.DestFacilityId,NULL,IF(NM.DestFacilityId IS NOT NULL,L4.DestId,NULL))) AS sixth_dest_id,
lookup_date(CAST(from_unixtime(unix_timestamp()) AS TIMESTAMP)) AS current_date_key
FROM
  (SELECT DISTINCT L3.SrcId,
                   L3.DestId,
                   L3.ShipmentType,
                   L3.Via1,
                   L3.Via2,
                   L3.Via3,
                   if(L3.DestId=NM.TransitFacilityId,NULL,NM.TransitFacilityId) AS Via4,
                   L3.RouteExists
   FROM
     (SELECT DISTINCT L2.SrcId,
                      L2.DestId,
                      L2.ShipmentType,
                      L2.Via1,
                      L2.Via2,
                      if(L2.DestId=NM.TransitFacilityId,NULL,NM.TransitFacilityId) AS Via3,
                      L2.RouteExists
      FROM
        (SELECT DISTINCT L1.SrcId,
                         L1.DestId,
                         L1.ShipmentType,
                         L1.Via1,
                         if(L1.DestId=NM.TransitFacilityId,NULL,NM.TransitFacilityId) AS Via2,
                         L1.RouteExists
         FROM
           (SELECT DISTINCT SD.SrcId,
                            SD.DestId,
                            SD.ShipmentType,
                            if(SD.DestId=NM.TransitFacilityId,NULL,NM.TransitFacilityId) AS Via1,
                            if(NM.TransitFacilityId IS NULL,'N','Y') AS RouteExists
            FROM
              (SELECT DISTINCT S.entityId AS SrcId,
                      D.entityId AS DestId,
                      ST.ship_type AS ShipmentType
               FROM bigfoot_snapshot.dart_wsr_scp_ekl_facility_0_11_view_total S
               FULL OUTER JOIN bigfoot_snapshot.dart_wsr_scp_ekl_facility_0_11_view_total D
               FULL OUTER JOIN 
               (select 'DG' as ship_type union select 'PRIORITY' AS ship_type union select 'REGULAR' as ship_type union select 'ECONOMY' as ship_type) ST
               WHERE S.`data`.type IN ('MOTHER_HUB','DELIVERY_HUB')
                 AND S.`data`.active_flag = true
                 AND D.`data`.type IN ('MOTHER_HUB','DELIVERY_HUB')
                 AND D.`data`.active_flag = true
                 AND S.entityId <> D.entityId) SD
            LEFT OUTER JOIN
              (SELECT shiptype AS ShipType,
                      srcfacilityid AS SrcFacilityId,
                      transitfacilityid AS TransitFacilityId,
                      destfacilityid AS DestFacilityId

               FROM bigfoot_external_neo.scp_ekl__network_map_intermediate_fact ) NM ON SD.SrcId = NM.SrcFacilityId
            AND SD.DestId = NM.DestFacilityId
            AND SD.ShipmentType = NM.ShipType) L1
         LEFT OUTER JOIN
           (SELECT shiptype AS ShipType,
                      srcfacilityid AS SrcFacilityId,
                      transitfacilityid AS TransitFacilityId,
                      destfacilityid AS DestFacilityId

               FROM bigfoot_external_neo.scp_ekl__network_map_intermediate_fact) NM ON L1.Via1 = NM.SrcFacilityId
         AND L1.DestId = NM.DestFacilityId
         AND L1.ShipmentType = NM.ShipType) L2
      LEFT OUTER JOIN
        (SELECT shiptype AS ShipType,
                      srcfacilityid AS SrcFacilityId,
                      transitfacilityid AS TransitFacilityId,
                      destfacilityid AS DestFacilityId

               FROM bigfoot_external_neo.scp_ekl__network_map_intermediate_fact) NM ON L2.Via2 = NM.SrcFacilityId
      AND L2.DestId = NM.DestFacilityId
      AND L2.ShipmentType = NM.ShipType) L3
   LEFT OUTER JOIN
     (SELECT shiptype AS ShipType,
                      srcfacilityid AS SrcFacilityId,
                      transitfacilityid AS TransitFacilityId,
                      destfacilityid AS DestFacilityId

               FROM bigfoot_external_neo.scp_ekl__network_map_intermediate_fact) NM ON L3.Via3 = NM.SrcFacilityId
   AND L3.DestId = NM.DestFacilityId
   AND L3.ShipmentType = NM.ShipType) L4
LEFT OUTER JOIN
  (SELECT shiptype AS ShipType,
                      srcfacilityid AS SrcFacilityId,
                      transitfacilityid AS TransitFacilityId,
                      destfacilityid AS DestFacilityId

               FROM bigfoot_external_neo.scp_ekl__network_map_intermediate_fact) NM ON L4.Via4 = NM.SrcFacilityId
AND L4.DestId = NM.DestFacilityId
AND L4.ShipmentType = NM.ShipType;
