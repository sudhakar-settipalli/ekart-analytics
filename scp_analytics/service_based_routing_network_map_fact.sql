INSERT OVERWRITE TABLE  service_based_routing_network_map_fact
select 
distinct
source_id,
first_dest_id,
destination_id,
if(shiptype_mod ='NDD','PRIORITY',shiptype_mod) as ship_type
from 
  (select 
  source_id,
  first_dest_id,
  destination_id,
  shiptype_mod from 
    (select 
    srcfacilityid as source_id,
    transitfacilityid as first_dest_id,
    destfacilityid as destination_id,
    (case when shiptype ='NORMAL' then 
    array('REGULAR','ECONOMY')
    when shiptype = 'ALL' then 
      array('DG','NDD','REGULAR','ECONOMY')
    else 
    array(shiptype)
    end 
    ) as shiptype_mod_array
    from 
      (SELECT DISTINCT    a.`data`.source_facility.id AS srcfacilityid,
                          a.`data`.transit_facility.id AS transitfacilityid,
                          a.`data`.destination_facility.id AS destfacilityid,
                          a.`data`.service_types[0] as shiptype,
                          b.feature_facility_id
                   FROM bigfoot_snapshot.dart_wsr_scp_ekl_networkmap_2_0_view_total a 
                   left join 
                   (select distinct `data`.facility_id as feature_facility_id
                   from
                   bigfoot_journal.dart_wsr_scp_ekl_featureusageevent_1_0_view
                   where lower(eventid) like '%bag_forwarding_service%'
                   and `data`.status = 'ENABLED' 
                    ) b 
                   on a.`data`.source_facility.id = b.feature_facility_id
                   where a.`data`.status = true
                   and a.`data`.service_types[0] not in ('MP','RETURN')
                   and (!(a.`data`.service_types[0] ='ALL' and b.feature_facility_id is not null))
                  ) A

    )B  lateral view explode(shiptype_mod_array) sbr_service_types as shiptype_mod 
)C;

