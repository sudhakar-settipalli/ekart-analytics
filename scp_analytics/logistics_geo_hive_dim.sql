INSERT OVERWRITE TABLE logistics_geo_hive_dim
SELECT lookupkey('pincode',`data`.pincode) AS logistics_geo_dim_key,
       `data`.latitude AS latitude,
       `data`.longitude AS longitude,
       `data`.address_tag AS address_tag,
       `data`.area_code AS area_code,
       `data`.pincode AS pincode,
       `data`.local_territory AS local_territory,
       `data`.city AS city,
       `data`.district AS district,
       `data`.region AS region,
       `data`.state AS STATE,
       `data`.zone AS ZONE,
       `data`.country AS country,
       `data`.city_type AS city_type,
       `data`.location_type AS location_type,
       "ekl" AS geo_source,
       Mapping.city_tier as city_tier,
	if(upper(`data`.city) in ('AHMEDABAD','PUNE'),'METRO', Mapping.city_tier)  as metro_flag
FROM bigfoot_snapshot.dart_fki_scp_ekl_geo_1_1_view_total Geo
Left Outer Join bigfoot_common.city_tier_mapping Mapping ON Geo.`data`.city = Mapping.city;
