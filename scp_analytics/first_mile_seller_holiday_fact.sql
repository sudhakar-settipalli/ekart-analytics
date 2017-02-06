INSERT OVERWRITE TABLE first_mile_seller_holiday_fact
Select 
`data`.sellerid,
`data`.holidaytype,
`data`.isholiday,
`data`.calendardate as calendardate_time,
lookup_date(cast(`data`.calendardate as timestamp)) as calenderdate_key,
lookup_time(cast(`data`.calendardate as timestamp)) as calenderdate_time_key,
lookupkey('seller_hive_dim_key',`data`.sellerid) as seller_key

from
 
bigfoot_snapshot.dart_fkmp_sp_seller_sellerholiday_3_1_view_total A

where A.data.isholiday=true
