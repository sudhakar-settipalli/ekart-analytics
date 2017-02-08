INSERT OVERWRITE TABLE ekl_notes_fact
select 
coalesce(A.vendor_tracking_id,B.vendor_tracking_id) as vendor_tracking_id,
concat_ws("-",collect_set(A.flag)) as hub_notes,
concat_ws("-",collect_set(B.flag)) as cs_notes
from 
(select 
`data`.vendor_tracking_id,
exp.flag as flag,
exp.type as type
from 
bigfoot_journal.dart_wsr_scp_ekl_shipment_4 LATERAL VIEW explode(`data`.notes) exploded_table as exp
where day > #120#DAY#
--concat_ws("",split(date_sub(to_date(from_unixtime(unix_timestamp())),90),"-"))
and exp.type = 'Hub Notes'
) A
full outer join
(select 
`data`.vendor_tracking_id,
exp.flag as flag,
exp.type as type
from 
bigfoot_journal.dart_wsr_scp_ekl_shipment_4 LATERAL VIEW explode(`data`.notes) exploded_table as exp
where day > #120#DAY#

--concat_ws("",split(date_sub(to_date(from_unixtime(unix_timestamp())),90),"-"))
and exp.type = 'CS Notes'
) B ON A.vendor_tracking_id=B.vendor_tracking_id AND (B.vendor_tracking_id<>'not_assigned')
group by coalesce(A.vendor_tracking_id,B.vendor_tracking_id);
INSERT OVERWRITE TABLE ekl_notes_fact
select 
coalesce(A.vendor_tracking_id,B.vendor_tracking_id) as vendor_tracking_id,
concat_ws("-",collect_set(A.flag)) as hub_notes,
concat_ws("-",collect_set(B.flag)) as cs_notes
from 
(select 
`data`.vendor_tracking_id,
exp.flag as flag,
exp.type as type
from 
bigfoot_journal.dart_wsr_scp_ekl_shipment_4 LATERAL VIEW explode(`data`.notes) exploded_table as exp
where day > #91#DAY#
--concat_ws("",split(date_sub(to_date(from_unixtime(unix_timestamp())),90),"-"))
and exp.type = 'Hub Notes'
) A
full outer join
(select 
`data`.vendor_tracking_id,
exp.flag as flag,
exp.type as type
from 
bigfoot_journal.dart_wsr_scp_ekl_shipment_4 LATERAL VIEW explode(`data`.notes) exploded_table as exp
where day > #91#DAY#

--concat_ws("",split(date_sub(to_date(from_unixtime(unix_timestamp())),90),"-"))
and exp.type = 'CS Notes'
) B ON A.vendor_tracking_id=B.vendor_tracking_id AND (B.vendor_tracking_id<>'not_assigned')
group by coalesce(A.vendor_tracking_id,B.vendor_tracking_id);
