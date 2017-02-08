INSERT OVERWRITE TABLE ekartappsync_l0_fact
Select
`data`.id as shipment_id,
`data`.entitytype,
`data`.eventtype,	
`data`.type,
`data`.source,
`data`.createdat,
`data`.uploadedat,
`data`.devicelatitude,
`data`.devicelongitude,
`data`.batterylevel,
`data`.sheetid,
`data`.agentid,
`data`.isrepeataddress
from bigfoot_journal.dart_wsr_scp_ekl_ekartappsync_1
where  day > #100#DAY# ;
