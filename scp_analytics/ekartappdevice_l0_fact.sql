INSERT OVERWRITE TABLE ekartappdevice_l0_fact
Select
`data`.agentid as agentid,
`data`.fhrId as fhrid,
`data`.hubId as hubid,
`data`.imei1 as imei1,
`data`.imei2 as imei2,
`data`.createdat as createdat,
`data`.updatedat as updatedat,
`data`.source as source,
`data`.deviceModel as devicemodel,
`data`.appversion as appversion,
`data`.ossdk as ossdk,
`data`.osversion as osversion,
`data`.deviceid as deviceid
from bigfoot_journal.dart_wsr_scp_ekl_ekartappdevice_1
where day >#100#DAY#;
