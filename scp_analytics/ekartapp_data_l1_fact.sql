INSERT OVERWRITE TABLE ekartapp_data_l1_fact
Select
sync.shipment_id,
sync.sheetid,
sync.agentid,
device.fhrid,
device.hubid as facility_id,
sync.entitytype,
sync.eventtype,	
sync.type,
sync_source,
sync_createdat,
sync_uploadedat,
device.createdat as device_createdat,
device.imei1,
device.imei2,
device.source as device_source,
device.devicemodel,
device.appversion,
device.ossdk,
device.osversion,
device.deviceid,
sync.devicelatitude,
sync.devicelongitude,
sync.batterylevel,
sync.isrepeataddress
from (select shipment_id,
sheetid,
agentid,
entitytype,
eventtype,	
type,
source as sync_source,
createdat as sync_createdat,
uploadedat as sync_uploadedat,
devicelatitude,
devicelongitude,
batterylevel,
isrepeataddress,
row_number() over(partition by shipment_id,sheetid order by createdat asc ) rnk
from bigfoot_external_neo.scp_ekl__ekartappsync_l0_fact
where entitytype <> 'SIGNATURE')sync
left outer join(select device.agentid,
device.fhrid,
device.hubid,
device.imei1,
device.imei2,
device.createdat,
device.source,
device.devicemodel,
device.appversion,
device.ossdk,
device.osversion,
device.deviceid,
row_number() over (partition by agentid, lookup_date(device.createdat) order by device.createdat asc) rnk
from bigfoot_external_neo.scp_ekl__ekartappdevice_l0_fact device
where device.deviceid  is not null) device
on device.agentid=sync.agentid and lookup_date(sync.sync_createdat)=lookup_date(device.createdat)
where device.rnk=1 and sync.rnk=1;
