Insert Overwrite table fulfillment_cartman_weight_fact
select
maintab.createdat as ship_createdat,
maintab.merchantreferenceid as merchantreferenceid,
maintab.sellerid as sellerid,
maintab.packagename as packagename, 
maintab.packagetype as packagetype, 
maintab.vendortrackingid as vendortrackingid,
maintab.worth as worth,
maintab.category as category, 
maintab.fsn as fsn, 
maintab.isdangerous as isdangerous, 
maintab.isfragile as isfragile, 
maintab.quantity as quantity,
maintab.countfsn as no_of_items,
weigh_decision.`data`.breadth as wdecision_breadth,
weigh_decision.`data`.confidence as wdecision_confidence,
weigh_decision.`data`.createdAt as wdecision_createdat,
weigh_decision.`data`.deadWeightRuleName as wdecision_deadweight_rulename,
weigh_decision.`data`.height as wdecision_height, 
weigh_decision.`data`.length as wdecision_length, 
weigh_decision.`data`.merchantReferenceId as wdecision_merchantreferenceid,
weigh_decision.`data`.selectedDeadWeightSource as wdecision_selecteddeadweightsource,
weigh_decision.`data`.selectedVolumeSource as wdecision_selectedvolumesource, 
weigh_decision.`data`.updatedAt as wdecision_updatedat, 
weigh_decision.`data`.vendorTrackingId as wdecision_vendortrackingid, 
weigh_decision.`data`.volumeRuleName as wdecision_volumerulename, 
weigh_decision.`data`.weight as wdecision_weight,
concat(maintab.merchantreferenceid,'-',maintab.fsn) as p_key
from
(select
`data`.createdAt as createdat,
`data`.merchantReferenceId as merchantreferenceid,
`data`.sellerId as sellerid,
`data`.shipmentPackage.packageName as packagename, 
`data`.shipmentPackage.packageType as packagetype, 
`data`.vendorTrackingId as vendortrackingid,
`data`.worth as worth,
max(si.category) as category, 
max(si.fsn) as fsn, 
max(si.isDangerous) as isdangerous, 
max(si.isFragile) as isfragile, 
sum(si.quantity) as quantity,
count(si.fsn) as countfsn,
count(distinct si.fsn) as countfsn2
from bigfoot_snapshot.dart_fkint_scp_ekl_cartmanshipmententity_1_0_view ship_ent
LATERAL VIEW explode(`data`.shipmentItems) exploded_table_si as si
where  bigint(from_unixtime(bigint(substr(updatedat,1,10)),'yyyyMMdd'))>=20160101
group by `data`.createdAt,
`data`.merchantReferenceId,
`data`.sellerId,
`data`.shipmentPackage.packageName, 
`data`.shipmentPackage.packageType, 
`data`.vendorTrackingId,
`data`.worth) maintab
left join bigfoot_snapshot.dart_fkint_scp_ekl_cartmanweightdecisionentity_1_0_view weigh_decision on
maintab.merchantreferenceid<=>weigh_decision.`data`.merchantReferenceId and maintab.vendortrackingid<=>weigh_decision.`data`.vendortrackingid;
