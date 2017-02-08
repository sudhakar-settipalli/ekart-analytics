INSERT OVERWRITE TABLE  service_based_connections_fact
select 
connection_id,
asscon_connection_id,
source_id,
destination_id,
updated_at,
created_at,
state,
mode,
transit_type,
frequency_type,
cutoff,
asscon_flag,
service_type
from 
(select
all_con.connection_id as connection_id,
associated_con_map.asscon_connection_id as asscon_connection_id,
all_con.source_address_id as source_id,
if(all_con.ass_con_size <=0,all_con.destination_address_id,
associated_con.associated_destination_address_id) as destination_id,
all_con.updated_at as updated_at,
all_con.created_at as created_at,
all_con.state as state,
if(all_con.ass_con_size <=0,all_con.mode,
associated_con.associated_mode) as mode,
if(all_con.ass_con_size <=0,all_con.transit_type,
associated_con.associated_transit_type) as transit_type,
if(all_con.ass_con_size <=0,all_con.frequency_type,
associated_con.associated_frequency_type) as frequency_type,
if(all_con.ass_con_size <=0,all_con.service_types,
associated_con.associated_service_types) as service_types,
all_con.cutoff as cutoff,
if(all_con.ass_con_size <=0,0,
1) as asscon_flag
from 
(   select 
    `data`.coloader as coloader,
    `data`.code as code,
    `data`.created_at as created_at,
    `data`.type as type,
    `data`.relative_reaching_date_time as reaching_date_time,
    `data`.operator as operator,
    `data`. mode as mode,
    `data`. updated_at as updated_at,
    `data`. source_address.pincode as source_pincode,
    `data`. source_address.id as source_address_id,
    `data`. source_address.type as source_address_type,
    `data`.external_tat as external_tat,
    `data`.state as state,
    `data`.vehicle_id as vehicle_id,
    `data`.destination_address.pincode as destination_address_pincode,
    `data`.destination_address.id as destination_address_id,
    `data`.destination_address.type as destination_address_type,
    `data`.design_tat as design_tat,
    `data`.vehicle_no as vehicle_no,
    `data`.transit_type as transit_type,
    `data`.quality as quality,
    `data`.agreement_id as agreement_id,
    `data`.group_id as connection_id,
    `data`.updated_by as updated_by,
    size(`data`.associated_connections) as ass_con_size,
    `data`.associated_connections as associated_connections,
    if(size(`data`.service_types) <=0,
        if(`data`.source_address.type = 'MOTHER_HUB' and `data`.destination_address.type = 'DELIVERY_HUB' and upper(`data`.transit_type) = 'SURFACE',
            array('REGULAR','PRIORITY','ECONOMY','DG'),
            if(upper(`data`.transit_type) ='NDD' or upper(`data`.transit_type) ='SDD' or upper(`data`.transit_type) ='PRIME',
                array('PRIORITY'),
                if (upper(`data`.transit_type) in ('GCR','GCR1','GCR2','GCR3'),
                    array('REGULAR','ECONOMY'),
                    if(upper(`data`.transit_type) = 'SURFACE',
                        array('REGULAR','DG','ECONOMY'),
                        array('NONE')
                        )

                    )
                )
            ),
            `data`.service_types
        ) as service_types,
    `data`.cutoff as cutoff,
    `data`.frequency_type as frequency_type,
    entityid as entityid
    from  bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total 
    where `data`.frequency_type = 'REGULAR') all_con
    left join
    (select 
     connection_id,
     asscon_connection_id
     from 
    (select 
    `data`.group_id as connection_id,
    asscon_connection_id
    from bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total lateral view explode(`data`.associated_connections)  conn_exp as asscon_connection_id
    )asscon_explode
    )associated_con_map
    on all_con.connection_id = associated_con_map.connection_id
    left join 
    (
    select 
    `data`.coloader as associated_coloader,
    `data`.code as associated_code,
    `data`.created_at as associated_created_at,
    `data`.type as associated_type,
    `data`.relative_reaching_date_time as associated_reaching_date_time,
    `data`.operator as associated_operator,
    `data`. mode as associated_mode,
    `data`. updated_at as associated_updated_at,
    `data`. source_address.pincode as associated_source_pincode,
    `data`. source_address.id as associated_source_address_id,
    `data`. source_address.type as associated_source_address_type,
    `data`.external_tat as associated_external_tat,
    `data`.state as associated_state,
    `data`.vehicle_id as associated_vehicle_id,
    `data`.destination_address.pincode as associated_destination_address_pincode,
    `data`.destination_address.id as associated_destination_address_id,
    `data`.destination_address.type as associated_destination_address_type,
    `data`.design_tat as associated_design_tat,
    `data`.vehicle_no as associated_vehicle_no,
    `data`.transit_type as associated_transit_type,
    `data`.quality as associated_quality,
    `data`.agreement_id as associated_agreement_id,
    `data`.group_id as associated_connection_id,
    `data`.updated_by as associated_updated_by,
    size(`data`.associated_connections) as associated_ass_con_size,
    `data`.associated_connections as associated_associated_connections,
    if(size(`data`.service_types) <=0,
        if(`data`.source_address.type = 'MOTHER_HUB' and `data`.destination_address.type = 'DELIVERY_HUB' and upper(`data`.transit_type) = 'SURFACE',
            array('REGULAR','PRIORITY','ECONOMY','DG'),
            if(upper(`data`.transit_type) ='NDD' or upper(`data`.transit_type) ='SDD' or upper(`data`.transit_type) ='PRIME',
                array('PRIORITY'),
                if (upper(`data`.transit_type) in ('GCR','GCR1','GCR2','GCR3'),
                    array('REGULAR','ECONOMY'),
                    if(upper(`data`.transit_type) = 'SURFACE',
                        array('REGULAR','DG','ECONOMY'),
                        array('NONE')
                        )

                    )
                )
            ),
            `data`.service_types
        ) as  associated_service_types,
    `data`.cutoff as associated_cutoff,
    `data`.frequency_type as associated_frequency_type,
    entityid as associated_entityid
    from bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total 
    where `data`.frequency_type = 'REGULAR'
    )associated_con
    on associated_con.associated_connection_id = associated_con_map.asscon_connection_id
)A lateral view explode(service_types)  final_con as service_type;

