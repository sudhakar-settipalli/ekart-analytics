INSERT OVERWRITE TABLE service_based_routing_ideal_connection_60_fact
 -- select unix_timestamp(to_date(from_unixtime(unix_timestamp())),'yyyy-MM-dd')+8640000
select
source_id,
destination_id,
shipment_type,
dg_mp_priority_flag,
cast(date_min as int) as date_min_unixseconds,
cast(date_max as int) as date_max_unixseconds,
cutoff,
cast(split(new_ideal_intervals,'-')[0] as int) as ideal_time_int_min,
cast(split(new_ideal_intervals,'-')[1] as int) as ideal_time_int_max,
cast(split(new_ideal_intervals,'-')[2] as int) as processing_cue_flag
FROM(
    SELECT
    source_id,
    destination_id,
    shipment_type,
    dg_mp_priority_flag,
    date_min,
    date_max,
    cutoff,
    new_ideal_intervals
    --time correction
    FROM(
        SELECT
        source_id,
        destination_id,
        shipment_type,
        dg_mp_priority_flag,
        date_min,
        date_max,
         cutoff,
        ideal_time_range_array as time_correction
        FROM(
            SELECT 
            cutoffs_exploded.source_id,
            cutoffs_exploded.destination_id,
            cutoffs_exploded.shipment_type,
            cutoffs_exploded.dg_mp_priority_flag,
            cutoffs_exploded.date_min,
            cutoffs_exploded.date_max,
            cutoffs_exploded.cutoff_exp as cutoff,
            if(cast(cutoff_unexp.cutoff_list_size as int) = 1,
                array(concat_ws('-','0','86400','1')),
                 if(cast(find_in_set(cutoffs_exploded.cutoff_exp,cutoff_unexp.cutoff_string) as int) = 1,
                    if(cutoff_unexp.cutoff_list[cast(cutoff_unexp.cutoff_list_size as int)-1] -3600+1 <=0 and 
                       cutoffs_exploded.cutoff_exp-3600 <=0,
                        if(cutoffs_exploded.cutoff_exp <> 0,
                           array(concat_ws('-','0',cast(cutoffs_exploded.cutoff_exp as string),'1'),
                                 concat_ws('-',cast(cutoffs_exploded.cutoff_exp+1 as string),
                                           cast(cutoffs_exploded.cutoff_exp -3600+86400 as string),'0'),
                                 concat_ws('-',cast(cutoff_unexp.cutoff_list[cast(cutoff_unexp.cutoff_list_size as int)-1] -3600+1+86400 as string),'86400','1')),
                           array(concat_ws('-',cast(cutoffs_exploded.cutoff_exp+1 as string),
                                           cast(cutoffs_exploded.cutoff_exp -3600+86400 as string),'0'),
                                 concat_ws('-',cast(cutoff_unexp.cutoff_list[cast(cutoff_unexp.cutoff_list_size as int)-1] -3600+1+86400 as string),'86400','1'))
                           )
                        ,
                        if(cutoff_unexp.cutoff_list[cast(cutoff_unexp.cutoff_list_size as int)-1] -3600+1 > 0 and 
                         cutoffs_exploded.cutoff_exp-3600 <=0,
                          if(cutoffs_exploded.cutoff_exp-3600=0,
                             array(concat_ws('-',cast(cutoff_unexp.cutoff_list[cast(cutoff_unexp.cutoff_list_size as int)-1] -3600+1 as string),'86400','0')),
                             array(concat_ws('-',cast(cutoff_unexp.cutoff_list[cast(cutoff_unexp.cutoff_list_size as int)-1] -3600+1 as string),cast(pmod(cutoffs_exploded.cutoff_exp-3600+86400,86400) as string),'0'))
                             ),
                           if(cutoff_unexp.cutoff_list[cast(cutoff_unexp.cutoff_list_size as int)-1]-3600+1 > 0 and 
                         cutoffs_exploded.cutoff_exp-3600 >0,
                             if(cutoff_unexp.cutoff_list[cast(cutoff_unexp.cutoff_list_size as int)-1] -3600+1 > cutoffs_exploded.cutoff_exp-3600,
                               array(concat_ws('-',cast(cutoff_unexp.cutoff_list[cast(cutoff_unexp.cutoff_list_size as int)-1] -3600+1 as string),'86400','1'),
                                     concat_ws('-','0',cast(cutoffs_exploded.cutoff_exp-3600 as string),'0')),
                               array(concat_ws('-',cast(cutoff_unexp.cutoff_list[cast(cutoff_unexp.cutoff_list_size as int)-1] -3600+1 as string),cast(cutoffs_exploded.cutoff_exp-3600 as string),'0'))
                              ),
                             null
                           )
                        )
                       ),
                    if(cutoff_unexp.cutoff_list[cast(find_in_set(cutoffs_exploded.cutoff_exp,cutoff_unexp.cutoff_string)-2 as int)] -3600+1 <=0 and cutoffs_exploded.cutoff_exp-3600 <=0,
                        array(concat_ws('-',cast(cutoff_unexp.cutoff_list[cast(find_in_set(cutoffs_exploded.cutoff_exp,cutoff_unexp.cutoff_string)-2 as int)] -3600+1+86400 as string),
                                           cast(cutoffs_exploded.cutoff_exp -3600+86400 as string),'0')),
                         if( cutoffs_exploded.cutoff_exp-3600 > 0 and 
                           cutoff_unexp.cutoff_list[cast(find_in_set(cutoffs_exploded.cutoff_exp,cutoff_unexp.cutoff_string)-2 as int)] -3600+1 < 0,
                            array(concat_ws('-','0',cast(cutoffs_exploded.cutoff_exp-3600 as string),'0'),
                              concat_ws('-',cast(cutoff_unexp.cutoff_list[cast(find_in_set(cutoffs_exploded.cutoff_exp,cutoff_unexp.cutoff_string)-2 as int)] -3600+1 +86400 as string),'86400','0')
                              ),
                            if(cutoff_unexp.cutoff_list[cast(find_in_set(cutoffs_exploded.cutoff_exp,cutoff_unexp.cutoff_string)-2 as int)] -3600+1 > 0 and cutoffs_exploded.cutoff_exp-3600 > 0,
                              if(cutoff_unexp.cutoff_list[cast(find_in_set(cutoffs_exploded.cutoff_exp,cutoff_unexp.cutoff_string)-2 as int)] -3600+1 =1,
                                array(concat_ws('-','0',cast(cutoffs_exploded.cutoff_exp-3600 as string),'0')),
                                array(concat_ws('-',cast(cutoff_unexp.cutoff_list[cast(find_in_set(cutoffs_exploded.cutoff_exp,cutoff_unexp.cutoff_string)-2 as int)] -3600+1 as string),cast(cutoffs_exploded.cutoff_exp-3600 as string),'0'))
                                ),
                              null
                              )
                            )
                          )
                        )
                  )as ideal_time_range_array
            --ideal_range_calculator
            FROM(
            select 
            source_id,
            destination_id,
            shipment_type,
            dg_mp_priority_flag,
            date_min,
            date_max,
            cutoff_exp 
            FROM(
                SELECT 
                source_id,
                destination_id,
                shipment_type,
                dg_mp_priority_flag,
                date_min,
                date_max,
                sort_array(collect_set(cutoff)) as cutoff_list,
                concat_ws(',',sort_array(split(concat_ws(',',collect_set(cutoff)),','))) as cutoff_string,
                size(sort_array(collect_set(cutoff))) as cutoff_list_size
                --collect  cutoffs within date range
                FROM(
                    SELECT 
                    date_ranges.source_id,
                    date_ranges.destination_id,
                    date_ranges.shipment_type,
                    date_ranges.dg_mp_priority_flag,
                    date_ranges.date_min,
                    date_ranges.date_max,
                    --padding done to get  string sorting to work correctly
                    lpad(cast(conexions.cutoff as string),5,'0') as cutoff,
                    if (conexions.max_unixtime > cast(date_ranges.date_min as int),1,0) as cutoff_select_flag
                    --date range join with cutoff and filter eligible cutoffs
                    FROM(
                        SELECT 
                        date_range_exploded.source_id,
                        date_range_exploded.destination_id,
                        date_range_exploded.shipment_type,
                        date_range_exploded.dg_mp_priority_flag,
                        date_range_unexp.date_list[cast(find_in_set(date_exp,date_range_unexp.date_string) as int)-1] as date_min,
                        date_range_unexp.date_list[cast(find_in_set(date_exp,date_range_unexp.date_string) as int)] as date_max
                        --date range exploded
                        FROM(
                            select
                            source_id,
                            destination_id,
                            shipment_type,
                            dg_mp_priority_flag,
                            date_exp
                            FROM(
                            select
                            source_id,
                            destination_id,
                            shipment_type,
                            dg_mp_priority_flag,
                            sort_array(collect_set(date_exp_with_dup)) as date_list
                            FROM(
                            SELECT 
                            source_id,
                            destination_id,
                            shipment_type,
                            dg_mp_priority_flag,
                            date_exp_with_dup
                            --collect and  sort dates
                            FROM(
                                SELECT 
                                source_id,
                                destination_id,
                                shipment_type,
                                dg_mp_priority_flag,
                                sort_array(split(concat_ws(',',collect_set(date_range)),',')) as date_list_rem_dup
                                FROM(
                                    SELECT 
                                    source_id,
                                    destination_id,
                                    conn_id,
                                    conn_cutoff,
                                    shipment_type,
                                    dg_mp_priority_flag,
                                    concat_ws(',',cast(min_unixtime as string),cast(max_unixtime as string)) as date_range
                                    FROM(
                                        select 
                                        all_conn.source_id,
                                        all_conn.destination_id,
                                        all_conn.conn_id,
                                        all_conn.conn_cutoff,
                                        all_conn.shipment_type,
                                        all_conn.dg_mp_priority_flag,
                                        --coalesce used to take care of created_date is null
                                        coalesce(unix_timestamp(to_date(conn_entity.`data`.created_at),'yyyy-MM-dd'),1420050600) as min_unixtime,
                                        if(conn_entity.`data`.state ='inactive',
                                        coalesce(unix_timestamp(to_date(conn_entity.`data`.updated_at),'yyyy-MM-dd'),1420050600),unix_timestamp(to_date(from_unixtime(unix_timestamp())),'yyyy-MM-dd')+8640000) as max_unixtime
                                        FROM
                                        (select 
                                            source_id,
                                            destination_id,
                                            conn_id,
                                            conn_cutoff,
                                            shipment_type,
                                            dg_mp_priority_flag
                                            from bigfoot_external_neo.scp_ekl__service_based_network_and_connection_map_fact
                                            )all_conn
                                        --will replace it with left join when entity ingestion is done 
                                        left outer join
                                        bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total conn_entity 
                                        on 
                                        conn_entity.`data`.group_id = all_conn.conn_id
                                        -- left outer join 
                                        -- --static map with min max consignment create date,need to use entity once ingestion issue is resolved
                                        -- bigfoot_common.conn_start_end_date stat_conn
                                        -- on stat_conn.conn_id = all_conn.conn_id
                                        ) A

                                    )B 
                                    group by 
                                    source_id,
                                    destination_id,
                                    shipment_type,
                                    dg_mp_priority_flag
                                 )date_range_1 lateral view explode(date_list_rem_dup) C_exp as date_exp_with_dup
                                )collect_date_wo_dup
                                group by
                                source_id,
                                destination_id,
                                shipment_type,
                                dg_mp_priority_flag
                                )date_range_collect_wo_dup lateral view explode(date_list) D_exp as date_exp
                            )date_range_exploded

                        INNER JOIN 
                            (
                            SELECT
                            B_all.source_id,
                            B_all.destination_id,
                            B_all.shipment_type,
                            B_all.dg_mp_priority_flag,
                            B_all.date_list,
                            B_all.date_string 
                            FROM(
                                select
                            source_id,
                            destination_id,
                            shipment_type,
                            dg_mp_priority_flag,
                            sort_array(collect_set(date_exp_with_dup)) as date_list,
                            concat_ws(',',sort_array(split(concat_ws(',',collect_set(date_exp_with_dup)),','))) as date_string
                            FROM(
                            SELECT 
                            source_id,
                            destination_id,
                            shipment_type,
                            dg_mp_priority_flag,
                            date_exp_with_dup
                            --collect and  sort dates
                            FROM(
                                SELECT 
                                source_id,
                                destination_id,
                                shipment_type,
                                dg_mp_priority_flag,
                                sort_array(split(concat_ws(',',collect_set(date_range)),',')) as date_list_rem_dup
                                FROM(
                                    SELECT 
                                    source_id,
                                    destination_id,
                                    conn_id,
                                    conn_cutoff,
                                    shipment_type,
                                    dg_mp_priority_flag,
                                    concat_ws(',',cast(min_unixtime as string),cast(max_unixtime as string)) as date_range
                                    FROM(
                                        select 
                                        all_conn.source_id,
                                        all_conn.destination_id,
                                        all_conn.conn_id,
                                        all_conn.conn_cutoff,
                                        all_conn.shipment_type,
                                        all_conn.dg_mp_priority_flag,
                                        --coalesce used to take care of created_date is null
                                        coalesce(unix_timestamp(to_date(conn_entity.`data`.created_at),'yyyy-MM-dd'),1420050600) as min_unixtime,
                                        if(conn_entity.`data`.state ='inactive',
                                        coalesce(unix_timestamp(to_date(conn_entity.`data`.updated_at),'yyyy-MM-dd'),1420050600),unix_timestamp(to_date(from_unixtime(unix_timestamp())),'yyyy-MM-dd')+8640000) as max_unixtime
                                        FROM
                                        (select 
                                            source_id,
                                            destination_id,
                                            conn_id,
                                            conn_cutoff,
                                            shipment_type,
                                            dg_mp_priority_flag
                                            from bigfoot_external_neo.scp_ekl__service_based_network_and_connection_map_fact
                                            )all_conn
                                        --will replace it with left join when entity ingestion is done 
                                        left outer join
                                        bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total conn_entity 
                                        on 
                                        conn_entity.`data`.group_id = all_conn.conn_id
                                        -- left outer join 
                                        -- --static map with min max consignment create date,need to use entity once ingestion issue is resolved
                                        -- bigfoot_common.conn_start_end_date stat_conn
                                        -- on stat_conn.conn_id = all_conn.conn_id
                                        ) A

                                    )B 
                                    group by 
                                    source_id,
                                    destination_id,
                                    shipment_type,
                                    dg_mp_priority_flag
                                 )date_range_1 lateral view explode(date_list_rem_dup) C_exp as date_exp_with_dup
                                )collect_date_wo_dup
                                group by
                                source_id,
                                destination_id,
                                shipment_type,
                                dg_mp_priority_flag
                                )B_all
                                INNER JOIN
                                (
                                SELECT 
                                source_id,
                                destination_id,
                                shipment_type,
                                max(dg_mp_priority_flag)
                                FROM(
                                    select 
                                    all_conn.source_id,
                                    all_conn.destination_id,
                                    all_conn.conn_id,
                                    all_conn.conn_cutoff,
                                    all_conn.shipment_type,
                                    all_conn.dg_mp_priority_flag,
                                    --coalesce used to take care of created_date is null
                                  coalesce(unix_timestamp(to_date(conn_entity.`data`.created_at),'yyyy-MM-dd'),1420050600) as min_unixtime,
                                        if(conn_entity.`data`.state ='inactive',
                                        coalesce(unix_timestamp(to_date(conn_entity.`data`.updated_at),'yyyy-MM-dd'),1420050600),unix_timestamp(to_date(from_unixtime(unix_timestamp())),'yyyy-MM-dd')+8640000) as max_unixtime
                                        FROM
                                        (select 
                                            source_id,
                                            destination_id,
                                            conn_id,
                                            conn_cutoff,
                                            shipment_type,
                                            dg_mp_priority_flag
                                            from bigfoot_external_neo.scp_ekl__service_based_network_and_connection_map_fact
                                            )all_conn
                                        --will replace it with left join when entity ingestion is done 
                                        left outer join
                                        bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total conn_entity 
                                        on 
                                        conn_entity.`data`.group_id = all_conn.conn_id
                                        -- left outer join 
                                        -- --static map with min max consignment create date,need to use entity once ingestion issue is resolved
                                        -- bigfoot_common.conn_start_end_date stat_conn
                                        -- on stat_conn.conn_id = all_conn.conn_id
                                    ) A
                            group by
                            source_id,
                            destination_id,
                            shipment_type
                            )B_dg_mp_priority
                            ON
                            B_all.source_id = B_dg_mp_priority.source_id
                            and
                            B_all.destination_id = B_dg_mp_priority.destination_id
                            and
                            B_all.shipment_type = B_all.shipment_type
                            and
                            B_all.dg_mp_priority_flag = B_all.dg_mp_priority_flag
                            )date_range_unexp
                            on 
                            date_range_exploded.source_id = date_range_unexp.source_id
                            and
                            date_range_exploded.destination_id = date_range_unexp.destination_id
                            and
                            date_range_exploded.shipment_type = date_range_unexp.shipment_type
                            and
                            date_range_exploded.dg_mp_priority_flag = date_range_unexp.dg_mp_priority_flag
                            where  
                            date_range_unexp.date_list[cast(find_in_set(date_exp,date_range_unexp.date_string) as int)-1]<>date_range_unexp.date_list[cast(find_in_set(date_exp,date_range_unexp.date_string) as int)]
                            and  date_range_unexp.date_list[cast(find_in_set(date_exp,date_range_unexp.date_string) as int)] is not null
                        )date_ranges

                    inner join
                    
                    (
                    select 
                    all_conn.source_id,
                    all_conn.destination_id,
                    all_conn.conn_id,
                    all_conn.conn_cutoff as cutoff,
                    all_conn.shipment_type,
                    all_conn.dg_mp_priority_flag,
                     if(conn_entity.`data`.state ='inactive',
                                        coalesce(unix_timestamp(to_date(conn_entity.`data`.updated_at),'yyyy-MM-dd'),1420050600),unix_timestamp(to_date(from_unixtime(unix_timestamp())),'yyyy-MM-dd')+8640000) as max_unixtime
                    FROM
                    (select 
                                            source_id,
                                            destination_id,
                                            conn_id,
                                            conn_cutoff,
                                            shipment_type,
                                            dg_mp_priority_flag
                                            from bigfoot_external_neo.scp_ekl__service_based_network_and_connection_map_fact
                                            )all_conn
                    --will replace it with left join when entity ingestion is done 
                    left outer join 
                    bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total conn_entity 
                    on 
                    conn_entity.`data`.group_id = all_conn.conn_id
                    -- left outer join  
                    -- --static map with min max consignment create date,need to use entity once ingestion issue is resolved
                    -- bigfoot_common.conn_start_end_date stat_conn
                    -- on 
                    -- stat_conn.conn_id = all_conn.conn_id

                    ) conexions
                    on
                    concat_ws('-',cast(conexions.source_id as string),
                        cast(conexions.destination_id as string),
                        cast(conexions.shipment_type as string),
                        cast(conexions.dg_mp_priority_flag as string))
                    = concat_ws('-',cast(date_ranges.source_id as string),
                        cast(date_ranges.destination_id as string),
                        cast(date_ranges.shipment_type as string),
                        cast(date_ranges.dg_mp_priority_flag as string))
                    )cutoff_collector
                    where cutoff_select_flag = 1
                    group by
                    source_id,
                    destination_id,
                    shipment_type,
                    dg_mp_priority_flag,
                    date_min,
                    date_max

                )cutoffs lateral view explode(cutoff_list) cut_tab as cutoff_exp

            )cutoffs_exploded

            INNER JOIN
            ( SELECT 
                source_id,
                destination_id,
                shipment_type,
                dg_mp_priority_flag,
                date_min,
                date_max,
                sort_array(collect_set(cutoff)) as cutoff_list,
                concat_ws(',',sort_array(split(concat_ws(',',collect_set(cutoff)),','))) as cutoff_string,
                size(sort_array(collect_set(cutoff))) as cutoff_list_size
                --collect  cutoffs within date range
                FROM(
                    SELECT 
                    date_ranges.source_id,
                    date_ranges.destination_id,
                    date_ranges.shipment_type,
                    date_ranges.dg_mp_priority_flag,
                    date_ranges.date_min,
                    date_ranges.date_max,
                    --padding done to get  string sorting to work correctly
                    lpad(cast(conexions.cutoff as string),5,'0') as cutoff,
                    if (conexions.max_unixtime > cast(date_ranges.date_min as int),1,0) as cutoff_select_flag
                    --date range join with cutoff and filter eligible cutoffs
                    FROM(
                        SELECT 
                        date_range_exploded.source_id,
                        date_range_exploded.destination_id,
                        date_range_exploded.shipment_type,
                        date_range_exploded.dg_mp_priority_flag,
                        date_range_unexp.date_list[cast(find_in_set(date_exp,date_range_unexp.date_string) as int)-1] as date_min,
                        date_range_unexp.date_list[cast(find_in_set(date_exp,date_range_unexp.date_string) as int)] as date_max
                        --date range exploded
                        FROM(
                            select
                            source_id,
                            destination_id,
                            shipment_type,
                            dg_mp_priority_flag,
                            date_exp
                            FROM(
                            select
                            source_id,
                            destination_id,
                            shipment_type,
                            dg_mp_priority_flag,
                            sort_array(collect_set(date_exp_with_dup)) as date_list
                            FROM(
                            SELECT 
                            source_id,
                            destination_id,
                            shipment_type,
                            dg_mp_priority_flag,
                            date_exp_with_dup
                            --collect and  sort dates
                            FROM(
                                SELECT 
                                source_id,
                                destination_id,
                                shipment_type,
                                dg_mp_priority_flag,
                                sort_array(split(concat_ws(',',collect_set(date_range)),',')) as date_list_rem_dup
                                FROM(
                                    SELECT 
                                    source_id,
                                    destination_id,
                                    conn_id,
                                    conn_cutoff,
                                    shipment_type,
                                    dg_mp_priority_flag,
                                    concat_ws(',',cast(min_unixtime as string),cast(max_unixtime as string)) as date_range
                                    FROM(
                                        select 
                                        all_conn.source_id,
                                        all_conn.destination_id,
                                        all_conn.conn_id,
                                        all_conn.conn_cutoff,
                                        all_conn.shipment_type,
                                        all_conn.dg_mp_priority_flag,
                                        --coalesce used to take care of created_date is null
                                        coalesce(unix_timestamp(to_date(conn_entity.`data`.created_at),'yyyy-MM-dd'),1420050600) as min_unixtime,
                                        if(conn_entity.`data`.state ='inactive',
                                        coalesce(unix_timestamp(to_date(conn_entity.`data`.updated_at),'yyyy-MM-dd'),1420050600),unix_timestamp(to_date(from_unixtime(unix_timestamp())),'yyyy-MM-dd')+8640000) as max_unixtime
                                        FROM
                                        (select 
                                            source_id,
                                            destination_id,
                                            conn_id,
                                            conn_cutoff,
                                            shipment_type,
                                            dg_mp_priority_flag
                                            from bigfoot_external_neo.scp_ekl__service_based_network_and_connection_map_fact
                                            )all_conn
                                        --will replace it with left join when entity ingestion is done 
                                        left outer join
                                        bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total conn_entity 
                                        on 
                                        conn_entity.`data`.group_id = all_conn.conn_id
                                        -- left outer join 
                                        -- --static map with min max consignment create date,need to use entity once ingestion issue is resolved
                                        -- bigfoot_common.conn_start_end_date stat_conn
                                        -- on stat_conn.conn_id = all_conn.conn_id
                                        ) A

                                    )B 
                                    group by 
                                    source_id,
                                    destination_id,
                                    shipment_type,
                                    dg_mp_priority_flag
                                 )date_range_1 lateral view explode(date_list_rem_dup) C_exp as date_exp_with_dup
                                )collect_date_wo_dup
                                group by
                                source_id,
                                destination_id,
                                shipment_type,
                                dg_mp_priority_flag
                                )date_range_collect_wo_dup lateral view explode(date_list) D_exp as date_exp
                            )date_range_exploded

                        INNER JOIN 
                            (
                            SELECT
                            B_all.source_id,
                            B_all.destination_id,
                            B_all.shipment_type,
                            B_all.dg_mp_priority_flag,
                            B_all.date_list,
                            B_all.date_string 
                            FROM(
                                select
                            source_id,
                            destination_id,
                            shipment_type,
                            dg_mp_priority_flag,
                            sort_array(collect_set(date_exp_with_dup)) as date_list,
                            concat_ws(',',sort_array(split(concat_ws(',',collect_set(date_exp_with_dup)),','))) as date_string
                            FROM(
                            SELECT 
                            source_id,
                            destination_id,
                            shipment_type,
                            dg_mp_priority_flag,
                            date_exp_with_dup
                            --collect and  sort dates
                            FROM(
                                SELECT 
                                source_id,
                                destination_id,
                                shipment_type,
                                dg_mp_priority_flag,
                                sort_array(split(concat_ws(',',collect_set(date_range)),',')) as date_list_rem_dup
                                FROM(
                                    SELECT 
                                    source_id,
                                    destination_id,
                                    conn_id,
                                    conn_cutoff,
                                    shipment_type,
                                    dg_mp_priority_flag,
                                    concat_ws(',',cast(min_unixtime as string),cast(max_unixtime as string)) as date_range
                                    FROM(
                                        select 
                                        all_conn.source_id,
                                        all_conn.destination_id,
                                        all_conn.conn_id,
                                        all_conn.conn_cutoff,
                                        all_conn.shipment_type,
                                        all_conn.dg_mp_priority_flag,
                                        --coalesce used to take care of created_date is null
                                        coalesce(unix_timestamp(to_date(conn_entity.`data`.created_at),'yyyy-MM-dd'),1420050600) as min_unixtime,
                                        if(conn_entity.`data`.state ='inactive',
                                        coalesce(unix_timestamp(to_date(conn_entity.`data`.updated_at),'yyyy-MM-dd'),1420050600),unix_timestamp(to_date(from_unixtime(unix_timestamp())),'yyyy-MM-dd')+8640000) as max_unixtime
                                        FROM
                                        (select 
                                            source_id,
                                            destination_id,
                                            conn_id,
                                            conn_cutoff,
                                            shipment_type,
                                            dg_mp_priority_flag
                                            from bigfoot_external_neo.scp_ekl__service_based_network_and_connection_map_fact
                                            )all_conn
                                        --will replace it with left join when entity ingestion is done 
                                        left outer join
                                        bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total conn_entity 
                                        on 
                                        conn_entity.`data`.group_id = all_conn.conn_id
                                        -- left outer join 
                                        -- --static map with min max consignment create date,need to use entity once ingestion issue is resolved
                                        -- bigfoot_common.conn_start_end_date stat_conn
                                        -- on stat_conn.conn_id = all_conn.conn_id
                                        ) A

                                    )B 
                                    group by 
                                    source_id,
                                    destination_id,
                                    shipment_type,
                                    dg_mp_priority_flag
                                 )date_range_1 lateral view explode(date_list_rem_dup) C_exp as date_exp_with_dup
                                )collect_date_wo_dup
                                group by
                                source_id,
                                destination_id,
                                shipment_type,
                                dg_mp_priority_flag
                                )B_all
                                INNER JOIN
                                (
                                SELECT 
                                source_id,
                                destination_id,
                                shipment_type,
                                max(dg_mp_priority_flag)
                                FROM(
                                    select 
                                    all_conn.source_id,
                                    all_conn.destination_id,
                                    all_conn.conn_id,
                                    all_conn.conn_cutoff,
                                    all_conn.shipment_type,
                                    all_conn.dg_mp_priority_flag,
                                    --coalesce used to take care of created_date is null
                                    coalesce(unix_timestamp(to_date(conn_entity.`data`.created_at),'yyyy-MM-dd'),1420050600) as min_unixtime,
                                    if(conn_entity.`data`.state ='inactive',
                                        coalesce(unix_timestamp(to_date(conn_entity.`data`.updated_at),'yyyy-MM-dd'),1420050600),unix_timestamp(to_date(from_unixtime(unix_timestamp())),'yyyy-MM-dd')+8640000) as max_unixtime
                                    FROM
                                    (select 
                                            source_id,
                                            destination_id,
                                            conn_id,
                                            conn_cutoff,
                                            shipment_type,
                                            dg_mp_priority_flag
                                            from bigfoot_external_neo.scp_ekl__service_based_network_and_connection_map_fact
                                            )all_conn
                                    --will replace it with left join when entity ingestion is done 
                                    LEFT OUTER JOIN 
                                    bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total conn_entity 
                                    on 
                                    conn_entity.`data`.group_id = all_conn.conn_id
                                    -- LEFT OUTER JOIN 
                                    -- --static map with min max consignment create date,need to use entity once ingestion issue is resolved
                                    -- bigfoot_common.conn_start_end_date stat_conn
                                    -- on stat_conn.conn_id = all_conn.conn_id
                                    ) A
                            group by
                            source_id,
                            destination_id,
                            shipment_type
                            )B_dg_mp_priority
                            ON
                            B_all.source_id = B_dg_mp_priority.source_id
                            and
                            B_all.destination_id = B_dg_mp_priority.destination_id
                            and
                            B_all.shipment_type = B_all.shipment_type
                            and
                            B_all.dg_mp_priority_flag = B_all.dg_mp_priority_flag
                            )date_range_unexp
                            on 
                            date_range_exploded.source_id = date_range_unexp.source_id
                            and
                            date_range_exploded.destination_id = date_range_unexp.destination_id
                            and
                            date_range_exploded.shipment_type = date_range_unexp.shipment_type
                            and
                            date_range_exploded.dg_mp_priority_flag = date_range_unexp.dg_mp_priority_flag
                            where  
                            date_range_unexp.date_list[cast(find_in_set(date_exp,date_range_unexp.date_string) as int)-1]<>date_range_unexp.date_list[cast(find_in_set(date_exp,date_range_unexp.date_string) as int)]
                            and  date_range_unexp.date_list[cast(find_in_set(date_exp,date_range_unexp.date_string) as int)] is not null
                        )date_ranges

                    inner join
                    
                    (
                    select 
                    all_conn.source_id,
                    all_conn.destination_id,
                    all_conn.conn_id,
                    all_conn.conn_cutoff as cutoff,
                    all_conn.shipment_type,
                    all_conn.dg_mp_priority_flag,
                    if(conn_entity.`data`.state ='inactive',
                                        coalesce(unix_timestamp(to_date(conn_entity.`data`.updated_at),'yyyy-MM-dd'),1420050600),unix_timestamp(to_date(from_unixtime(unix_timestamp())),'yyyy-MM-dd')+8640000) as max_unixtime
                    FROM
                    (select 
                                            source_id,
                                            destination_id,
                                            conn_id,
                                            conn_cutoff,
                                            shipment_type,
                                            dg_mp_priority_flag
                                            from bigfoot_external_neo.scp_ekl__service_based_network_and_connection_map_fact
                                            )all_conn
                    --will replace it with left join when entity ingestion is done 
                    left outer join 
                    bigfoot_snapshot.dart_wsr_scp_ekl_connection_1_11_view_total conn_entity 
                    on 
                    conn_entity.`data`.group_id = all_conn.conn_id
                    -- left outer join  
                    -- --static map with min max consignment create date,need to use entity once ingestion issue is resolved
                    -- bigfoot_common.conn_start_end_date stat_conn
                    -- on 
                    -- stat_conn.conn_id = all_conn.conn_id

                    ) conexions
                    on
                    concat_ws('-',cast(conexions.source_id as string),
                        cast(conexions.destination_id as string),
                        cast(conexions.shipment_type as string),
                        cast(conexions.dg_mp_priority_flag as string))
                    = concat_ws('-',cast(date_ranges.source_id as string),
                        cast(date_ranges.destination_id as string),
                        cast(date_ranges.shipment_type as string),
                        cast(date_ranges.dg_mp_priority_flag as string))
                    )cutoff_collector
                    where cutoff_select_flag = 1
                    group by
                    source_id,
                    destination_id,
                    shipment_type,
                    dg_mp_priority_flag,
                    date_min,
                    date_max
            )cutoff_unexp
            ON 
            cutoffs_exploded.source_id = cutoff_unexp.source_id
            and
            cutoffs_exploded.destination_id  = cutoff_unexp.destination_id
            and
            cutoffs_exploded.shipment_type = cutoff_unexp.shipment_type
            and 
            cutoffs_exploded.dg_mp_priority_flag = cutoff_unexp.dg_mp_priority_flag
            and
            cutoffs_exploded.date_min = cutoff_unexp.date_min
            and
            cutoffs_exploded.date_max = cutoff_unexp.date_max
            )ideal_time_ranges
        )ideal_time_hack lateral view explode(time_correction) time_cor_tab as new_ideal_intervals
    )corrected_ideal_time_ranges;

