INSERT overwrite TABLE la_shipment_l0_runsheet_fact
SELECT tasklist_tracking_id ,
       ekl_facility_id ,
       last_tasklist_agent_id ,
       last_tasklist_updated_at ,
       tasklist_type ,
       last_tasklist_id ,
       tasklist_status ,
       runsheet_id ,
       last_tasklist_agent_id_key ,
       ekl_facility_id_key ,
       IF (last_tasklist_agent_id IN ('85070',
                                      '85071',
                                      '85073',
                                      '85068',
                                      '85072',
                                      '85069',
                                      '83267',
                                      '85443',
                                      '83268',
                                      '83271',
                                      '85442',
                                      '83270',
                                      '85444',
                                      '85650',
                                      '85441',
                                      '85446',
                                      '85651',
                                      '85653',
                                      '85652',
                                      '83269',
                                      '74352',
                                      '74353',
                                      '73788',
                                      '73793',
                                      '73798',
                                      '85405',
                                      '73766',
                                      '85402',
                                      '73794',
                                      '73790',
                                      '73797',
                                      '85406',
                                      '73791',
                                      '73787',
                                      '73789',
                                      '85403',
                                      '73786',
                                      '73796',
                                      '73792',
                                      '73768',
                                      '73795',
                                      '85404',
                                      '73779',
                                      '77288',
                                      '73767',
                                      '73780',
                                      '73773',
                                      '73777',
                                      '85973',
                                      '73774',
                                      '73775',
                                      '77290',
                                      '73776',
                                      '77289',
                                      '73778',
                                      '74496',
                                      '74499',
                                      '74354',
                                      '79298',
                                      '79299',
                                      '82779'),
									  1,
                                      0) AS is_cpu_vendor
FROM bigfoot_external_neo.scp_ekl__la_shipment_runsheet_fact;