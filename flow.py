from etl import *

class Full_flow:
    def __init__(self,
                 stage:str,
                 stage_file:str,
                 stage_query:str,
                 local_path:str,
                 option:dict):
        self.stage = stage
        self.stage_file = stage_file
        self.stage_query = stage_query
        self.local_path = local_path
        self.option = option
        self.sf_engine = open_sf_conn()

    def start_flow(self):
        clean_stage(stage_name=self.stage,
                    sf_engine=self.sf_engine)

        create_stage(stage_name=self.stage,
                     sf_engine=self.sf_engine)

        add_data_to_stage(stage_name=self.stage,
                          stage_table=self.stage_file,
                          sf_engine=self.sf_engine,
                          stage_query=self.stage_query)

        unload_from_stage_to_local(stage_name=self.stage,
                                   stage_table=self.stage_file,
                                   sf_engine=self.sf_engine,
                                   local_path=self.local_path)

        data = read_data_from_local(data_path=self.local_path)

        return data


stage_query_file = open('approves.sql',
                        encoding='utf-8')
stage_query = stage_query_file.read()
execute_flow = Full_flow(stage = 'calc_mfo_stage',
                            stage_file = 'calc_mfo',
                            stage_query = stage_query,
                            local_path = r'C:\temp\load\calc_mfo',
                            option = {'type': 'special',
                                      'expr': "EVENT_DT >= today() - 1"})
