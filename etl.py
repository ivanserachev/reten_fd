from sf_connection import SF_Connection
import snowflake.connector
import polars as pl
import os

SF_engine = snowflake.connector.cursor.SnowflakeCursor

def open_sf_conn() -> SF_engine:
    sf=SF_Connection()
    engine=sf.open_conn()

    return engine

def create_stage(stage_name:str, sf_engine:SF_engine) -> None:
    query =  (f"""CREATE OR REPLACE TEMPORARY STAGE PROD_TEAM.MFO.{stage_name} 
                  FILE_FORMAT = PROD_TEAM.MFO.sf_tut_parquet_format""")
    sf_engine.execute(query)
    print('stage created')

def add_data_to_stage(stage_name:str, stage_table:str, sf_engine:SF_engine, stage_query:str)-> None:
    query = (f"COPY INTO @~/PROD_TEAM.MFO.{stage_name}/out/{stage_table} FROM \n"
             f"                ({stage_query}) \n"
             f"                file_format = (type = 'parquet') \n"
             f"                header = true")
    sf_engine.execute(query)
    print('data added to stage')

def unload_from_stage_to_local(stage_name:str, stage_table:str, sf_engine:SF_engine, local_path:str)-> None:
    isExist = os.path.exists(local_path)
    if not isExist:
        os.makedirs(local_path)
    query = f"GET @~/PROD_TEAM.MFO.{stage_name}/out/{stage_table} file://{local_path}"
    sf_engine.execute(query)
    print('data unload to local')

def clean_stage(stage_name:str, sf_engine:SF_engine)-> None:
    query = f"""REMOVE @~/PROD_TEAM.MFO.{stage_name}"""
    sf_engine.execute(query)


def read_data_from_local(data_path:str):
    file_lst = os.listdir(data_path)
    res_data = pl.DataFrame()
    for file in file_lst:
        data = pl.read_parquet(source=os.path.join(data_path,file),
                               )
        data.fill_nan(0)
        res_data = pl.concat([res_data, data])

        os.remove(os.path.join(data_path,file))
    return  res_data