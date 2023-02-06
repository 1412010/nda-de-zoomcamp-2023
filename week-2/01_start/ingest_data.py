import argparse
import os
from time import time
from datetime import timedelta
import pandas as pd
from sqlalchemy import create_engine
from prefect import task, flow
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    csv_name = 'yellow_tripdata_2021-01.csv.gz'
    
    # Dowload the CSV
    if not os.path.exists(csv_name):
        os.system(f"wget {url} -O {csv_name}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    
    df = next(df_iter)
        
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    return df

@task(log_prints=True)
def transform_data(df):
    print(f"pre:missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post:missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df    

@task(log_prints=True, retries=3)
def ingest_data(data):
    df = data    
    connection_block = SqlAlchemyConnector.load("nytaxiconnector")
    with connection_block.get_connection(begin=False) as engine:        
        t_start = time()
        # Create table with header only
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace') 
        df.to_sql(name=table_name, con=engine, if_exists='append') 
        t_end = time()
        
        print('ingest data into postgres completed..., took %.3f seconds' % (t_end - t_start))

@flow(name='subflow', log_prints=True)
def log_subflow(table_name: str):
    print(f'Logging subflow for {table_name}')

@flow(name="Ingest Flow")
def main_flow(table_name: str):
    
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    
    log_subflow(table_name=table_name)
    
    raw_data = extract_data(url)
    t_data = transform_data(raw_data)
    
    ingest_data(data=t_data)
 
    
if __name__ == '__main__':
    table_name="yellow_taxi_trips"
    main_flow(table_name)
    