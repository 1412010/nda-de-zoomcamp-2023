import argparse
import os
from time import time
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    zip_name = 'download'
    csv_name = 'green_tripdata_2019-01.csv.gz'
    
    # Dowload the CSV
    os.system(f"wget {url} -O {csv_name}")
    
    # Create engine and connect to Postgres DB
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    # engine.connect()
    
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    
    df = next(df_iter)
        
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    # Create table with header only
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace') 
    df.to_sql(name=table_name, con=engine, if_exists='append') 

    while True:
        t_start = time()
        
        df = next(df_iter)
                
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        
        df.to_sql(name=table_name, con=engine, if_exists='append') 
        
        t_end = time()
        
        print('insert another chunk..., took %.3f seconds' % (t_end - t_start))
    
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser("Ingest CSV data to Postgres")
    
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')
    
    args = parser.parse_args()
    
    main(args)
    
    
    