from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(log_prints=True, retries=3)
def extract_from_gcs(color: str, year: str, month: str) -> Path:
    """Download tripdata from GCS"""
    dataset_file = f'{color}_tripdata_{year}-{month:02}.csv.gz'
    gcs_path = f"data/{color}/{dataset_file}.parquet"
    
    gcs_block = GcsBucket.load("gcs-bucket-block")
    gcs_block.get_directory(
        from_path=gcs_path, 
        local_path=f'../data/'
    )
    return Path(f'../data/{gcs_path}')

@task(log_prints=True)
def transform_data(path: Path) -> pd.DataFrame:
    """Brief data cleaning"""    
    df = pd.read_parquet(path)
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df['passenger_count'] = df['passenger_count'].fillna(0)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task(log_prints=True)
def write_to_bq(df: pd.DataFrame) -> None:
    """Write Dataframe into Bigquery"""
    gcp_credentials_block = GcpCredentials.load("nda-gcp-credentials")
    df.to_gbq(
        destination_table='ny_taxi_trips.yellow_trips_data',
        project_id='nda-de-zoomcamp',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=50_000,
        if_exists='append'
    )

@flow()
def etl_gcs_to_bq(color, year, month) -> None:
    """The main ETL function to load data to BigQuery"""
    
    path = extract_from_gcs(color, year, month)
    
    df = transform_data(path) # do nothing but read dataframe only
    write_to_bq(df)
    print(f"BQ: written {len(df)} rows successfully into Bigquery")
    pass


@flow(name='main')
def etl_main_flow(
        color: str = 'yellow', year: int = 2021, months: list[int] = [1, 2]
    ) -> None:
        for month in months:
            etl_gcs_to_bq(color, year, month)
    

if __name__ == '__main__':
    color = 'yellow'
    year = 2021
    months = [1, 2, 3] 
    etl_main_flow(color, year, months)
