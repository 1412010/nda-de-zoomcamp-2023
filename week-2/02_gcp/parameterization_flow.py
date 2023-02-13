from pathlib import Path
from datetime import timedelta
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas Dataframe"""
    print(url)
    df = pd.read_csv(url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issue"""
    if 'lpep_pickup_datetime' in df.columns:
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    else:
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    return df    
    
@task(log_prints=True)
def write_local(df, color, dataset_file) -> Path:
    """Write dataframe out locally as parquet file"""
    path_file = f"{dataset_file}.parquet"
    path_dir = Path(f"data/{color}")
    path_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path_dir / path_file, compression="gzip")
    return path_dir / path_file

@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs-bucket-block")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path.as_posix()
    )
    

@flow()
def etl_web_to_gcs(color, year, month) -> None:
    """The main ETL function"""
    dataset_file = f'{color}_tripdata_{year}-{month:02}.csv.gz'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}'
    
    df = fetch(dataset_url)
    df_clean = clean(df)
    
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)
    pass

@flow(name='parent_flow')
def etl_parent_flow(
        color: str = 'yellow', year: int = 2021, months: list[int] = [1, 2]
    ) -> None:
        for month in months:
            etl_web_to_gcs(color, year, month)
    

if __name__ == '__main__':
    color = 'yellow'
    year = 2021
    months = [1, 2, 3] 
    etl_parent_flow(color, year, months)
