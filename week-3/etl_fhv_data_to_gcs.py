import os
from pathlib import Path
from datetime import timedelta
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(url: str, file_name: str):
    """Read taxi data from web into pandas Dataframe"""
    print(url)
    if not os.path.exists(file_name):
        os.system(f"wget {url} -O {file_name}")
    return True
    
@task(log_prints=True)
def write_local(df, year, dataset_file) -> Path:
    """Write dataframe out locally as parquet file"""
    path_file = f"{dataset_file}.parquet"
    path_dir = Path(f"data/fhv/{year}")
    path_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(path_dir / path_file, index=False)
    return path_dir / path_file

@task(log_prints=True)
def write_to_gcs(path: str, year: int) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs-bucket-block")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=f"data/fhv/{year}/{path}"
    )
    

@flow(log_prints=True)
def etl_fhv_web_to_gcs(year, month) -> None:
    """The main ETL function"""
    dataset_file = f'fhv_tripdata_{year}-{month:02}.csv.gz'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}'
    
    fetch(dataset_url, dataset_file)
    # df_clean = clean(df)
    # path = write_local(df, year, dataset_file)
    write_to_gcs(dataset_file, year)
    pass

@flow(name='parent_flow')
def etl_parent_flow(
        year: int = 2019, months: list[int] = [1, 2]
    ) -> None:
        for month in months:
            etl_fhv_web_to_gcs(year, month)
    

if __name__ == '__main__':
    year = 2019
    months = [1,2]
    etl_parent_flow(year, months)
