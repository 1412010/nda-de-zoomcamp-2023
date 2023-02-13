# Week 2: Workflow orchestration with Prefect

+ Create external table from uploaded file in GCS for FHV trip data (2019):  

```sql
CREATE OR REPLACE EXTERNAL TABLE
  `nda-de-zoomcamp.fhv_trips.external_fhv_data_2019` 

OPTIONS ( 
  format = 'csv',
  uris = ['gs://nda-de-zoomcamp-bucket/data/fhv/2019/fhv_tripdata_2019-01.csv.gz',
          'gs://nda-de-zoomcamp-bucket/data/fhv/2019/fhv_tripdata_2019-02.csv.gz',
          'gs://nda-de-zoomcamp-bucket/data/fhv/2019/fhv_tripdata_2019-03.csv.gz',
          'gs://nda-de-zoomcamp-bucket/data/fhv/2019/fhv_tripdata_2019-04.csv.gz',
          'gs://nda-de-zoomcamp-bucket/data/fhv/2019/fhv_tripdata_2019-05.csv.gz',
          'gs://nda-de-zoomcamp-bucket/data/fhv/2019/fhv_tripdata_2019-06.csv.gz',
          'gs://nda-de-zoomcamp-bucket/data/fhv/2019/fhv_tripdata_2019-07.csv.gz',
          'gs://nda-de-zoomcamp-bucket/data/fhv/2019/fhv_tripdata_2019-08.csv.gz',
          'gs://nda-de-zoomcamp-bucket/data/fhv/2019/fhv_tripdata_2019-09.csv.gz',
          'gs://nda-de-zoomcamp-bucket/data/fhv/2019/fhv_tripdata_2019-10.csv.gz',
          'gs://nda-de-zoomcamp-bucket/data/fhv/2019/fhv_tripdata_2019-11.csv.gz',
          'gs://nda-de-zoomcamp-bucket/data/fhv/2019/fhv_tripdata_2019-12.csv.gz'] 
); 
```

+ Create BigQuery table from external table:  

```sql
CREATE OR REPLACE TABLE `nda-de-zoomcamp.fhv_trips.fhv_data_2019_non_partition` AS
SELECT * FROM `nda-de-zoomcamp.fhv_trips.external_fhv_data_2019`;
```

+ Count distinct number of 'affiliate_base_number':  

```sql
SELECT COUNT(DISTINCT affiliated_base_number) AS distinct_number_of_affiliated_base_number
FROM `nda-de-zoomcamp.fhv_trips.external_fhv_data_2019`;
```

+ Count number of records that have both a blank (null) PUlocationID and DOlocationID in the entire dataset:  

```sql
SELECT COUNT(1) 
FROM `nda-de-zoomcamp.fhv_trips.fhv_data_2019_non_partition`
WHERE PUlocationID is null AND DOlocationID is null;
```

+ Create partitioned and clusterd table:  

```sql
CREATE OR REPLACE TABLE `nda-de-zoomcamp.fhv_trips.fhv_data_2019_partitioned`
PARTITION BY DATE(pickup_datetime) 
CLUSTER BY Affiliated_base_number AS
SELECT * FROM `fhv_trips.external_fhv_data_2019`;
```
