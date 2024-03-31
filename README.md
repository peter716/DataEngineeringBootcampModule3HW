This is the 3rd module of the data engineering zoomcamp, which covers creating external tables, partitioning and clustering the table, and more.


-- Creating external table referring to gcs path
```sql
CREATE OR REPLACE EXTERNAL TABLE `digital-gearbox-411612.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://homework-3-parquets/green/green_tripdata_2022-*.parquet']
);
```

```sql
DROP TABLE IF EXISTS digital-gearbox-411612.ny_taxi.green_tripdata_table;
```
```sql
CREATE TABLE digital-gearbox-411612.ny_taxi.green_tripdata_table AS
SELECT
    *,
    TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS lpep_pickup_date_micros
FROM
    digital-gearbox-411612.ny_taxi.external_green_tripdata;
```
```sql
SELECT COUNT(*) FROM digital-gearbox-411612.ny_taxi.external_green_tripdata;
```

-- Create a non partitioned table from external table
```sql
CREATE OR REPLACE TABLE digital-gearbox-411612.ny_taxi.green_tripdata_non_partitoned AS
SELECT * FROM digital-gearbox-411612.ny_taxi.green_tripdata_table;
```

```sql
SELECT 
    COUNT(DISTINCT t1.PULocationID) AS distinct_PULocationIDs_non_partitioned
FROM 
    digital-gearbox-411612.ny_taxi.green_tripdata_non_partitoned  t1;`
```
    
```sql
SELECT 
   COUNT(DISTINCT t2.PULocationID) AS distinct_PULocationIDs_external
FROM
   digital-gearbox-411612.ny_taxi.external_green_tripdata t2;
```

```sql
SELECT 
    COUNT(*)
FROM 
    digital-gearbox-411612.ny_taxi.external_green_tripdata
WHERE 
    fare_amount = 0;`
```

-- Create a partitioned table from external table
```sql
CREATE OR REPLACE TABLE digital-gearbox-411612.ny_taxi.green_tripdata_partitoned
PARTITION BY
  DATE(lpep_pickup_date_micros) AS
SELECT * FROM digital-gearbox-411612.ny_taxi.green_tripdata_table;
```

```sql
SELECT DISTINCT(PULocationID )
FROM digital-gearbox-411612.ny_taxi.green_tripdata_non_partitoned
WHERE DATE(lpep_pickup_date_micros) BETWEEN '2022-06-01' AND '2022-06-30';
```

```sql
SELECT DISTINCT(PULocationID )
FROM digital-gearbox-411612.ny_taxi.green_tripdata_partitoned
WHERE DATE(lpep_pickup_date_micros) BETWEEN '2022-06-01' AND '2022-06-30';
```

```sql
SELECT COUNT(*)
FROM digital-gearbox-411612.ny_taxi.green_tripdata_non_partitoned
``
