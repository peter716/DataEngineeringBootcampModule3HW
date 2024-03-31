# DataEngineeringBootcampModule3HW


-- Creating external table referring to gcs path
`CREATE OR REPLACE EXTERNAL TABLE `digital-gearbox-411612.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://homework-3-parquets/green/green_tripdata_2022-*.parquet']
);`

`DROP TABLE IF EXISTS digital-gearbox-411612.ny_taxi.green_tripdata_table;`

`CREATE TABLE digital-gearbox-411612.ny_taxi.green_tripdata_table AS
SELECT
    *,
    TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS lpep_pickup_date_micros
FROM
    digital-gearbox-411612.ny_taxi.external_green_tripdata;`

`SELECT COUNT(*) FROM digital-gearbox-411612.ny_taxi.external_green_tripdata;`

-- Create a non partitioned table from external table
`CREATE OR REPLACE TABLE digital-gearbox-411612.ny_taxi.green_tripdata_non_partitoned AS
SELECT * FROM digital-gearbox-411612.ny_taxi.green_tripdata_table;`

`SELECT 
    COUNT(DISTINCT t1.PULocationID) AS distinct_PULocationIDs_non_partitioned
FROM 
    digital-gearbox-411612.ny_taxi.green_tripdata_non_partitoned  t1;`
    

`SELECT 
   COUNT(DISTINCT t2.PULocationID) AS distinct_PULocationIDs_external
FROM
   digital-gearbox-411612.ny_taxi.external_green_tripdata t2;`

`SELECT 
    COUNT(*)
FROM 
    digital-gearbox-411612.ny_taxi.external_green_tripdata
WHERE 
    fare_amount = 0;`


-- Create a partitioned table from external table
`CREATE OR REPLACE TABLE digital-gearbox-411612.ny_taxi.green_tripdata_partitoned
PARTITION BY
  DATE(lpep_pickup_date_micros) AS
SELECT * FROM digital-gearbox-411612.ny_taxi.green_tripdata_table;`

`SELECT DISTINCT(PULocationID )
FROM digital-gearbox-411612.ny_taxi.green_tripdata_non_partitoned
WHERE DATE(lpep_pickup_date_micros) BETWEEN '2022-06-01' AND '2022-06-30';`

`SELECT DISTINCT(PULocationID )
FROM digital-gearbox-411612.ny_taxi.green_tripdata_partitoned
WHERE DATE(lpep_pickup_date_micros) BETWEEN '2022-06-01' AND '2022-06-30';`

`SELECT COUNT(*)
FROM digital-gearbox-411612.ny_taxi.green_tripdata_non_partitoned`
