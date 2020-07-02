# Azure-Synapse-SQL-Serverless-Generate-From-Hive
This is a script that will read a Hive metastore and generate SQL Serverless CREATE VIEW statements.

## About this script
- This is a rudimentry script that will perform some code generation to create SQL On-Demand (Serverless) views.  
- I have customers that have hundreds of Hive tables and they want to start to use SQL OD to query the data
- This gives a jump start to allow views to be created that will support Hive partitioning

## To run the script
- Option A
  - Import the notebook SQL-OD-Code-Gen-Example to your Synapse workspace for a working sample.  This will demo a partitioned and non-partitioned table.
- Option B
  - Create a Spark notebook in a cluster than can see your Hive metastore
  - Copy the code from Generate-Views.scala
    - Change the REPLACE-WITH-TABLE-NAME 
  - Run
  - Copy the output to a SQL OD Script window in Synapse
  - Run the generated code in a SQL OD database.  You might want to create one by running ```CREATE DATABASE DataLake```
  - Highlight the SELECT TOP 10 * command and ensure the generated view is working

## What the script does
- The script will read each Hive table and generate the SQL OD code to create a view
- The script will also handle partitioned tables (projects the partitioned folders into columns)
- The script assumes "dbo" schema in your SQL OD database
- The script assumes that you are using the full ABFS path (not a mount path like some spark vendors, SQL OD will not be able to resolve a mount path)
- The script assumes are pointing at Parquet files (has not been tested for CSV, etc.)
- The script does not handle nested Parquet types or arrays (SQL OD does, so you can code it up: https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/query-parquet-nested-types#access-elements-from-nested-columns)

## Needs more work
- Currently this works with views, but ideally the view would be decomposed into the corrisponding SQL OD statement.  This would avoid having a view call a view (since we are generated a View from a Hive Table)
- The data type mapping is very basic and needs more work
- Strings do not have length in parquet files.  Ideally, a Spark job would run and find the max length of each string and store this information so this script could consume.  Right now it is hardcoded to VARCAHAR(255)

## Data Types (Tips)
- To see what data types SQL OD is inferring you run this statement, substituting your SELECT statement.  This will help you in mapping additional data types.
   - ```EXEC sp_describe_first_result_set N'SELECT TOP 100 * FROM OPENROWSET(BULK ''https://STORAGE-ACCOUNT.dfs.core.windows.net/CONTAINER/FOLDER/MY-FILE.parquet'',FORMAT=''PARQUET'') AS [r]'```

- You can also use parquet-tools for viewing parquet meta-data
   - https://github.com/apache/parquet-mr/tree/master/parquet-tools
   - ```java -jar parquet-tools-1.12.0-SNAPSHOT.jar meta "us-states.parquet"```


## Sample generated code - Taxi Data (Partitioned by Year and Month)
```
-----------------------------------------------------------------------------------------------
-- nyx_taxi_green
-----------------------------------------------------------------------------------------------
IF EXISTS (SELECT * FROM sys.objects WHERE name ='nyx_taxi_green')
  BEGIN
  DROP VIEW nyx_taxi_green;
  END
GO

CREATE VIEW nyx_taxi_green AS
SELECT
   CAST(r.filepath(1) AS int) AS puYear,
   CAST(r.filepath(2) AS int) AS puMonth,
  *
FROM OPENROWSET(
   BULK 'abfss://data-lake@paternostrosynapse.dfs.core.windows.net/nyx_taxi_green/puYear=*/puMonth=*/*.parquet',
   FORMAT='PARQUET'
)
WITH (
  doLocationId VARCHAR(255),
  dropoffLatitude FLOAT,
  dropoffLongitude FLOAT,
  extra FLOAT,
  fareAmount FLOAT,
  improvementSurcharge VARCHAR(255),
  lpepDropoffDatetime DATETIME2,
  lpepPickupDatetime DATETIME2,
  mtaTax FLOAT,
  passengerCount int,
  paymentType int,
  pickupLatitude FLOAT,
  pickupLongitude FLOAT,
  puLocationId VARCHAR(255),
  rateCodeID int,
  storeAndFwdFlag VARCHAR(255),
  tipAmount FLOAT,
  tollsAmount FLOAT,
  totalAmount FLOAT,
  tripDistance FLOAT,
  tripType int,
  vendorID int
) AS [r];
GO

-- SELECT TOP 10 * FROM nyx_taxi_green;
-----------------------------------------------------------------------------------------------
```

## Sample generated code - US Population Data (non-partitioned)
```
-----------------------------------------------------------------------------------------------
-- us_population_by_county
-----------------------------------------------------------------------------------------------
IF EXISTS (SELECT * FROM sys.objects WHERE name ='us_population_by_county')
  BEGIN
  DROP VIEW us_population_by_county;
  END
GO

CREATE VIEW us_population_by_county AS
SELECT
  *
FROM OPENROWSET(
   BULK 'abfss://data-lake@paternostrosynapse.dfs.core.windows.net/us_population_by_county/*.parquet',
   FORMAT='PARQUET'
)
WITH (
  decennialTime VARCHAR(255),
  stateName VARCHAR(255),
  countyName VARCHAR(255),
  population int,
  race VARCHAR(255),
  sex VARCHAR(255),
  minAge int,
  maxAge int,
  year int
) AS [r];
GO

-- SELECT TOP 10 * FROM us_population_by_county;
-----------------------------------------------------------------------------------------------

```

## References
- https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/query-data-storage