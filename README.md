# Azure-Synapse-SQL-Serverless-Generate-From-Hive
This is a script that will read a Hive metastore and generate SQL Serverless CREATE VIEW statements.

## About this script
- This is a rudimentry script that will perform some code generation to create SQL On-Demand (Serverless) views.  
- I have customers that have hundreds of Hive tables and they want to start to use SQL OD to query the data
- This gives a jump start to allow views to be created that will support Hive partitioning

## To run the script
- Create a Spark notebook in a cluster than can see your Hive metastore
- Copy the code from Generate-Views.scala
  - Change the REPLACE-WITH-TABLE-NAME 
- Run
- Copy the output to a SQL OD Script window in Synapse
- Run the generated code
- Highlight the SELECT TOP 10 * command and ensure the generated view is working

## What the script does
- The script will read each Hive table and generate the SQL OD code to create a view
- The script will also handle partitioned tables (projects the partitioned folders into columns)
- The script assuming "dbo" schema in your SQL OD database
- The script assumes that you are using the full ABFS path (not a mount path like some spark vendors)
- The script assumes are pointing at Parquet file (has not been tested for CSV, etc.)
- Ths script does not handle nested Parquet types or array (you can code it up: https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/query-parquet-nested-types#access-elements-from-nested-columns)

## Needs more work
- Currently this works with views, but ideally the view would be decomposed into the corrisponding SQL OD statement.  This would avoid having a view call a view (since we are generated a View from a Hive Table)
- The data type mapping is light and needs more work
- Strings do not have length in parquet files.  Ideally, a Spark job would run and find the max length of each string and store this information so this script could consume.  Right now it is hardcoded to VARCAHAR(255)

## Data Types (Tips)
- To see what data types SQL OD is inferring you run this statement, substituting your SELECT statement.  This will help you in mapping additional data types.
   - ```EXEC sp_describe_first_result_set N'SELECT TOP 100 * FROM OPENROWSET(BULK ''https://STORAGE-ACCOUNT.dfs.core.windows.net/CONTAINER/FOLDER/MY-FILE.parquet'',FORMAT=''PARQUET'') AS [r]'```

- You can also use parquet-tools for viewing parquet meta-data
   - https://github.com/apache/parquet-mr/tree/master/parquet-tools
   - ```java -jar parquet-tools-1.12.0-SNAPSHOT.jar meta "us-states.parquet"```


## Sample generated code
```
-----------------------------------------------------------------------------------------------
-- nytaxi_green
-----------------------------------------------------------------------------------------------
IF EXISTS (SELECT * FROM sys.objects WHERE name ='nytaxi_green')
  BEGIN
  DROP VIEW nytaxi_green;
  END
GO

CREATE VIEW nytaxi_green AS
SELECT
  *
FROM OPENROWSET(
   BULK 'abfss://data@mystorageaccount.dfs.core.windows.net/synapse/workspaces/folder/nytaxi_green/*.parquet',
   FORMAT='PARQUET'
)
WITH (
  vendorID int,
  lpepPickupDatetime DATETIME2,
  lpepDropoffDatetime DATETIME2,
  passengerCount int,
  tripDistance FLOAT,
  puLocationId VARCHAR(255),
  doLocationId VARCHAR(255),
  pickupLongitude FLOAT,
  pickupLatitude FLOAT,
  dropoffLongitude FLOAT,
  dropoffLatitude FLOAT,
  rateCodeID int,
  storeAndFwdFlag VARCHAR(255),
  paymentType int,
  fareAmount FLOAT,
  extra FLOAT,
  mtaTax FLOAT,
  improvementSurcharge VARCHAR(255),
  tipAmount FLOAT,
  tollsAmount FLOAT,
  ehailFee FLOAT,
  totalAmount FLOAT,
  tripType int
) AS [r];
GO

-- SELECT TOP 10 * FROM nytaxi_green;
-----------------------------------------------------------------------------------------------
```

## References
- https://docs.microsoft.com/en-us/azure/synapse-analytics/sql/query-data-storage