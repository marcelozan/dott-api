# Dott API

Dott API is a web based solution that allows users to retrieve cycle and rides information for a vehicle or a QRCode. A cycle is defined by:
-	A deployment; 
-	The rides after the deployment, if they are available; 
-	And a pickup, if it is available;

The cycles have a start and an end, if the pickup is already done. The cycle’s start is defined by the date/time the deployment task is concluded. The cycle’s end is defined by the date/time the pickup task was created. The ride within a cycle are the rides where the ride’s start time is between the cycle’s start and cycle’s end times.

## API
### Prerequisites
- Python 3.6
- Flask Library
- SQLite3

### Business rules
The business requirements for the API were:
Given a vehicle id or a QRCode, find the last cycle for the vehicle/QRcode, by the cycle start time and:
- If the last cycle has no rides, return the last five cycles, by the deployment task created
- If the last cycle has 5 or more rides, return the last 5 rides, within that cycle by the ride start date
- If the last cycle has less than 5 rides, return the rides ordered by the ride’s start time, and complete the data, with the last cycles, ordered by deployment task created time, until reach 5 elements (rides + cycles)

### API usage
It was provided a initial database build with the ETL files output. The database file is on ./DottAPI/dott.db, if you want generate or reset the database, delete the .db file and run the following commands, from ./DottAPI folder:
```bash
sqlite3 dott.db 
.sepator ','
.import ../sample-data/cycles.csv cycles
.import ../sample-data/rides.csv rides
```
Start the API using the following command, from the same folder:
```bash
python3 API.py
```
You can hit the API on the follwing link:
http://127.0.0.1:5000/

And some usage examples are:
- http://127.0.0.1:5000/vehicles/?qrcode=00SVYC&vehicle=zlgO7gTd13bjDC0eTSMM
- http://127.0.0.1:5000/vehicles/?vehicle=zlgO7gTd13bjDC0eTSMM
- http://127.0.0.1:5000/vehicles/?qrcode=00SVYC
- http://127.0.0.1:5000/vehicles/?vehicle=zlgO7gTd13bjDC0eTSMM&qrcode=00SVYC

The API will return the data, as stated on the Business rules section.
If no query parameter is provide or a vehicle

### Technical Approach
The API reads a database with two tables:
rides: Containing the rides data
cycles: Containing the cycles data

First the API extracts the query parameters from the URL. If no parameters were found, an 404 is raised. After that, the API looks for the last cycle in the cycles table. If the cycle does not have rides, the API looks for the last 5 cycles, if the cycle has rides, the API looks for the last 5 rides for that cycle in the rides table. If the cycle has at least 5 rides, return them, else it looks for the last X cycles, where X is the value of 5 - number of rides. And then, return the rides and cycles found.

For simplicity of delivery and evaluate the assignment it was  sqlite3 as RDBMS. In a real solution approach, a more robust RDBMS needs to be chosen i.e MySQL, PostgreSQL, Oracle. Those RDBMS support concurrent connections, increasing the response time of the API. Also they support indexes on tables’ columns, enabling fast scan of the rows. The index strategy for the tables used on the API should be:
- Cycles: One index for vehicle_id column and one index for the qr_code column
- Rides: One index on deployment_id column

Those RDBMS also support table’s partitions, and index’s partitions. If those tables grow on large volumes, those features could be used to distribute data files evenly, and enabling parallel scan on those data files. If partition the tables and the indexes are needed, the time_cycle_started column should be used in the cycles table, and the time_cycle_started should be used on the rides table.

Those databases can be hosted on internal company servers, or a cloud solution can be used as Google Cloud SQL

The tables’ load data needs to be done by a scheduled job. I will provide a little more details on Scheduling session.

The source code for the API can be found in ./DottAPI/API.py
The tests can be found in ./DottAPI/TestAPI.py
For the tests, unittest was used

**Performance issue**: One of the requirements was that the API supports more than 5000 hits per minute. Using sqlite3 I was not able to fulfil the requirement. Sqlite3 does not support the features described above. On a more robust RDBMS, the requirement can be achieved.

### Scaling up for huge amount of data:
If the amount of data grows exponentially, to a size that a non distribute RDBMS can handle, one could think about use a distributed noSQL database such as Hbase or Google’s Big Table. But, the actual data model needs to be adapted. Instead of two tables, the data model should have just one. The row key for the table should be vehicle_id + time_cycle_started. Also, the row in the table should have all the cycle’s data, and rides’ data. The API should query the table just by the vehicle and retrieve the most recent row. The API should apply the rules discussed earlier, and retrieve the data for the user.
This approach works better on real huge amounts of data (such as few TBs). 
If the volume is under one TB, I think the RDBMS approach is more suitable.
I do not recommend using query engines such as BigQuery for this API, because the requirements are operational and restricted. Big Query is more suitable for analytical requirements. Queries done against BigQuery tables, has a little delay, while the engine is building the query plan and setup the job. For operational purposes this delay can impact heavily on the users experience. 

## ETL
The purpose of an ETL is to perform some actions on raw data, to clean it, enhance it, and load the treated data into a target system. ETLs are commonly used to load data on Data Warehouses and Data Lakes (Analytical and Support Decisions systems).

The Dott API ETL has a final goal to generate files to be loaded on the API's database. Those files can also be loaded to an Data Warehouse or Data Lake.

### Prerequisites
- Spark 2.4.x
- Java JDK 8

### ETL Assumptions
- The ETL is built on the top of some assumptions:
- The scheduling will be deal on other application, such as Apache Airflow
- The ETL will consume and process all the files, with the proper name in source path
- So to work right an clean up policy should be placed. This cleaned up policy was assumed as: we expect for 7 days for late arrival events, and the maximum cycle duration is also 7 days. So for deployments we will have files for the last 7 days, and for rides and pickups we will have files for the last 14 days
- With the assumption above, the ETL will process the source files and rewrite the last 7 days of data on target


### Run the ETL
You can easily run the ETL by the following command from ./jar folder:
```bash
spark-submit dott_2.11-0.1.jar
```
To run the ETL, the source files must be in /data-engineer-test/ folder. And the user must have write permissions for the target folders /target/cycle/ and /target/rides

The ETL will run and appy the following steps on the source files:
- Extract
- DataClean
- Transform
- Load

### Extract
In this phase, the application will get the source files path from ApplicationParameters trait, and will apply the schema for each file

### DataClean
In this phase, the application will get the data extracted, and will perform some quality checks. The quality checks in place are:
- Null values on every column from extracted data
- Invalids latitude and longitude
- Invalids amounts (less than 0)
- task created greater than task resolved  
- Ride start greater than ride end
- Duplicates by task_id and ride_id

More checks can be added on DataClean Object, just putting the name of the check, and the column expression to be checked
The checks are performed, and the rows that do not pass are excluded from the processing, and logged into the rejects folder, with the name of the failed check. (see a reject file example in /sample-data/reject-output).

Those checks are performed by the objects in Rules.scala, that can be evolved to a more complex data quality and data profile framework

### Transform
In this phase, the ETL will perform the actual business transformations on the cleaned data. The steps of the transformations are:
- Rename the raw columns to a more meaningful business names
- Calculate the rides metrics. For the distance it was used the Haversine formula
- Conform and union the 3 sources (rides, deployments and pickups)
- Build the cycle, using window functions
- Exclude anomalies
- Aggregate the rows to calculate the cycle metrics
- Output the cycles and rides
#### Anomalies
Since the cycle is defined by deployment -> rides -> pickup, some anomalies are excluded after the cycle is built:
- Rows with no deployment associated, since deployments start a cycle
- Deployments associated with more than one pickup; those rows represent two or more consecutive pickups with no deployment between them. The application keeps the rows with the most closed pickup from the deployment
- Pickups associated with more than one deployment; those rows represent two or more deployments with no pickup between them. The application will keeps the rows with the most closed deployment from the pickup
- Rides that happened after the cycle ends

Cycles with no pickup are not considered anomalies, because they are considered as open cycles

### Load
In this phase, the application gets the transformed data and loads into two different folders, /target/cycle/ and /target/rides . The application loads the data in csv files

### ETL Considerations
The ETL development was done in Spark. Spark is a scalable processing engine, that can run in local environments and in distributed clusters. Also, it is portable, and can be run as is on ephemeral clusters such as Google DataProc.

Spark has many APIs. For performance reasons, the DataFrame Scala API was chosen. This API uses Catalyst to build optimal query plans. Also it uses Tungsten, an off-heap data encoder. Tungsten, as an off-heap encoder, avoids JVM garbage collector to be triggered while processing the data.

Other advantage of using this API, is the simplicity of adapt it to streaming data requirements

To simplify the delivery and evaluation, the file formats used were csv and json files. In a production environment, the file format needs to be different. If the ETL will deal huge amounts of data, in a batch way, the best choice would be parquet. Parquet is an open source file format that can be easily split between different nodes on a File system like HDFS or Google FS, even when compacted. The snappy compaction is the default compaction used by Spark, and achieve great compaction rates, and it is little CPU intensive.

One more advantage of using parquet: parquet has the schema enforcement, increasing the quality of data to be read and load. 
  
For a production environment, the ETL needs also a little adaptation. Right now, it is overwriting the entire target folder. If the assumptions above are in place, ETL should only overwrite the last 7 days of data for cycles and rides.

The source code for the ETL can be found in the folder /Dott/src/main
The test can be found in /Dott/src/test and the test library used was scalatest

Be aware that for run the ExtractTest.scala and TrasfomationStepsTest.scala the source files must be in the source folder

To change the source files paths and targets folder, you need to change the ApplicationParameters trait and recompile the code. That forces for each change the application needs to pass through the entire development cycle, increasing quality.

## Analytical Solutions
The first approach would be design a dimensional model to answer the business questions regarding cycles and rides.

Looking at the source data we can clearly identify two fact tables:
Rides
Cycles
Rides have the grain of a ride, and has all the metrics regarding the ride such as amount, distance, duration.
Cycles have the grain of deployment and has metrics regarding the cycle such as total amount, total distance, total rides, duration, deployment duration, pickup duration.
In addition to the above metrics, the tables should have surrogate keys(SK) to join with the dimensions.
The dimensions should be:
- Vehicle dimension
- Date dimension
- Location dimension (at the grain of street as an example); this dimension only connects to rides fact table in the assignment
- Customer dimension (data is not provided in the source files) ;this dimension only connects to rides fact table in the assignment

Those dimensions should have SCD1 and SCD2 attributes as required by the business questions. 
Also, degenerate dimensions should be included for rides(the ride_id) and for cycles(deployment_id and pickup_id)
The metrics between the two tables should be combined using the conformed attributes dimensions.

For performance reasons the two fact tables need to be partitioned by the date (cycle start and ride start). All facts SK needs to be indexed. Also the dimensions SK needs to be indexed. 

A second approach would be use a distributed query engine such as Google BigQuery. 

The data model for BigQuery is similar to the dimensional modeling, with little adaptations:
- Instead of use two fact tables, the rides table can be modeled as a REPEATED RECORD inside the cycles fact table
- Instead of using SK on facts and dimensions, the most used attributes in dimension tables needs to be materialized at cycles table, in RECORD columns

## Scheduling and Orchestration
No scheduler solution was built for the solution, but the steps that the scheduler must perform are described below

For the scheduling solution, I assume we are going to implement the Analytical Solution, using BigQuery, and DataProc as the Cluster solution for the ETL.

It is also assumed that the ETL output is already in the format describe in Analytical Solutions. 

This solution can be used in Apache Airflow or Google Cloud Composer. The steps for the DAG are:
- task for lookup dependencies state (i.e job that copy the files to source folder)
- task for perform the cleanup policy
- task for create the cluster on DataProc
- task for submit the ETL on DataProc cluster
- task for turn off the DataProc cluster
- task for load the ETL output files on BigQuery table
- task for right a signal informing the DAG is finished

## Deployment pipeline in GCP
The repository should be able to use a package managment system like Jenkins, with a CI/CD pipeline.

Using the same assumptions of Scheduling, the steps for the pipeline are:
- compile the Scala code
- Run the unit tests (if fails, skip the next steps)
- copy the jar file generated to GFS bucket
- copy the DAG files to Airflow Server
- apply changes to the data model in BigQuery

## Final Considerations
For the Analytical Solution, the best configuration could be:
- Google File System as the primary storage
- Parquet as the file format
- Spark with Google DataProc as the processing engine 
- Google BigQuery for the analytical queries
- Apache Airflow as the orchestrator

For the API: I will ague that the Cloud SQL or even a in house database server could works better than BigQuery.   

