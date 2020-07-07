## NYC taxi trip records analysis

This project represents Spark jobs which analyze and aggregate NYC taxi trips publicly available data.  
You can download data from here: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page  
Yellow taxi trip records for September of 2019 was used for development and testing.  
Currently results are written to stdout but the I/O method can be simply switched to another.

![master-status](https://github.com/rob-sys/nyc-trip-data-analysis/workflows/master/badge.svg)
### Jobs

1. `blog.iamrob.jobs.TripMetrics` 

   Calculates metrics & dimensions to understand and get familiar with the dataset.  
   Sample outputs: [TripMetrics.txt](https://github.com/rob-sys/nyc-trip-data-analysis/blob/master/output/TripMetrics.txt)  
   Note: most of the exploration was done via Jupyter notebooks

2. `blog.iamrob.jobs.TripTop`  

   Removes trip_distance outliers by using combined dataset features with box-and-whisker outlier detection method.  
   Calculates top 10 PULocationId, DOLocationId pairs for total_amount.  
   Sample outputs: [TripTop.txt](https://github.com/rob-sys/nyc-trip-data-analysis/blob/master/output/TripTop.txt)  
   Data stats before & after outlier removal: [Outliers.txt](https://github.com/rob-sys/nyc-trip-data-analysis/blob/master/output/Outliers.txt)

3. `blog.iamrob.jobs.TripNeighbourhoodTop`  

   Calculates the same as previous job, but assumes the pair includes values from neighboured numbers,  
   i.e. pair (5,5) includes (4,4), (4,5), (5,4), (5,5), (4,6), (6,4), (5,6), (6,5), (6,6)  
   Sample outputs: [TripNeighbourhoodTop.txt](https://github.com/rob-sys/nyc-trip-data-analysis/blob/master/output/TripNeighbourhoodTop.txt)
   

#### Prerequisites

Scala 2.11.11 https://www.scala-lang.org/download/  
Spark 2.4.0 https://spark.apache.org/downloads.html  
NYC taxi trips publicly available data: https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2019-09.csv

## Instructions

To build and create JARs, please, run:
```
sbt assembly
```
To run integration tests, please, run:
```
sbt it:test
```
To run unit tests, please, run:
```
sbt test
```
To run spark job, please, run:  
**NOTE:**  
1. **Use** `^` instead of `\` for new lines in Windows CMD environment  
2. **Replace** `CLASS_NAME` with one of the job class name: `TripMetrics`, `TripTop`, `TripNeighbourhoodTop`  
3. **Replace** `INPUT_PATH` with path to the taxi trip data csv file (i.e. `C:/data/yellow_tripdata_2019-09.csv`, `hdfs://...`)  
4. **IMPORTANT**: If `--deploy-mode cluster` data file has to be loaded to HDFS
<pre>
spark-submit \
--class blog.iamrob.jobs.<b>CLASS_NAME</b> \
--driver-memory 12g \
--master local[*] \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:./log4j.properties" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:./log4j.properties" \
./target/scala-2.11/nyc-trip-data-analysis-assembly-0.1.0.jar \
--inputPath <b>INPUT_PATH</b>
</pre>

Parameters:
```
--class  
  The entry point for your application
  Example: blog.iamrob.jobs.TripMetrics
  Required: true

--driver-memory 12g
  Set driver memory
  Required: false

--master local[*] 
  Run Spark locally with as many worker threads as logical cores on your machine
  Note: for local run only
  Required: false

--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:./log4j.properties" 
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:./log4j.properties" 
  Suppresses spark execution INFO messages in console
  Required: false

--inputPath
  Path to the taxi trip data csv file
  Required: true

--outputPath  
  Path to the output file
  Note: currently writes to stdout - not required
  Required: false
```

More about spark-submit parameters: http://spark.apache.org/docs/latest/submitting-applications.html