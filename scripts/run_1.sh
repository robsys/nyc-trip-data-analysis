spark-submit ^
--class blog.iamrob.jobs.TripDownload ^
--driver-memory 12g ^
--master local[*] ^
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:./log4j.properties" ^
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:./log4j.properties" ^
./target/scala-2.11/nyc-trip-data-analysis-assembly-0.1.0.jar ^
--inputPath https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2019-09.csv ^
--inputFormat csv ^
--inputYear 2019 ^
--inputMonth 9 ^
--outputPath C:/data/bronze/nyc_yellow_taxi/ ^
--outputFormat orc ^
--outputMode overwrite