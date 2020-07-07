spark-submit ^
--class blog.iamrob.jobs.TripTop ^
--driver-memory 12g ^
--master local[*] ^
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:./log4j.properties" ^
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:./log4j.properties" ^
./target/scala-2.11/trip-data-analysis-assembly-0.1.0.jar ^
--inputPath C:/data/yellow_tripdata_2019-09.csv