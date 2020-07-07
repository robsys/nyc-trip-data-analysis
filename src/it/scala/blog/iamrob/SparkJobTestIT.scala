package blog.iamrob

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FlatSpec
import blog.iamrob.jobs._
import blog.iamrob.test.SharedSparkSession.spark
import org.apache.spark.sql.Row
import blog.iamrob.storage.{Storage}

class SparkJobTestIT extends FlatSpec {

  class TestStorage(spark: SparkSession) extends Storage {
    import spark.implicits._

    override def read(path: String): Dataset[Row] = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv(path)

    override def write(ds: Dataset[_], path: String): Unit = {}
  }

  "TripMetrics" should "read from disk" in {
    val config = UsageConfig("src/it/resources/tripdata.csv")
    TripMetrics.run(spark, config, new TestStorage(spark))
  }

  "TripTop" should "read and write to disk" in {
    val config = UsageConfig("src/it/resources/tripdata.csv")
    TripTop.run(spark, config, new TestStorage(spark))
  }

  "TripNeighbourhoodTop" should "read and write to disk"  in {
    val config = UsageConfig("src/it/resources/tripdata.csv")
    TripNeighbourhoodTop.run(spark, config, new TestStorage(spark))
  }
}