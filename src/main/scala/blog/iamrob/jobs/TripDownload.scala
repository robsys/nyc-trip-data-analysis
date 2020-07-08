package blog.iamrob.jobs

import blog.iamrob._
import scala.io.Source._
import blog.iamrob.storage._
import org.apache.spark.sql.{Dataset, SparkSession, DataFrameWriter}
import org.apache.spark.sql.functions.{col, desc, max, expr, year, month}

object TripDownload extends SparkJob {

  override def appName: String = "NYC trip data download"

  override def run(spark: SparkSession, config: UsageConfig, storage: Storage): Unit = {
    val tripData = download(spark, config)
    val enhancedData = transform(spark, tripData, config)
    
    val formatWriter = (x: DataFrameWriter[_]) => x.partitionBy("year", "month")
    storage.write(
      enhancedData, 
      config.outputPath, 
      config.outputFormat, 
      config.outputMode,
      formatWriter)
  }

  def download(spark: SparkSession, config: UsageConfig): Dataset[_] = {
    import spark.implicits._

    val res = fromURL(config.inputPath).mkString.stripMargin.lines.toList
    val csvData = spark.sparkContext.parallelize(res).toDS()
    spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv(csvData)
  }

  def transform(spark: SparkSession, data: Dataset[_], config: UsageConfig): Dataset[_] = {
    import spark.implicits._

    // Filter out data which does not belong to the year and month (there are some outliers in most cases)
    // Another approach would be to update the inccorect dates
    val dataWithPartitions = data
      .withColumn("year",  expr("year(tpep_pickup_datetime)"))
      .withColumn("month", expr("month(tpep_pickup_datetime)"))

    val topYear = getTopValueByCount(dataWithPartitions, "year")
    val topMonth = getTopValueByCount(dataWithPartitions, "month")

    data
      .withColumn("year",  expr("year(tpep_pickup_datetime)"))
      .withColumn("month", expr("month(tpep_pickup_datetime)"))
      .where(f"year = ${topYear} AND month = ${topMonth}")
  }

  def getTopValueByCount(data: Dataset[_], column: String) = {
    data
      .groupBy(column)
      .count()
      .orderBy(desc("count"))
      .limit(1)
      .collect()(0)(0)
  }
}
