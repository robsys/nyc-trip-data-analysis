package blog.iamrob.storage


import scala.io.Source._
import org.apache.spark.sql.Row
import org.apache.spark.sql.{Encoder, SparkSession, Dataset, DataFrameWriter}

trait Storage {
  def read(path: String, format: String): Dataset[Row]
  def write(
    ds: Dataset[_], path: String,
    format: String, mode: String,
    formatWriter: DataFrameWriter[_] => DataFrameWriter[_] = x => x)
}

class LocalStorage(spark: SparkSession) extends Storage {
  import spark.implicits._

   override def read(path: String, format: String): Dataset[Row] = {
    format match {
      case "csv" => readCsv[Row](path)
      case "api-csv" => downloadCsv[Row](path)
      case _ => readAny(path, format)
    }
  }

  override def write(
    ds: Dataset[_], path: String, 
    format: String, mode: String,
    formatWriter: DataFrameWriter[_] => DataFrameWriter[_]) : Unit = {

    val writer = ds.write.format(format).mode(mode)    
    formatWriter(writer).save(path)
  }

  private def readCsv[T](path: String)= {
    getCsvReader().csv(path)
  }

  private def downloadCsv[T](path: String) = {
    val res = fromURL(path).mkString.stripMargin.lines.toList
    val csvData = spark.sparkContext.parallelize(res).toDS()
    getCsvReader().csv(csvData)
  } 

  private def readAny[T](path: String, format: String) = {
    spark.read.format(format).load(path)
  }

  private def getCsvReader() = {
    spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
  }
}