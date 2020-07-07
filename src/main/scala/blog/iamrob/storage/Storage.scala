package blog.iamrob.storage

import org.apache.spark.sql.{Encoder, SparkSession, Dataset, DataFrameWriter}
import org.apache.spark.sql.Row

trait Storage {
  def read(path: String, format: String): Dataset[Row]
  def write(
    ds: Dataset[_], path: String,
    format: String, mode: String,
    formatWriter: DataFrameWriter[_] => DataFrameWriter[_] = x => x)
}

class LocalStorage(spark: SparkSession) extends Storage {
  import spark.implicits._

  private def readAny[T](path: String, format: String) = {
    spark.read.format(format).load(path)
  }

  private def readCsv[T](path: String) = {
    spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv(path)
  }

  override def read(path: String, format: String): Dataset[Row] = {
    format match {
      case "csv" => readCsv[Row](path)
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
}