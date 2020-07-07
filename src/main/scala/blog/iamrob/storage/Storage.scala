package blog.iamrob.storage

import org.apache.spark.sql.{Encoder, SparkSession, Dataset}
import org.apache.spark.sql.Row

trait Storage {
  def read(path: String): Dataset[Row]
  def write(ds: Dataset[_], path: String)
}

class LocalStorage(spark: SparkSession) extends Storage {
  import spark.implicits._

  private def readCsv[T](path: String) = {

    spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv(path)
  }

  override def read(path: String): Dataset[Row] = readCsv[Row](path)

  override def write(ds: Dataset[_], path: String): Unit = ds.show(false)
}
