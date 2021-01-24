package ldbc.snb.datagen

import org.apache.spark.sql.SparkSession

trait SparkApp {
  def appName: String

  implicit def spark = SparkSession
    .builder()
    .appName(appName)
    .getOrCreate()

}
