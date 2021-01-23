package ldbc.snb.datagen.util

import org.apache.spark.sql.SparkSession

trait SparkTesting {
  implicit lazy val spark: SparkSession = GlobalSparkTesting.spark
}

object GlobalSparkTesting {
  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("test")
    .config("spark.driver.extraJavaOptions", "-XX:+CMSClassUnloadingEnabled")
    .config("spark.ui.enabled", "false")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
}
