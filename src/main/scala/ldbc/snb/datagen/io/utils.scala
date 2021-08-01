package ldbc.snb.datagen.io

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.net.URI

object utils {

  def fileExists(path: String)(implicit spark: SparkSession): Boolean = {
    val hadoopPath = new Path(path)
    FileSystem.get(new URI(path), spark.sparkContext.hadoopConfiguration).exists(hadoopPath)
  }

}
