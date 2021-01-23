package ldbc.snb.datagen.util

import org.apache.spark.sql.SparkSession


object SparkUI {

  def job[T](jobGroup: String, jobDescription: String)(action: => T)(implicit spark: SparkSession) = {
    spark.sparkContext.setJobGroup(jobGroup, jobDescription)
    action
  }
}
