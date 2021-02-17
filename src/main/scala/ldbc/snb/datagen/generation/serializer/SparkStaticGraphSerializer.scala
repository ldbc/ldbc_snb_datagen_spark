package ldbc.snb.datagen.generation.serializer

import ldbc.snb.datagen.hadoop.serializer.HadoopStaticSerializer
import ldbc.snb.datagen.util.GeneratorConfiguration
import org.apache.spark.sql.SparkSession

object SparkStaticGraphSerializer {

  def apply(config: GeneratorConfiguration, partitions: Some[Int])(implicit spark: SparkSession): Unit = {
    val serializer = new HadoopStaticSerializer(
      config,
      spark.sparkContext.hadoopConfiguration,
      1
    )
    serializer.run()
  }
}
