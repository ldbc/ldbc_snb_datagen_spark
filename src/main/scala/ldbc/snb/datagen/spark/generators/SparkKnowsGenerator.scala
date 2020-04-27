package ldbc.snb.datagen.spark.generators

import java.util

import ldbc.snb.datagen.{DatagenContext, DatagenParams}
import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.generator.generators.knowsgenerators.KnowsGenerator
import ldbc.snb.datagen.util.LdbcConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.collection.SortedMap
import scala.reflect.ClassTag

object SparkKnowsGenerator {
  def apply[K: Ordering: ClassTag](
    persons: RDD[Person],
    conf: LdbcConfiguration,
    percentages: Seq[Float],
    stepIndex: Int,
    sortBy: Person => K,
    knowsGeneratorClassName: String,
    numPartitions: Option[Int] = None
  )(implicit spark: SparkSession) = {
    val partitions = numPartitions.getOrElse(spark.sparkContext.defaultParallelism)
    val blockSize = DatagenParams.blockSize

    val sortedPersons = persons.sortBy(sortBy, numPartitions = partitions).cache()

    // single count / partition. Assumed small enough to collect and broadcast
    val counts = sortedPersons
      .mapPartitionsWithIndex((i, ps) => Seq((i, ps.length.toLong)).iterator)
      .collectAsMap()

    val aggregatedCounts = SortedMap(counts.toSeq : _*)
      .foldLeft((0L, Map.empty[Int, Long])) {
        case ((total, map), (i, c)) => (total + c, map + (i -> total))
      }
      ._2

    val broadcastedCounts = spark.sparkContext.broadcast(aggregatedCounts)

    val indexed = sortedPersons.mapPartitionsWithIndex((i, ps) => {
      val start = broadcastedCounts.value(i)
      for { (p, j) <- ps.zipWithIndex } yield ((start + j) / blockSize, (start + j, p))
    })

    val percentagesJava = percentages.map(new java.lang.Float(_)).asJava

    indexed
      // groupByKey wouldn't guarantee keeping the order inside groups
      // TODO check if it actually has better performance than sorting inside mapPartitions (probably not)
      .combineByKeyWithClassTag(
          personByRank => SortedMap(personByRank),
          (map: SortedMap[Long, Person], personByRank) => map + personByRank,
          (a: SortedMap[Long, Person], b: SortedMap[Long, Person]) => a ++ b
        )
      .mapPartitions(groups => {
        DatagenContext.initialize(conf)
        val knowsGeneratorClass = Class.forName(knowsGeneratorClassName)
        val knowsGenerator = knowsGeneratorClass.newInstance().asInstanceOf[KnowsGenerator]
        knowsGenerator.initialize(conf)
        val personSimilarity = DatagenParams.getPersonSimularity

        val personGroups = for { (block, persons) <- groups } yield {
          val clonedPersons = new util.ArrayList[Person]
          for (p <- persons.values) {
            clonedPersons.add(p.clone())
          }
          knowsGenerator.generateKnows(clonedPersons, block.toInt, percentagesJava, stepIndex, personSimilarity)
          clonedPersons
        }

        for { 
          persons <- personGroups
          person <- persons.iterator().asScala
        } yield person
      })
  }
}
