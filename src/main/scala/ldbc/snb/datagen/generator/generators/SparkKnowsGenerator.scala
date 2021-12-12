package ldbc.snb.datagen.generator.generators

import ldbc.snb.datagen.entities.dynamic.person.Person

import java.util
import ldbc.snb.datagen.generator.{DatagenContext, DatagenParams}
import ldbc.snb.datagen.generator.generators.knowsgenerators.KnowsGenerator
import ldbc.snb.datagen.util.GeneratorConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.collection.SortedMap
import scala.reflect.ClassTag

object SparkKnowsGenerator {
  def apply(
      persons: RDD[Person],
      ranker: SparkRanker,
      conf: GeneratorConfiguration,
      percentages: Seq[Float],
      stepIndex: Int,
      knowsGeneratorClassName: String
  )(implicit spark: SparkSession) = {
    val blockSize = DatagenParams.blockSize

    val indexed = ranker(persons)
      .map { case (k, v) => (k / blockSize, (k, v)) }

    val percentagesJava = percentages.map(Float.box).asJava

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
        val knowsGenerator      = knowsGeneratorClass.getConstructor().newInstance().asInstanceOf[KnowsGenerator]
        knowsGenerator.initialize(conf)
        val personSimilarity = DatagenParams.getPersonSimularity

        val personGroups = for { (block, persons) <- groups } yield {
          val clonedPersons = new util.ArrayList[Person]
          for (p <- persons.values) {
            clonedPersons.add(new Person(p))
          }
          knowsGenerator.generateKnows(clonedPersons, block.toInt, percentagesJava, stepIndex, personSimilarity)
          clonedPersons
        }

        for {
          persons <- personGroups
          person  <- persons.iterator().asScala
        } yield person
      })
  }
}
