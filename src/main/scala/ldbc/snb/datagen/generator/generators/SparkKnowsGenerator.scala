package ldbc.snb.datagen.generator.generators

import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.generator.generators.knowsgenerators.KnowsGenerator
import ldbc.snb.datagen.generator.{DatagenContext, DatagenParams}
import ldbc.snb.datagen.util.GeneratorConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

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

    val create: ((Long, Person)) => mutable.SortedMap[Long, Person] =
      value => mutable.SortedMap(value)
    val merge: (mutable.SortedMap[Long, Person], (Long, Person)) => mutable.SortedMap[Long, Person] =
      (map, value) => map += value
    val combine: (mutable.SortedMap[Long, Person], mutable.SortedMap[Long, Person]) => mutable.SortedMap[Long, Person] =
      (a, b) => a ++= b

    indexed
      // groupByKey wouldn't guarantee keeping the order inside groups
      // TODO check if it actually has better performance than sorting inside mapPartitions (probably not)
      .combineByKeyWithClassTag[mutable.SortedMap[Long, Person]](create, merge, combine)
      .mapPartitions(groups => {
        DatagenContext.initialize(conf)
        val knowsGeneratorClass = Class.forName(knowsGeneratorClassName)
        val knowsGenerator      = knowsGeneratorClass.getConstructor().newInstance().asInstanceOf[KnowsGenerator]
        knowsGenerator.initialize(conf)
        val personSimilarity = DatagenParams.getPersonSimularity

        val personGroups = for { (block, persons) <- groups } yield {
          val personList = new util.ArrayList[Person](persons.size)
          for (p <- persons.values) { personList.add(p) }
          knowsGenerator.generateKnows(personList, block.toInt, percentagesJava, stepIndex, personSimilarity)
          personList
        }
        for {
          persons <- personGroups
          person  <- persons.iterator().asScala
        } yield person
      })
  }
}
