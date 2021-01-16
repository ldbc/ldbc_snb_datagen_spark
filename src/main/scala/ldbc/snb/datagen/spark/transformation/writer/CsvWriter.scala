package ldbc.snb.datagen.spark.transformation.writer

import better.files._

import ldbc.snb.datagen.spark.transformation.model.{EntityType, Graph}
import ldbc.snb.datagen.spark.transformation.model.EntityType.{Attr, Edge, Node}
import ldbc.snb.datagen.spark.util.Utils.snake
import org.apache.spark.sql.DataFrame
import shapeless._

import scala.collection.immutable.TreeMap

case class CsvWriter(header: Boolean = false, separator: Char = '|') {

  implicit val cacheFriendlyOrdering = new Ordering[EntityType] {
    private val and = (a: Int, b: Int) => if (a == 0) b else a

    private object mapper extends Poly1 {
      implicit val atNode = at[Node](n => Seq(n.name.hashCode, 0, n.hashCode()))
      implicit val atEdge = at[Edge](e => {
        val primary = e match {
          case Edge("Likes", "Person", "Comment", _, _) => "Comment"
          case Edge("Likes", "Person", "Post", _, _) => "Post"
          case Edge("ContainerOf", "Forum", "Post", _, _) => "Post"
          case Edge(_, source, _, _, _) => source
        }
        Seq(primary.hashCode, 2, e.hashCode)
      })
      implicit val atAttr = at[Attr](a => Seq(a.parent.hashCode, 1, a.hashCode()))
    }

    private def orderingKeys(t: EntityType): Seq[Int] = Generic[EntityType].to(t).map(mapper).unify

    override def compare(x: EntityType, y: EntityType): Int =
      orderingKeys(x).zip(orderingKeys(y)).foldLeft(0)((a, v) => and(a, Ordering[Int].compare(v._1, v._2)))
  }

  def write(graph: Graph[DataFrame], outputDir: String) = {
    TreeMap(graph.entities.toSeq: _*).foreach {
      case (tpe, dataset) =>
        println(tpe)
        dataset.write

          .format("csv")
          .options(Map(
            "header" -> header.toString,
            "sep" -> separator.toString
          ))
          .save((outputDir / "csv" / snake(graph.layout) / tpe.entityPath).toString())
    }
  }
}