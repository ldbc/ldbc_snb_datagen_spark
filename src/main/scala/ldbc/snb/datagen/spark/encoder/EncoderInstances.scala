package ldbc.snb.datagen.spark.encoder

import ldbc.snb.datagen.model.legacy.{Activity, Person}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.types.{DataType, IntegerType, ObjectType}
import shapeless.{Witness, cachedImplicit}

import scala.reflect.ClassTag

trait EncoderInstances {
  import frameless._

  implicit def typedEncoderForEnum[A <: Enumeration](implicit w: Witness.Aux[A]): TypedEncoder[A#Value] = new TypedEncoder[A#Value]() {
    override def nullable: Boolean = true

    override def jvmRepr: DataType = ObjectType(classOf[A#Value])

    override def catalystRepr: DataType = TypedEncoder[Byte].catalystRepr

    override def fromCatalyst(path: Expression): Expression =
      StaticInvoke(w.value.getClass, jvmRepr, "apply", Seq(path))

    override def toCatalyst(path: Expression): Expression =
      Invoke(path, "id", IntegerType, Nil, returnNullable = false)
  }

  implicit def setToArrayInjection[A: ClassTag] = new Injection[Set[A], Array[A]] {
    override def apply(a: Set[A]): Array[A] = a.toArray
    override def invert(b: Array[A]): Set[A] = b.toSet
  }

  // Cache implicits for some frequently used entities for faster compile time.
  // Can be deleted safely, and is unnecessary for Scala 2.12+
  implicit val typedEncoderForPerson = cachedImplicit[TypedEncoder[Person]]
  implicit val typedEncoderForActivity = cachedImplicit[TypedEncoder[Activity]]
}
