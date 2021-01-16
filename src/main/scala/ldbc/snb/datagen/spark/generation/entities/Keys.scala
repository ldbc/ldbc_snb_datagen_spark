package ldbc.snb.datagen.spark.generation.entities

import ldbc.snb.datagen.spark.generation.entities.dynamic.person.Person

object Keys {

  implicit class PersonByRandomId(val self: Person) extends AnyVal {
    def byRandomId = (self.getRandomId, self.getAccountId)
  }

  implicit class PersonByUni(val self: Person) extends AnyVal {
    def byUni = (self.getUniversityLocationId, self.getAccountId)
  }

  implicit class PersonByInterest(val self: Person) extends AnyVal {
    def byInterest = (self.getMainInterest, self.getAccountId)
  }

  implicit def orderingForPair[A: Ordering, B: Ordering]: Ordering[(A, B)] = new Ordering[(A, B)] {

    override def compare(x: (A, B), y: (A, B)): Int = (x, y) match {
      case ((x1, x2), (y1, y2)) =>
        val d1 = implicitly[Ordering[A]].compare(x1, y1)
        if (d1 == 0) implicitly[Ordering[B]].compare(x2, y2) else d1
    }
  }
}
