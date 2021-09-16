package ldbc.snb.datagen.entities

object raw {

  case class Person(
                          creationDate: Long,
                          deletionDate: Long,
                          explicitlyDeleted: Boolean,
                          id: Long,
                          firstName: String,
                          lastName: String,
                          `gender`: String,
                          `birthday`: Long,
                          `locationIP`: String,
                          `browserUsed`: String,
                          `place`: Integer,
                          `language`: Seq[String],
                          `email`: Seq[String]
                        )

  case class PersonHasInterestTag(
                                        creationDate: Long,
                                        deletionDate: Long,
                                        personId: Long,
                                        interestIdx: Int
                                      )

  case class PersonKnowsPerson(
                                     creationDate: Long,
                                     deletionDate: Long,
                                     explicitlyDeleted: Boolean,
                                     `Person1.Id`: Long,
                                     `Person2.Id`: Long
                                   )

  case class PersonStudyAtUniversity(
                                           creationDate: Long,
                                           deletionDate: Long,
                                           explicitlyDeleted: Boolean,
                                           `Person.id`: Long,
                                           `Organisation.id`: Long,
                                           classYear: Int
                                         )

  case class PersonWorkAtCompany(

                                  creationDate: Long,
                                  deletionDate: Long,
                                  explicitlyDeleted: Boolean,
                                  `Person.id`: Long,
                                  `Organisation.id`: Long,
                                  workFrom: Int
                                )

}
