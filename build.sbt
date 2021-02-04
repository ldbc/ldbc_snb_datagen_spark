ThisBuild / Test / parallelExecution := false
ThisBuild / Test / fork := true
ThisBuild / cancelable := true

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case x => MergeStrategy.first
}
