name := "ldbc_snb_datagen"
organization := "ldbc.snb.datagen"

scmInfo := {
  val org = "ldbc"
  val project = "ldbc_snb_datagen_spark"
  Some(ScmInfo(url(s"https://github.com/$org/$project"), s"git@github.com:$org/$project.git"))
}

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / cancelable := true
ThisBuild / Test / parallelExecution := false
ThisBuild / Test / fork := true

val sparkVersion = settingKey[String]("The version of Spark used for building.")
val sparkCompatVersion = taskKey[String]("The compatibility version of Spark")
val platformVersion = taskKey[String]("The version of the target platform")

sparkVersion := "3.3.3"
sparkCompatVersion := { sparkVersion.value.split("\\.", 3).take(2).mkString(".") }
platformVersion := { scalaBinaryVersion.value + "_spark" + sparkCompatVersion.value }

resolvers += "TUDelft Repository" at "https://simulation.tudelft.nl/maven/"

libraryDependencies ++= Seq(
  "org.apache.spark" %%  "spark-sql" % sparkVersion.value % "provided",
  "org.apache.spark" %%  "spark-core" % sparkVersion.value % "provided",
  "org.apache.spark" %%  "spark-graphx" % sparkVersion.value % "provided",
  "com.chuusai" %%  "shapeless" % "2.3.3",
  "com.github.scopt" %%  "scopt" % "3.7.1",
  "org.javatuples" %  "javatuples" % "1.2",
  "ca.umontreal.iro" %  "ssj" % "2.5",
  "xml-apis" %  "xml-apis" % "1.4.01",
  "org.specs2" %%  "specs2-core" % "4.2.0" % Test,
  "org.specs2" %%  "specs2-junit" % "4.2.0" % Test,
  "org.mockito" %  "mockito-core" % "3.3.3" % Test,
  "org.scalatest"     %% "scalatest"   % "3.1.0" % Test withSources(),
  "junit"             %  "junit"       % "4.13.1"  % Test
)

scalaVersion := "2.12.16"

scalacOptions ++= Seq(
  "-deprecation",                      // Emit warning and location for usages of deprecated APIs.
  "-encoding", "utf-8",                // Specify character encoding used by source files.
  "-explaintypes",                     // Explain type errors in more detail.
  "-feature",                          // Emit warning and location for usages of features that should be imported explicitly.
  "-language:existentials",            // Existential types (besides wildcard types) can be written and inferred
  "-language:experimental.macros",     // Allow macro definition (besides implementation and application)
  "-language:higherKinds",             // Allow higher-kinded types
  "-language:implicitConversions",     // Allow definition of implicit functions called views
  "-unchecked",                        // Enable additional warnings where generated code depends on assumptions.
  "-Xcheckinit",                       // Wrap field accessors to throw an exception on uninitialized access.
  "-Xlint:adapted-args",               // Warn if an argument list is modified to match the receiver.
  "-Xlint:constant",                   // Evaluation of a constant arithmetic expression results in an error.
  "-Xlint:delayedinit-select",         // Selecting member of DelayedInit.
  "-Xlint:doc-detached",               // A Scaladoc comment appears to be detached from its element.
  "-Xlint:inaccessible",               // Warn about inaccessible types in method signatures.
  "-Xlint:infer-any",                  // Warn when a type argument is inferred to be `Any`.
  "-Xlint:missing-interpolator",       // A string literal appears to be missing an interpolator id.
  "-Xlint:nullary-override",           // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Xlint:nullary-unit",               // Warn when nullary methods return Unit.
  "-Xlint:option-implicit",            // Option.apply used implicit view.
  "-Xlint:package-object-classes",     // Class or object defined in package object.
  "-Xlint:poly-implicit-overload",     // Parameterized overloaded implicit methods are not visible as view bounds.
  "-Xlint:private-shadow",             // A private field (or class parameter) shadows a superclass field.
  "-Xlint:stars-align",                // Pattern sequence wildcard must align with sequence component.
  "-Xlint:type-parameter-shadow",      // A local type parameter shadows a type already in scope.
)

scalacOptions ++=
  scalaVersion {
    case sv if sv.startsWith("2.13") => List(
    )

    case sv if sv.startsWith("2.12") => List(
      "-Yno-adapted-args",                 // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver.
      "-Ypartial-unification",             // Enable partial unification in type constructor inference
      "-Ywarn-extra-implicit",             // Warn when more than one implicit parameter section is defined.
      "-Ywarn-inaccessible",               // Warn about inaccessible types in method signatures.
      "-Ywarn-infer-any",                  // Warn when a type argument is inferred to be `Any`.
      "-Ywarn-nullary-override",           // Warn when non-nullary `def f()' overrides nullary `def f'.
      "-Ywarn-nullary-unit",               // Warn when nullary methods return Unit.
      "-Ywarn-numeric-widen"               // Warn when numerics are widened.
    )

    case _ => Nil
  }.value

// The REPL can’t cope with -Ywarn-unused:imports or -Xfatal-warnings so turn them off for the console
Compile / console / scalacOptions --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings")

javacOptions ++= Seq(
  "-Xlint",
  "-source", "1.8",
  "-target", "1.8",
  "-g:vars"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

// Override JAR name
assembly / assemblyJarName := {
  moduleName.value + "_" + platformVersion.value + "-" + version.value + "-jar-with-dependencies.jar"
}

// Put under target instead of target/<scala-binary-version>
assembly / target := { target.value }
