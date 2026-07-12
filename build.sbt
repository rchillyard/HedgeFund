name := "HedgeFund"

version := "1.0"

scalaVersion := "3.3.8"

val akkaGroup = "com.typesafe.akka"
val akkaVersion = "2.8.8"
val akkaHttpVersion = "10.5.3"
val sprayJsonVersion = "1.3.6"
val scalaTestVersion = "3.2.20"

libraryDependencies ++= Seq(
  akkaGroup %% "akka-actor-typed" % akkaVersion,
  akkaGroup %% "akka-actor-testkit-typed" % akkaVersion % "test",
  akkaGroup %% "akka-slf4j" % akkaVersion,
  akkaGroup %% "akka-http" % akkaHttpVersion,
  akkaGroup %% "akka-stream" % akkaVersion,
  "io.spray" %% "spray-json" % sprayJsonVersion,
  "org.scala-lang.modules" %% "scala-parser-combinators" % "2.4.0",
  "org.scala-lang.modules" %% "scala-xml" % "2.4.0",
  "com.typesafe" % "config" % "1.4.9",
  "com.github.nscala-time" %% "nscala-time" % "3.0.0",
  "ch.qos.logback" % "logback-core" % "1.5.37",
  "ch.qos.logback" % "logback-classic" % "1.5.37" % "runtime",
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
)