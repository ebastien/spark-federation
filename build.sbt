
enablePlugins(JavaAppPackaging)

val spark = Seq(
    "org.apache.spark" %% "spark-core" % "2.1.0",
    "org.apache.spark" %% "spark-sql" % "2.1.0"
  )

val http4s = Seq(
    "org.http4s" %% "http4s-blaze-client" % "0.15.8a",
    "org.http4s" %% "http4s-circe" % "0.15.8a",
    "io.circe" %% "circe-generic" % "0.6.1"
)

lazy val commonSettings = Seq(
    organization := "name.ebastien.spark",
    version := "0.1.0",
    scalaVersion := "2.11.8"
  )

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "federation",
    libraryDependencies ++= spark ++ http4s
  )

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfuture"
//  "-Ywarn-unused-import"
)
