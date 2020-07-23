name := "ICP13"

version := "0.1"

scalaVersion := "2.11.12"


resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-mllib" % "2.4.4" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-graphx" % "2.4.4",
  "graphframes" % "graphframes" % "0.8.0-spark2.4-s_2.11"
)
