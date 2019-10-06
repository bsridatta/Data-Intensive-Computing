scalaVersion := "2.11.8"
name := "Lab2Graphs"

val sparkVersion = "1.6.0"
val spark = "org.apache.spark"

libraryDependencies ++= Seq(
  spark %% "spark-core" % sparkVersion,
  spark %% "spark-graphx" % sparkVersion
)

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.0"