name := "kafka-hdfs-zoomdata"

version := "1.0"

scalaVersion := "2.11.8"

autoScalaLibrary := false

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.1.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.1"

libraryDependencies +=  "net.databinder.dispatch" %% "dispatch-core" % "0.11.2"

libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.3.0"

libraryDependencies += "commons-lang" % "commons-lang" % "2.6"