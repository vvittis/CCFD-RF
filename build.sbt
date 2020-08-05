name := "CCFD"

version := "0.1"

scalaVersion := "2.12.3"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.0"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.6.0"
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.6.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.6.0"


