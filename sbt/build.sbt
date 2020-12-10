name := "StructuredRandomForest"

version := "0.1"

scalaVersion := "2.11.8"

//dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.2" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.3.2" % "provided",
  "org.apache.kafka" %% "kafka" % "2.0.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.0.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.0.2"
)


assemblyMergeStrategy in assembly := {
	case PathList("org","apache", xs @ _*) => MergeStrategy.first
 	case x =>
 		val oldStategy = (assemblyMergeStrategy in assembly).value
 		oldStategy(x)
}


/*assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 	case x => MergeStrategy.last
}*/

// sbt assembly
//c:\spark\bin\spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --num-executors 2 StructuredRandomForest-assembly-0.1.jar
//c:\spark\bin\spark-submit --num-executors 2 StructuredRandomForest-assembly-0.1.jar
//Works also with 2.3.0 version on kafka (depend on AppData\Coursier\repo1\maven\https\v1\cache\......\kafka\..)