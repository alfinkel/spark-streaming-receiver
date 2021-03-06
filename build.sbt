name := "spark-mh-receiver"
 
version := "1.6"
 
scalaVersion := "2.10.4"

libraryDependencies ++= {
  val sparkVersion =  "1.6.0"
  Seq(
    "org.apache.spark" %%  "spark-core"	  %  sparkVersion % "provided",
    "org.apache.spark" %%  "spark-streaming"	  %  sparkVersion % "provided",
    "org.apache.kafka" % "kafka-log4j-appender" % "0.9.0.0",
    "org.apache.kafka" % "kafka-clients" % "0.9.0.0",
    "org.apache.kafka" %% "kafka" % "0.9.0.0"
  )
}

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", xs @ _*) => MergeStrategy.first
  case PathList("scala", xs @ _*) => MergeStrategy.discard
  case PathList("META-INF", "maven", "org.slf4j", xs @ _* ) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

unmanagedBase <<= baseDirectory { base => base / "lib" }

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
