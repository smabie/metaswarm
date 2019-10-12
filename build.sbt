name := "metaswarm"

version := "0.1"

scalaVersion := "2.12.6"

libraryDependencies ++= Seq(
  "com.beachape" %% "enumeratum" % "1.5.13",
  "com.lihaoyi" %% "fastparse" % "1.0.0",
  "com.lihaoyi" %% "fastparse-byte" % "1.0.0",
  "org.scalaz" %% "scalaz-core" % "7.2.27",
  "com.github.pathikrit" %% "better-files" % "3.6.0",
  "com.github.pathikrit" %% "better-files-akka" % "3.6.0",
  "com.typesafe.akka" %% "akka-actor" % "2.5.23",
  "com.typesafe.akka" %% "akka-stream" % "2.5.23",
  "com.github.nscala-time" %% "nscala-time" % "2.22.0",
  "org.xerial.larray" %% "larray" % "0.4.1",
  "com.storm-enroute" %% "scalameter" % "0.18"
)

testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
parallelExecution in Test := false

//enablePlugins(GraalVMNativeImagePlugin)