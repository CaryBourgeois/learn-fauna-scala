name := "learn-fauna-scala"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies += "com.faunadb" % "faunadb-scala_2.12" % "2.2.0"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.3"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.3"
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.18.0"

// These are here to support logging and get rid of the ugly SLF4J error messages
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.5"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.5"
libraryDependencies += "org.clapper" %% "grizzled-slf4j" % "1.3.2"