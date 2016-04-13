name := """scala-pgq"""

organization := "com.github.jtjeferreira"

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html"))

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.scalikejdbc"     %% "scalikejdbc-async" % "0.5.+",
  "com.github.mauricio" %% "postgresql-async"  % "0.2.15",
  
  "postgresql" % "postgresql" % "9.1-901.jdbc4",
  "com.typesafe.akka" %% "akka-actor" % "2.4.3",
  "com.typesafe.akka" %% "akka-stream" % "2.4.3",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.4.3",
  
  "org.scalatest" %% "scalatest" % "2.1.6" % "test",
  "com.typesafe.akka" %% "akka-slf4j" % "2.4.3",
  "ch.qos.logback" % "logback-classic" % "1.1.3" % "test"
)

fork := true
