val commonSettings = Seq(
  organization := "com.github.jtjeferreira",
  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  scalaVersion := "2.11.7",
  scalacOptions ++= Seq(
    "-feature",
    "-language:postfixOps"
  ),
  fork := true,
  parallelExecution in Global := false,
  cancelable in Global := true
)

lazy val root =  project.in( file(".") )
  .settings(commonSettings)
  .aggregate(core, scalikejdbc, `scalikejdbc-async`, slick)

lazy val core = project
  .settings(commonSettings)
  .settings(Seq(
    name := "scala-pgq-core",
    libraryDependencies ++= Seq(
	  "com.typesafe.akka" %% "akka-actor" % "2.4.3",
	  "com.typesafe.akka" %% "akka-stream" % "2.4.3",
	  "com.typesafe.akka" %% "akka-stream-testkit" % "2.4.3",
	  
	  "joda-time" % "joda-time" % "2.6",
	  "org.joda" % "joda-convert" % "1.8",
	  
	  "org.scalatest" %% "scalatest" % "2.1.6" % "test",
	  "com.typesafe.akka" %% "akka-slf4j" % "2.4.3",
	  "ch.qos.logback" % "logback-classic" % "1.1.3" % "test"
	)
))

lazy val scalikejdbc = project
  .dependsOn(core % "test->test;compile->compile")
  .settings(commonSettings)
  .settings(Seq(
    name := "scala-pgq-scalikejdbc",
    libraryDependencies ++= Seq(
	  "org.scalikejdbc"     %% "scalikejdbc" % "2.3.5",
	  "postgresql" % "postgresql" % "9.1-901.jdbc4"
	)
))

lazy val `scalikejdbc-async` = project
  .dependsOn(core % "test->test;compile->compile")
  .settings(commonSettings)
  .settings(Seq(
    name := "scala-pgq-scalikejdbc-async",
    libraryDependencies ++= Seq(
	  "org.scalikejdbc"     %% "scalikejdbc-async" % "0.5.5",
	  "com.github.mauricio" %% "postgresql-async"  % "0.2.15"
	)
))

lazy val slick = project
  .dependsOn(core % "test->test;compile->compile")
  .settings(commonSettings)
  .settings(Seq(
    name := "scala-pgq-slick",
    libraryDependencies ++= Seq(
	  "com.typesafe.slick" %% "slick" % "3.1.1",
	  "com.github.tminglei" %% "slick-pg_joda-time" % "0.12.1",
	  "postgresql" % "postgresql" % "9.1-901.jdbc4"
	)
))


