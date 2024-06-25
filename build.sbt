import sbt.librarymanagement.InclExclRule

val awsSdkVersion = "1.11.728"
val sparkVersion = "3.1.2"
val dynamodbStreamsKinesisAdapterVersion = "1.5.2"

inThisBuild(
  List(
    organization := "com.scylladb",
    scalaVersion := "2.12.11",
    scalacOptions += "-target:jvm-1.8"
  )
)

// Augmentation of spark-streaming-kinesis-asl to also work with DynamoDB Streams
lazy val `spark-kinesis-dynamodb` = project.in(file("spark-kinesis-dynamodb")).settings(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-streaming-kinesis-asl"     % sparkVersion,
    "com.amazonaws"    % "dynamodb-streams-kinesis-adapter" % dynamodbStreamsKinesisAdapterVersion
  )
)

lazy val migrator = (project in file("migrator")).settings(
  name      := "scylla-migrator",
  version   := "0.0.1",
  mainClass := Some("com.scylladb.migrator.Migrator"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  javaOptions ++= Seq(
    "-Xms512M",
    "-Xmx2048M",
    "-XX:MaxPermSize=2048M",
    "-XX:+CMSClassUnloadingEnabled"),
  scalacOptions ++= Seq("-deprecation", "-unchecked", "-Ypartial-unification"),
  Test / parallelExecution := false,
  fork                     := true,
  scalafmtOnCompile        := true,
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-streaming"      % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql"            % sparkVersion % "provided",
    "com.amazonaws"    % "aws-java-sdk-dynamodb" % awsSdkVersion,
    "com.amazonaws"    % "aws-java-sdk-s3"       % awsSdkVersion,
    "com.amazonaws"    % "aws-java-sdk-sts"      % awsSdkVersion,
    ("com.amazonaws" % "dynamodb-streams-kinesis-adapter" % dynamodbStreamsKinesisAdapterVersion)
      .excludeAll(InclExclRule("com.fasterxml.jackson.core")),
    ("com.amazon.emr" % "emr-dynamodb-hadoop" % "5.0.0")
      .excludeAll(
        InclExclRule(organization = "software.amazon.awssdk"),
        InclExclRule(organization = "com.fasterxml.jackson.core"),
        InclExclRule(organization = "io.netty"),
        InclExclRule(organization = "org.apache.commons")
      ),
    "io.circe"       %% "circe-generic"      % "0.11.1",
    "io.circe"       %% "circe-parser"       % "0.11.1",
    "io.circe"       %% "circe-yaml"         % "0.10.1",
  ),
  assembly / assemblyShadeRules := Seq(
    ShadeRule.rename("org.yaml.snakeyaml.**" -> "com.scylladb.shaded.@1").inAll
  ),
  assembly / assemblyMergeStrategy := {
    // Handle conflicts between our own library dependencies and those that are bundled into
    // the spark-cassandra-connector fat-jar
    case PathList("com", "codahale", "metrics", _ @_*)                => MergeStrategy.first
    case PathList("digesterRules.xml")                                => MergeStrategy.first
    case PathList("org", "aopalliance", _ @_*)                        => MergeStrategy.first
    case PathList("org", "apache", "commons", "collections", _ @_*)   => MergeStrategy.first
    case PathList("org", "apache", "commons", "configuration", _ @_*) => MergeStrategy.first
    case PathList("org", "apache", "commons", "logging", _ @_*)       => MergeStrategy.first
    case PathList("org", "apache", "spark", _ @_*)                    => MergeStrategy.first
    case PathList("org", "slf4j", _ @_*)                              => MergeStrategy.first
    case PathList("com", "fasterxml", "jackson", _ @_*)               => MergeStrategy.first
    case PathList("io", "netty", _ @_*)                               => MergeStrategy.first
    case PathList("org", "apache", "commons", _ @_*)                  => MergeStrategy.first
    case PathList("org", "apache", "http", _ @_*)                     => MergeStrategy.first
    case PathList("mime.types")                                       => MergeStrategy.first
    case PathList("properties.dtd")                                   => MergeStrategy.first
    case PathList("PropertyList-1.0.dtd")                             => MergeStrategy.first
    case d if d.endsWith("public-suffix-list.txt") => MergeStrategy.first
    case d if d.endsWith("native-image.properties") => MergeStrategy.first
    case d if d.endsWith("reflection-config.json") => MergeStrategy.first
    case d if d.endsWith("native-image.properties") => MergeStrategy.first
    case d if d.endsWith("io.netty.versions.properties") => MergeStrategy.first
    case d if d.endsWith(".jar:jetty-dir.css") => MergeStrategy.first
    case d if d.endsWith("jetty-dir.css") => MergeStrategy.first
    case d if d.endsWith(".jar:module-info.class") => MergeStrategy.first
    case d if d.endsWith("module-info.class") => MergeStrategy.first
    // Other conflicts
    case PathList("javax", "inject", _ @_*)         => MergeStrategy.first
    case PathList("org", "apache", "hadoop", _ @_*) => MergeStrategy.first
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  },
  // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
  Compile / run := Defaults
    .runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner)
    .evaluated,
  scalacOptions ++= Seq("-deprecation", "-unchecked"),
  pomIncludeRepository := { x =>
    false
  },
  pomIncludeRepository := { x =>
    false
  },
  // publish settings
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }
).dependsOn(`spark-kinesis-dynamodb`)

lazy val tests = project.in(file("tests")).settings(
  libraryDependencies ++= Seq(
    "com.amazonaws"            % "aws-java-sdk-dynamodb"     % awsSdkVersion,
    "org.apache.cassandra"     % "java-driver-query-builder" % "4.18.0",
    "com.github.mjakubowski84" %% "parquet4s-core"           % "1.9.4",
    "org.apache.hadoop"        % "hadoop-client"             % "3.2.0",
    "org.scalameta"            %% "munit"                    % "0.7.29",
    "org.scala-lang.modules"   %% "scala-collection-compat"  % "2.11.0"
  ),
  Test / parallelExecution := false
).dependsOn(migrator)

lazy val root = project.in(file("."))
  .aggregate(migrator, `spark-kinesis-dynamodb`, tests)
