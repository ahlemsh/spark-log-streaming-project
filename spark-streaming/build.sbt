name := "log-streaming"
version := "1.0"
scalaVersion := "2.13.16"

libraryDependencies ++= Seq(
  "org.apache.spark"    %% "spark-core"           % "4.0.2" % "provided",
  "org.apache.spark"    %% "spark-sql"            % "4.0.2" % "provided",
  "org.apache.spark"    %% "spark-sql-kafka-0-10" % "4.0.2",
  "org.postgresql"       % "postgresql"           % "42.6.0",
  "com.maxmind.geoip2"   % "geoip2"              % "4.2.0",
  "com.github.ua-parser" % "uap-java"            % "1.6.1"
)

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core"    % "jackson-core"            % "2.18.2",
  "com.fasterxml.jackson.core"    % "jackson-databind"        % "2.18.2",
  "com.fasterxml.jackson.core"    % "jackson-annotations"     % "2.18.2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala"    % "2.18.2"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", xs @ _*)             => MergeStrategy.discard
  case PathList("com", "fasterxml", xs @ _*)     => MergeStrategy.first
  case _                                          => MergeStrategy.first
}