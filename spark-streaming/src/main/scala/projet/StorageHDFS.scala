package projet

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StorageHDFS {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("StorageHDFS")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val jsonSchema = new StructType()
      .add("timestamp",     StringType)
      .add("level",         StringType)
      .add("source",        StringType)
      .add("message",       StringType)
      .add("ip",            StringType)
      .add("user_agent",    StringType)
      .add("response_time", IntegerType)
      .add("status_code",   IntegerType)

    val apacheRegex =
      """^(\S+) \S+ (\S+) \[([^\]]+)\] "(\S+) (\S+) \S+" (\d{3}) (\d+)$"""
    val syslogRegex =
      """^(\d{4}\s+\w+\s+\d+\s+\S+) (\S+) (\w+)\[(\d+)\]: (.+) from (\S+)$"""

    val rawDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:29092")
      .option("subscribe", "web-logs,auth-logs,microservices-logs")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as raw", "topic")

    val jsonDF = rawDF
      .filter(col("topic") === "web-logs")
      .select(from_json(col("raw"), jsonSchema).as("data"))
      .select(
        try_to_timestamp(col("data.timestamp"), lit("yyyy-MM-dd HH:mm:ss")).as("timestamp"),
        col("data.level"),
        lit("web").as("source"),
        col("data.message"),
        col("data.ip"),
        col("data.user_agent"),
        col("data.response_time"),
        col("data.status_code")
      )

    val apacheDF = rawDF
      .filter(col("topic") === "microservices-logs")
      .select(
        regexp_extract(col("raw"), apacheRegex, 1).as("ip"),
        regexp_extract(col("raw"), apacheRegex, 3).as("timestamp_str"),
        regexp_extract(col("raw"), apacheRegex, 4).as("method"),
        regexp_extract(col("raw"), apacheRegex, 5).as("endpoint"),
        regexp_extract(col("raw"), apacheRegex, 6).as("status_code_str"),
        regexp_extract(col("raw"), apacheRegex, 7).as("response_time_str")
      )
      .select(
        try_to_timestamp(col("timestamp_str"), lit("dd/MMM/yyyy:HH:mm:ss Z")).as("timestamp"),
        when(col("status_code_str") >= "500", "ERROR")
          .when(col("status_code_str") >= "400", "WARN")
          .otherwise("INFO").as("level"),
        lit("microservices").as("source"),
        concat(col("method"), lit(" "), col("endpoint")).as("message"),
        col("ip"),
        lit("").as("user_agent"),
        col("response_time_str").cast(IntegerType).as("response_time"),
        col("status_code_str").cast(IntegerType).as("status_code")
      )

    val syslogDF = rawDF
      .filter(col("topic") === "auth-logs")
      .select(
        regexp_extract(col("raw"), syslogRegex, 1).as("timestamp_str"),
        regexp_extract(col("raw"), syslogRegex, 5).as("message"),
        regexp_extract(col("raw"), syslogRegex, 6).as("ip")
      )
      .select(
        try_to_timestamp(col("timestamp_str"), lit("yyyy MMM dd HH:mm:ss")).as("timestamp"),
        when(col("message").contains("Failed"), "ERROR")
          .when(col("message").contains("FAILED"), "ERROR")
          .otherwise("INFO").as("level"),
        lit("auth").as("source"),
        col("message"),
        col("ip"),
        lit("").as("user_agent"),
        lit(0).as("response_time"),
        lit(0).as("status_code")
      )

    val unifiedDF = jsonDF
      .union(apacheDF)
      .union(syslogDF)
      .withColumn("date", to_date(col("timestamp")))
      .withColumn("year",  year(col("timestamp")))
      .withColumn("month", month(col("timestamp")))
      .withColumn("day",   dayofmonth(col("timestamp")))

    // Écriture Parquet sur HDFS partitionné par date et source
    val hdfsQuery = unifiedDF.writeStream
      .format("parquet")
      .option("path", "hdfs://namenode:9000/logs/parquet/")
      .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/storage/")
      .partitionBy("date", "source")
      .outputMode("append")
      .start()

    // Afficher dans la console pour vérification
    val consoleQuery = unifiedDF.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("append")
      .queryName("console_output")
      .start()

    spark.streams.awaitAnyTermination()
  }
}