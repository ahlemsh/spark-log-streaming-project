package projet

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Aggregations {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Aggregations")
      .master("spark://spark-master:7077")
      .config("spark.sql.shuffle.partitions", "6")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val jsonSchema = new StructType()
      .add("timestamp", StringType)
      .add("level", StringType)
      .add("source", StringType)
      .add("message", StringType)
      .add("ip", StringType)
      .add("user_agent", StringType)
      .add("response_time", IntegerType)
      .add("status_code", IntegerType)

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
      .selectExpr(
        "CAST(value AS STRING) as raw",
        "topic"
      )

    val jsonDF = rawDF
      .filter(col("topic") === "web-logs")
      .select(from_json(col("raw"), jsonSchema).as("data"))
      .select(
        to_timestamp(col("data.timestamp"), "yyyy-MM-dd HH:mm:ss").as("timestamp"),
        col("data.level"),
        lit("web").as("source"),
        col("data.response_time"),
        col("data.status_code")
      )

    val apacheDF = rawDF
      .filter(col("topic") === "microservices-logs")
      .select(
        regexp_extract(col("raw"), apacheRegex, 3).as("timestamp_str"),
        regexp_extract(col("raw"), apacheRegex, 6).as("status_code_str"),
        regexp_extract(col("raw"), apacheRegex, 7).as("response_time_str")
      )
      .select(
        to_timestamp(col("timestamp_str"), "dd/MMM/yyyy:HH:mm:ss Z").as("timestamp"),
        when(col("status_code_str").cast(IntegerType) >= 500, "ERROR")
          .when(col("status_code_str").cast(IntegerType) >= 400, "WARN")
          .otherwise("INFO").as("level"),
        lit("microservices").as("source"),
        col("response_time_str").cast(IntegerType).as("response_time"),
        col("status_code_str").cast(IntegerType).as("status_code")
      )

    val syslogDF = rawDF
      .filter(col("topic") === "auth-logs")
      .select(
        regexp_extract(col("raw"), syslogRegex, 1).as("timestamp_str"),
        regexp_extract(col("raw"), syslogRegex, 5).as("message")
      )
      .select(
        to_timestamp(col("timestamp_str"), "yyyy MMM dd HH:mm:ss").as("timestamp"),
        when(col("message").contains("Failed"), "ERROR")
          .when(col("message").contains("FAILED"), "ERROR")
          .otherwise("INFO").as("level"),
        lit("auth").as("source"),
        lit(0).as("response_time"),
        lit(0).as("status_code")
      )

    val unifiedDF = jsonDF
      .union(apacheDF)
      .union(syslogDF)
      .filter(col("timestamp").isNotNull)

    val requestsPerMinute = unifiedDF
      .groupBy(window(col("timestamp"), "1 minute"), col("source"))
      .agg(count("*").as("total_requests"))
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("source"),
        col("total_requests")
      )

    val errorsPerMinute = unifiedDF
      .filter(col("level") === "ERROR")
      .groupBy(window(col("timestamp"), "1 minute"), col("source"))
      .agg(count("*").as("error_count"))
      .select(
        col("window.start").as("window_start"),
        col("source"),
        col("error_count")
      )

    val errorRatePerMinute = unifiedDF
      .groupBy(window(col("timestamp"), "1 minute"), col("source"))
      .agg(
        count("*").as("total"),
        sum(when(col("level") === "ERROR", 1).otherwise(0)).as("errors")
      )
      .select(
        col("window.start").as("window_start"),
        col("source"),
        col("total"),
        col("errors"),
        round((col("errors") / col("total") * 100), 2).as("error_rate_pct")
      )

    val latencyPerMinute = unifiedDF
      .filter(col("source") =!= "auth")
      .groupBy(window(col("timestamp"), "1 minute"), col("source"))
      .agg(
        avg("response_time").as("avg_response_time"),
        max("response_time").as("max_response_time"),
        min("response_time").as("min_response_time")
      )
      .select(
        col("window.start").as("window_start"),
        col("source"),
        round(col("avg_response_time"), 2).as("avg_ms"),
        col("max_response_time").as("max_ms"),
        col("min_response_time").as("min_ms")
      )

    val checkpointBase = "hdfs://namenode:9000/checkpoints/aggregations"

    val q1 = requestsPerMinute.writeStream
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", s"$checkpointBase/requests_per_minute")
      .outputMode("complete")
      .queryName("requests_per_minute")
      .start()

    val q2 = errorsPerMinute.writeStream
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", s"$checkpointBase/errors_per_minute")
      .outputMode("complete")
      .queryName("errors_per_minute")
      .start()

    val q3 = errorRatePerMinute.writeStream
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", s"$checkpointBase/error_rate")
      .outputMode("complete")
      .queryName("error_rate")
      .start()

    val q4 = latencyPerMinute.writeStream
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", s"$checkpointBase/latency")
      .outputMode("complete")
      .queryName("latency")
      .start()

    spark.streams.awaitAnyTermination()
  }
}