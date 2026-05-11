package projet

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object GrafanaSink {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("GrafanaSink")
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
        when(col("status_code_str").cast(IntegerType) >= 500, "ERROR")
          .when(col("status_code_str").cast(IntegerType) >= 400, "WARN")
          .otherwise("INFO").as("level"),
        lit("microservices").as("source"),
        concat(col("method"), lit(" "), col("endpoint")).as("message"),
        col("ip"),
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
        lit(0).as("response_time"),
        lit(0).as("status_code")
      )

    val unifiedDF = jsonDF
      .union(apacheDF)
      .union(syslogDF)
      .filter(col("timestamp").isNotNull)

    val ERROR_RATE_THRESHOLD = 30.0
    val LATENCY_THRESHOLD = 1000.0
    val HIGH_LOAD_THRESHOLD = 100
    val INTRUSION_THRESHOLD = 5

    def writeJdbc(df: DataFrame, table: String): Unit = {
      df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/logs_db")
        .option("dbtable", table)
        .option("user", "spark")
        .option("password", "spark123")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    }

    def writeToPostgres(batchDF: DataFrame, batchId: Long): Unit = {

      val metricsDF = batchDF
        .groupBy(window(col("timestamp"), "1 minute"), col("source"))
        .agg(
          count("*").as("total_requests"),
          sum(when(col("level") === "ERROR", 1).otherwise(0)).as("errors"),
          sum(when(col("level") === "WARN", 1).otherwise(0)).as("warnings"),
          round(sum(when(col("level") === "ERROR", 1).otherwise(0)) * 100.0 / count("*"), 2).as("error_rate_pct"),
          round(avg(when(col("response_time") > 0, col("response_time"))), 2).as("avg_response_ms")
        )
        .select(
          col("window.start").as("window_start"),
          col("source"),
          col("total_requests"),
          col("errors"),
          col("warnings"),
          col("error_rate_pct"),
          col("avg_response_ms")
        )

      if (!metricsDF.isEmpty) {
        writeJdbc(metricsDF, "metrics_per_minute")
      }

      val errorRateAlerts = metricsDF
        .filter(col("error_rate_pct") >= ERROR_RATE_THRESHOLD)
        .select(
          col("window_start").as("alert_time"),
          lit("TAUX D'ERREUR ÉLEVÉ").as("alert_type"),
          col("source"),
          concat(lit("Taux d'erreur : "), col("error_rate_pct"), lit("% sur "), col("source")).as("message"),
          col("error_rate_pct").as("value")
        )

      val latencyAlerts = metricsDF
        .filter(col("avg_response_ms").isNotNull && col("avg_response_ms") >= LATENCY_THRESHOLD)
        .select(
          col("window_start").as("alert_time"),
          lit("LATENCE ÉLEVÉE").as("alert_type"),
          col("source"),
          concat(lit("Latence moyenne : "), col("avg_response_ms"), lit(" ms sur "), col("source")).as("message"),
          col("avg_response_ms").as("value")
        )

      val highLoadAlerts = metricsDF
        .filter(col("total_requests") >= HIGH_LOAD_THRESHOLD)
        .select(
          col("window_start").as("alert_time"),
          lit("PIC DE CHARGE").as("alert_type"),
          col("source"),
          concat(lit("Pic de charge : "), col("total_requests"), lit(" requêtes/minute sur "), col("source")).as("message"),
          col("total_requests").cast("double").as("value")
        )

      val intrusionAlerts = batchDF
        .filter(col("source") === "auth")
        .filter(col("message").contains("Failed password"))
        .groupBy(window(col("timestamp"), "1 minute"), col("ip"))
        .agg(count("*").as("failed_attempts"))
        .filter(col("failed_attempts") >= INTRUSION_THRESHOLD)
        .select(
          col("window.start").as("alert_time"),
          lit("INTRUSION").as("alert_type"),
          lit("auth").as("source"),
          concat(lit("Tentatives d'intrusion : "), col("failed_attempts"), lit(" échecs depuis IP : "), col("ip")).as("message"),
          col("failed_attempts").cast("double").as("value")
        )

      val allAlerts = errorRateAlerts
        .unionByName(latencyAlerts)
        .unionByName(highLoadAlerts)
        .unionByName(intrusionAlerts)

      if (!allAlerts.isEmpty) {
        writeJdbc(allAlerts, "alerts")
      }
    }

    val query = unifiedDF.writeStream
      .foreachBatch(writeToPostgres _)
      .outputMode("update")
      .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/grafana-sink")
      .queryName("grafana_sink")
      .start()

    query.awaitTermination()
  }
}