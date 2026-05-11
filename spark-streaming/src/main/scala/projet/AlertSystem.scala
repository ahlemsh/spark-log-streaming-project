package projet

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object AlertSystem {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("AlertSystem")
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
        col("data.response_time"),
        col("data.status_code")
      )

    val apacheDF = rawDF
      .filter(col("topic") === "microservices-logs")
      .select(
        regexp_extract(col("raw"), apacheRegex, 1).as("ip"),
        regexp_extract(col("raw"), apacheRegex, 3).as("timestamp_str"),
        regexp_extract(col("raw"), apacheRegex, 6).as("status_code_str"),
        regexp_extract(col("raw"), apacheRegex, 7).as("response_time_str")
      )
      .select(
        try_to_timestamp(col("timestamp_str"), lit("dd/MMM/yyyy:HH:mm:ss Z")).as("timestamp"),
        when(col("status_code_str") >= "500", "ERROR")
          .when(col("status_code_str") >= "400", "WARN")
          .otherwise("INFO").as("level"),
        lit("microservices").as("source"),
        lit("").as("message"),
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
      .withColumn("date", to_date(col("timestamp")))

    // Seuils d'alertes 
    val ERROR_RATE_THRESHOLD    = 30.0  // % d'erreurs par minute → alerte
    val RESPONSE_TIME_THRESHOLD = 1000  // ms → alerte latence élevée
    val INTRUSION_THRESHOLD     = 5     // tentatives Failed password → alerte intrusion
    val HIGH_LOAD_THRESHOLD     = 100   // requêtes/minute → alerte pic de charge

    // Alerte 1 : Taux d'erreur élevé par minute
    val errorRateAlert = unifiedDF
      .groupBy(
        window(col("timestamp"), "1 minute"),
        col("source")
      )
      .agg(
        count("*").as("total"),
        count(when(col("level") === "ERROR", 1)).as("errors")
      )
      .withColumn("error_rate_pct",
        round(col("errors") / col("total") * 100, 2)
      )
      .filter(col("error_rate_pct") >= ERROR_RATE_THRESHOLD)
      .select(
        lit("ALERTE TAUX D'ERREUR").as("alert_type"),
        col("window.start").as("window_start"),
        col("source"),
        col("total"),
        col("errors"),
        col("error_rate_pct"),
        concat(
          lit("Taux d'erreur critique : "),
          col("error_rate_pct"),
          lit("% sur la source "),
          col("source")
        ).as("message")
      )

    // Alerte 2 : Latence élevée
    val latencyAlert = unifiedDF
      .filter(col("source") =!= "auth")
      .groupBy(
        window(col("timestamp"), "1 minute"),
        col("source")
      )
      .agg(
        avg("response_time").as("avg_response_time"),
        max("response_time").as("max_response_time")
      )
      .filter(col("avg_response_time") >= RESPONSE_TIME_THRESHOLD)
      .select(
        lit("ALERTE LATENCE ÉLEVÉE").as("alert_type"),
        col("window.start").as("window_start"),
        col("source"),
        round(col("avg_response_time"), 2).as("avg_ms"),
        col("max_response_time").as("max_ms"),
        concat(
          lit("Latence moyenne critique : "),
          round(col("avg_response_time"), 0),
          lit("ms sur la source "),
          col("source")
        ).as("message")
      )

    // Alerte 3 : Tentatives d'intrusion
    val intrusionAlert = unifiedDF
      .filter(col("source") === "auth")
      .filter(col("message").contains("Failed password"))
      .groupBy(
        window(col("timestamp"), "1 minute"),
        col("ip")
      )
      .agg(count("*").as("failed_attempts"))
      .filter(col("failed_attempts") >= INTRUSION_THRESHOLD)
      .select(
        lit("ALERTE INTRUSION").as("alert_type"),
        col("window.start").as("window_start"),
        col("ip"),
        col("failed_attempts"),
        concat(
          lit("Tentatives d'intrusion : "),
          col("failed_attempts"),
          lit(" echecs depuis IP : "),
          col("ip")          
        ).as("message")
      )
      

    // Alerte 4 : Pic de charge
    val highLoadAlert = unifiedDF
      .groupBy(
        window(col("timestamp"), "1 minute"),
        col("source")
      )
      .agg(count("*").as("request_count"))
      .filter(col("request_count") >= HIGH_LOAD_THRESHOLD)
      .select(
        lit("ALERTE PIC DE CHARGE").as("alert_type"),
        col("window.start").as("window_start"),
        col("source"),
        col("request_count"),
        concat(
          lit("Pic de charge détecté : "),
          col("request_count"),
          lit(" requêtes/minute sur "),
          col("source")
        ).as("message")
      )

    // Afficher les alertes
    val q1 = errorRateAlert.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("complete")
      .queryName("alert_error_rate")
      .start()

    val q2 = latencyAlert.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("complete")
      .queryName("alert_latency")
      .start()

    val q3 = intrusionAlert.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("complete")
      .queryName("alert_intrusion")
      .start()

    val q4 = highLoadAlert.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("complete")
      .queryName("alert_high_load")
      .start()

    spark.streams.awaitAnyTermination()
  }
}