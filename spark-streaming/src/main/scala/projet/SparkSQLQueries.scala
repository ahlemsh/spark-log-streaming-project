package projet

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSQLQueries {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkSQLQueries")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val logsDF = spark.read
      .parquet("hdfs://namenode:9000/logs/parquet/")

    logsDF.createOrReplaceTempView("logs")

    println("\n" + "="*60)
    println("REQUÊTES ANALYTIQUES SPARK SQL")
    println("="*60)

    // Requête 1 : Nombre total de logs par source
    println("\n Requête 1 — Nombre total de logs par source")
    val q1 = spark.sql("""
      SELECT source,
             COUNT(*) as total_logs
      FROM logs
      GROUP BY source
      ORDER BY total_logs DESC
    """)
    q1.show()

    // Requête 2 : Nombre d'erreurs par source
    println("\n Requête 2 — Nombre d'erreurs par source")
    val q2 = spark.sql("""
      SELECT source,
             COUNT(*) as total_errors,
             ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as error_pct
      FROM logs
      WHERE level = 'ERROR'
      GROUP BY source
      ORDER BY total_errors DESC
    """)
    q2.show()

    // Requête 3 : Temps de réponse moyen par endpoint 
    println("\n Requête 3 — Temps de réponse moyen par endpoint")
    val q3 = spark.sql("""
      SELECT message as endpoint,
             COUNT(*) as nb_requetes,
             ROUND(AVG(response_time), 2) as avg_ms,
             MAX(response_time) as max_ms,
             MIN(response_time) as min_ms
      FROM logs
      WHERE source != 'auth'
      AND response_time > 0
      GROUP BY message
      ORDER BY avg_ms DESC
    """)
    q3.show()

    // Requête 4 : Distribution des status codes 
    println("\n Requête 4 — Distribution des status codes")
    val q4 = spark.sql("""
      SELECT status_code,
             COUNT(*) as count,
             ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
      FROM logs
      WHERE status_code > 0
      GROUP BY status_code
      ORDER BY count DESC
    """)
    q4.show()

    // Requête 5 : Top IPs les plus actives 
    println("\n Requête 5 — Top IPs les plus actives")
    val q5 = spark.sql("""
      SELECT ip,
             COUNT(*) as total_requests,
             COUNT(CASE WHEN level = 'ERROR' THEN 1 END) as errors,
             ROUND(AVG(response_time), 2) as avg_response_time
      FROM logs
      WHERE ip != ''
      GROUP BY ip
      ORDER BY total_requests DESC
    """)
    q5.show()

    // Requête 6 : Logs par heure 
    println("\n Requête 6 — Activité par heure")
    val q6 = spark.sql("""
      SELECT HOUR(timestamp) as heure,
             COUNT(*) as total_logs,
             COUNT(CASE WHEN level = 'ERROR' THEN 1 END) as errors,
             ROUND(AVG(response_time), 2) as avg_response_time
      FROM logs
      WHERE timestamp IS NOT NULL
      GROUP BY HOUR(timestamp)
      ORDER BY heure
    """)
    q6.show()

    // Requête 7 : Tentatives d'intrusion 
    println("\n Requête 7 — Tentatives d'intrusion par minute")
    val q7 = spark.sql("""
      SELECT DATE_TRUNC('minute', timestamp) as minute,
             COUNT(*) as failed_attempts
      FROM logs
      WHERE source = 'auth'
      AND message LIKE '%Failed password%'
      GROUP BY DATE_TRUNC('minute', timestamp)
      ORDER BY failed_attempts DESC
      LIMIT 10
    """)
    q7.show()

    // ─── Requête 8 : Performance par source ───
    println("\n Requête 8 — Performance globale par source")
    val q8 = spark.sql("""
      SELECT source,
             COUNT(*) as total,
             COUNT(CASE WHEN level = 'ERROR' THEN 1 END) as errors,
             COUNT(CASE WHEN level = 'WARN' THEN 1 END) as warnings,
             COUNT(CASE WHEN level = 'INFO' THEN 1 END) as info,
             ROUND(COUNT(CASE WHEN level = 'ERROR' THEN 1 END) * 100.0 / COUNT(*), 2) as error_rate_pct,
             ROUND(AVG(CASE WHEN response_time > 0 THEN response_time END), 2) as avg_response_ms
      FROM logs
      GROUP BY source
      ORDER BY error_rate_pct DESC
    """)
    q8.show()

    println("\n" + "="*60)
    println("FIN DES REQUÊTES ANALYTIQUES")
    println("="*60)

    spark.stop()
  }
}