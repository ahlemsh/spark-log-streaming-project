package projet

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.maxmind.geoip2.DatabaseReader
import ua_parser.Parser
import java.io.File
import java.net.InetAddress

object GeoReader {
  lazy val instance: DatabaseReader = {
    val dbFile = new File("/opt/spark/data/GeoLite2-City.mmdb")
    new DatabaseReader.Builder(dbFile).build()
  }
}

object UAParser {
  lazy val instance: Parser = new Parser()
}

object LogParser {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("LogParser")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val geolocateIP = udf((ip: String) => {
      try {
        val address  = InetAddress.getByName(ip)
        val response = GeoReader.instance.city(address)
        val city     = Option(response.getCity.getName).getOrElse("")
        val country  = Option(response.getCountry.getName).getOrElse("")
        if (city.nonEmpty && country.nonEmpty) s"$city, $country"
        else if (country.nonEmpty) country
        else "Unknown"
      } catch {
        case _: Exception => "Private/Unknown"
      }
    })

    val parseUserAgent = udf((ua: String) => {
      try {
        val client  = UAParser.instance.parse(ua)
        val browser = client.userAgent.family
        val os      = client.os.family
        s"$browser / $os"
      } catch {
        case _: Exception => "Unknown"
      }
    })

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

    // Parsing JSON 
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

    // Parsing Apache 
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

    // Enrichissement
    val enrichedDF = unifiedDF
      .withColumn("geo_location",
        when(col("ip") === "" || col("ip").isNull, "N/A")
          .otherwise(geolocateIP(col("ip")))
      )
      .withColumn("browser_os",
        when(col("user_agent") === "" || col("user_agent").isNull, "N/A")
          .otherwise(parseUserAgent(col("user_agent")))
      )

    enrichedDF.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("append")
      .start()

    spark.streams.awaitAnyTermination()
  }
}