package optimizer

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.PrintWriter
import scala.util.{Try, Success, Failure}
import scala.io.Source
import play.api.libs.json._

object UniversalPartitionOptimizer {

  case class Config(
    tableName: String,
    keyColumns: Seq[String],
    outputPath: String,
    skewThreshold: Double = 2.0,
    cardinalityThreshold: Int = 100,
    repartitionEnabled: Boolean = true,
    repartitionSaveMode: String = "overwrite",
    repartitionTableSuffix: String = "_optimized"
  )

  def loadConfig(path: String): Config = {
    val source = Source.fromFile(path)
    val json = try source.mkString finally source.close()
    val parsed = Json.parse(json)

    Config(
      (parsed \"tableName\").as[String],
      (parsed \"keyColumns\").as[Seq[String]],
      (parsed \"outputPath\").as[String],
      (parsed \"skewThreshold\").asOpt[Double].getOrElse(2.0),
      (parsed \"cardinalityThreshold\").asOpt[Int].getOrElse(100),
      (parsed \"repartitionEnabled\").asOpt[Boolean].getOrElse(true),
      (parsed \"repartitionSaveMode\").asOpt[String].getOrElse("overwrite"),
      (parsed \"repartitionTableSuffix\").asOpt[String].getOrElse("_optimized")
    )
  }

  def analyzeSkew(df: DataFrame, colName: String): (Double, Long, Long) = {
    val distribution = df.groupBy(col(colName)).count()
    val stats = distribution.agg(
      max("count").as("max_count"),
      avg("count").as("avg_count")
    ).collect().head

    val max = stats.getAs[Long]("max_count")
    val avg = stats.getAs[Double]("avg_count")

    val skew = if (avg == 0) 0.0 else max.toDouble / avg
    (skew, max, avg.toLong)
  }

  def analyzeCardinality(df: DataFrame, colName: String): Long = {
    df.select(col(colName)).distinct().count()
  }

  def generateReport(config: Config, spark: SparkSession): Unit = {
    import spark.implicits._

    val df = spark.table(config.tableName)

    val results = config.keyColumns.map { colName =>
      val (skew, max, avg) = analyzeSkew(df, colName)
      val cardinality = analyzeCardinality(df, colName)

      Json.obj(
        "column" -> colName,
        "skewFactor" -> skew,
        "maxCount" -> max,
        "avgCount" -> avg,
        "cardinality" -> cardinality,
        "recommendedPartitioning" -> (skew < config.skewThreshold && cardinality > config.cardinalityThreshold)
      )
    }

    val jsonReport = Json.obj(
      "table" -> config.tableName,
      "analysis" -> results,
      "timestamp" -> java.time.Instant.now.toString
    )

    val writer = new PrintWriter(s"${config.outputPath}/recommendation.json")
    writer.write(Json.prettyPrint(jsonReport))
    writer.close()
  }

  def applyRepartitioning(config: Config, spark: SparkSession): Unit = {
    val df = spark.table(config.tableName)
    val repartitioned = df.repartition(config.keyColumns.map(col): _*)

    val newTableName = config.tableName + config.repartitionTableSuffix

    repartitioned.write
      .mode(SaveMode.valueOf(config.repartitionSaveMode.toUpperCase))
      .format("parquet")
      .option("path", s"${config.outputPath}/$newTableName")
      .saveAsTable(newTableName)
  }

  def sendTelegramAlert(message: String): Unit = {
    val token = sys.env.getOrElse("TELEGRAM_BOT_TOKEN", "")
    val chatId = sys.env.getOrElse("TELEGRAM_CHAT_ID", "")
    val encodedMessage = java.net.URLEncoder.encode(message, "UTF-8")
    val url = s"https://api.telegram.org/bot$token/sendMessage?chat_id=$chatId&text=$encodedMessage"

    Try(scala.io.Source.fromURL(url).mkString) match {
      case Success(_) => println("Telegram alert sent.")
      case Failure(e) => println(s"Telegram alert failed: $e")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Universal Partition Optimizer")
      .enableHiveSupport()
      .getOrCreate()

    try {
      val configPath = args(0)
      val config = loadConfig(configPath)

      println(s"\uD83D\uDCCA Starting analysis for ${config.tableName} ...")
      generateReport(config, spark)

      if (config.repartitionEnabled) {
        println(s"⚙️ Repartitioning ${config.tableName} ...")
        applyRepartitioning(config, spark)
      }

      sendTelegramAlert(s"✅ Partition Optimizer completed for table ${config.tableName} at ${java.time.Instant.now}")
    } catch {
      case e: Throwable =>
        sendTelegramAlert(s"❌ Partition Optimizer failed: ${e.getMessage}")
        throw e
    } finally {
      spark.stop()
    }
  }
}

// example config.json
// {
//   "tableName": "finance.transactions",
//   "keyColumns": ["account_id", "transaction_month"],
//   "outputPath": "ozone://bucket/partition_optimizer",
//   "skewThreshold": 2.5,
//   "cardinalityThreshold": 200,
//   "repartitionEnabled": true,
//   "repartitionSaveMode": "overwrite",
//   "repartitionTableSuffix": "_opt"
// }
