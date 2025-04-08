import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import scala.io.Source
import scala.util.parsing.json.JSON
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

// === CONFIG LOADER ===
case class Rule(ruleType: String, column: Option[String], expression: Option[String])
case class TableCheck(tableName: String, rules: Seq[Rule])
case class Config(tables: Seq[TableCheck], outputPath: String, hiveReportTable: String)

object ConfigLoader {
  def loadConfig(path: String): Config = {
    val source = Source.fromFile(path)
    val raw = try source.mkString finally source.close()
    val parsed = JSON.parseFull(raw).get.asInstanceOf[Map[String, Any]]

    val tables = parsed("tables").asInstanceOf[List[Map[String, Any]]].map { t =>
      val tableName = t("tableName").toString
      val rules = t("rules").asInstanceOf[List[Map[String, Any]]].map { r =>
        Rule(r("ruleType").toString,
             r.get("column").map(_.toString),
             r.get("expression").map(_.toString))
      }
      TableCheck(tableName, rules)
    }

    val outputPath = parsed("outputPath").toString
    val hiveReportTable = parsed("hiveReportTable").toString
    Config(tables, outputPath, hiveReportTable)
  }
}

// === MAIN VALIDATOR ===
object DataQualityChecker {
  val spark = SparkSession.builder()
    .appName("Hive DQ Checker")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val configPath = if (args.length > 0) args(0) else "dq_config.json"
    val config = ConfigLoader.loadConfig(configPath)

    val results = config.tables.flatMap { tableCheck =>
      val df = spark.table(tableCheck.tableName)
      runChecks(tableCheck.tableName, df, tableCheck.rules)
    }

    val reportDF = results.toDF()
    reportDF.write.mode(SaveMode.Append).saveAsTable(config.hiveReportTable)
  }

  case class DQResult(table: String, ruleType: String, column: String, result: String, timestamp: String)

  def runChecks(tableName: String, df: DataFrame, rules: Seq[Rule]): Seq[DQResult] = {
    val ts = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    rules.map {
      case Rule("not_null", Some(col), _) =>
        val nulls = df.filter(df(col).isNull).count()
        DQResult(tableName, "not_null", col, s"$nulls nulls", ts)

      case Rule("uniqueness", Some(col), _) =>
        val total = df.count()
        val unique = df.select(col).distinct().count()
        DQResult(tableName, "uniqueness", col, s"$unique / $total unique", ts)

      case Rule("cardinality", Some(col), _) =>
        val count = df.select(col).distinct().count()
        DQResult(tableName, "cardinality", col, s"$count distinct", ts)

      case Rule("min_max", Some(col), _) =>
        val row = df.agg(min(col), max(col)).first()
        DQResult(tableName, "min_max", col, s"min=${row.get(0)}, max=${row.get(1)}", ts)

      case Rule("custom_expr", _, Some(expr)) =>
        val violations = spark.sql(s"SELECT * FROM ${tableName} WHERE NOT ($expr)").count()
        DQResult(tableName, "custom_expr", expr, s"$violations violations", ts)

      case Rule("empty_check", _, _) =>
        val total = df.count()
        DQResult(tableName, "empty_check", "*", s"$total rows", ts)

      case other =>
        DQResult(tableName, "unknown", other.toString, "not executed", ts)
    }
  }
}
