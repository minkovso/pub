package com.example

object BostonCrimesMap extends App {
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions.{broadcast, count, sum, split, trim, array}

  val spark = SparkSession.builder.appName("App").getOrCreate()
  import spark.implicits._

  val crime = args(0)
  val codes = args(1)
  val path = args(2)

  val crime_df = spark.read.option("header", "true").option("inferSchema", "true").csv(crime)
  val codes_df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(codes)
  val codes_df = codes_df_raw.dropDuplicates("code").withColumn("name", trim(split($"name", "-")(0)))

  crime_df
    .join(broadcast(codes_df), $"offense_code" === $"code")
    .groupBy($"district", $"name")
    .count()
    .withColumn("name_cnt", array($"name", $"count"))
    .createOrReplaceTempView("df_2_tmp")

  spark.udf.register("list_agg", (r: Seq[Seq[String]]) => r.sortWith(_(1).toInt > _(1).toInt).slice(0, 3).map(_.head).mkString(", "))

  val df_2 = spark.sql(
    "SELECT" +
    "  COALESCE(district, 'null') AS district" +
    ", LIST_AGG(COLLECT_LIST(name_cnt)) AS frequent_crime_types " +
    "FROM" +
    "  df_2_tmp " +
    "GROUP BY" +
    "  district"
  )

  crime_df.groupBy($"district", $"year", $"month").agg(
    count($"*").as("all_cnt"),
    count($"lat").as("lat_cnt"),
    sum($"lat").as("lat_sum"),
    count($"long").as("long_cnt"),
    sum($"long").as("long_sum")
  ).createOrReplaceTempView("df_1_tmp")

  val df_1 = spark.sql(
    "SELECT" +
    "  COALESCE(district, 'null') AS district" +
    ", SUM(all_cnt) AS crimes_total" +
    ", PERCENTILE_APPROX(all_cnt, 0.5) AS crimes_monthly" +
    ", SUM(lat_sum)/SUM(lat_cnt) AS lat" +
    ", SUM(long_sum)/SUM(long_cnt) AS lng " +
    "FROM" +
    "  df_1_tmp " +
    "GROUP BY" +
    "  district"
  )

  df_1.join(df_2, "district").select(
    $"district",
    $"crimes_total",
    $"crimes_monthly",
    $"frequent_crime_types",
    $"lat",
    $"lng"
  ).coalesce(1).write.mode("overwrite").parquet(path)

}
