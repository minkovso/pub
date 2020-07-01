
import java.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{explode, current_date, unix_timestamp}
import org.apache.spark.sql.streaming.Trigger

object DeStream {
  def main(args: Array[String]) {

    val connectionProperties = new Properties()
    connectionProperties.put("user", "spark")
    connectionProperties.put("password", "spark")

    val spark = SparkSession.builder
      .appName("DeStream")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate

    import spark.implicits._

    val stream = spark.readStream.json("file:///home/spark/stream")

    val query = stream.writeStream.foreachBatch((batchDF: DataFrame, _: Long) => {
      batchDF.persist

      batchDF.withColumn("p_snap_date", current_date)
        .write.mode("overwrite")
        .partitionBy("p_snap_date")
        .parquet("/user/stream")

      batchDF.select($"id", explode($"key_skills"))
        .select($"id", $"col".getField("name").alias("skill"))
        .groupBy($"skill").count
        .withColumnRenamed("count", "cnt")
        .withColumn("snap_date", unix_timestamp(current_date))
        .write.option("truncate", "true").mode("overwrite")
        .jdbc("jdbc:postgresql:spark", "public.stream", connectionProperties)

      batchDF.unpersist
      ()
    }).trigger(Trigger.ProcessingTime("120 seconds")).start

    query.awaitTermination
  }
}
