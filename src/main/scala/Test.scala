import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, window}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    val kafkaDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic-test2, topic-test1")
      .option("startingOffsets", "latest")
      .load()

    val kafkaStringDF = kafkaDF.selectExpr("CAST(value AS STRING)")

    val structType = new StructType()
      .add("word", StringType)
      .add("msg_ts", TimestampType)

    val data = kafkaStringDF.select(from_json(col("value"), structType).as("data"))
      .select("data.*")

    val groupData =  data
      .groupBy(
        window(data.col("msg_ts"), "10 seconds", "10 seconds"),
        data.col("word"))
      .count()

    groupData.writeStream.format("console").option("truncate", "false").start.awaitTermination()


    //    def saveToDB( askDF:DataFrame, batchID:Long ) : Unit = {
    //      askDF.persist()
    //      askDF.write.format("jdbc")
    //        .option("driver", "org.postgresql.Driver")
    //        .option("url", "jdbc:postgresql://localhost:5432/dbtest")
    //        .option("user", "postgres")
    //        .option("dbtable", "test")
    //        .mode("append")
    //        .option("password", "1234").save()
    //      askDF.unpersist()
    //    }
    //
    //    data.writeStream.foreachBatch(saveToDB _).start().awaitTermination()
  }

}
