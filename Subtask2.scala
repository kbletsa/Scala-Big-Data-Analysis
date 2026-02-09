import org.apache.spark.sql.{SparkSession, functions => F}

object Subtask2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("TweetsAnalysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/tweets.csv")


    val cleaned = df.withColumn(
      "clean_text",
      F.lower(F.regexp_replace($"text", "[^a-z0-9 ]", ""))
    )

    // 1) Top-5 words per sentiment

    val words = cleaned
      .withColumn("word", F.explode(F.split($"clean_text", "\\s+")))
      .filter($"word" =!= "")

    val wordCounts = words
      .groupBy("airline_sentiment", "word")
      .count()

    val window = org.apache.spark.sql.expressions.Window
      .partitionBy("airline_sentiment")
      .orderBy(F.col("count").desc)

    val ranked = wordCounts
      .withColumn("rank", F.row_number().over(window))
      .filter($"rank" <= 5)

    println("\n=== Top 5 words per sentiment ===")
    ranked.orderBy("airline_sentiment", "rank").show(50, truncate = false)


    // 2) Main complaint reason per airline
    
    val complaints = cleaned
      .filter($"negativereason_confidence" > 0.5)
      .groupBy("airline", "negativereason")
      .count()

    val window2 = org.apache.spark.sql.expressions.Window
      .partitionBy("airline")
      .orderBy(F.col("count").desc)

    val topReasons = complaints
      .withColumn("rank", F.row_number().over(window2))
      .filter($"rank" === 1)

    println("\n=== Main complaint reason per airline (confidence > 0.5) ===")
    topReasons.show(false)

    spark.stop()
  }
}
