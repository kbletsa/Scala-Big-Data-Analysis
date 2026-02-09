import org.apache.spark.{SparkConf, SparkContext}

object Subtask1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WordLengthAverage").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val text = sc.textFile("src/main/resources/SherlockHolmes.txt")

    val cleanedWords = text
      .map(_.toLowerCase.replaceAll("[^a-z0-9 ]", " "))
      .flatMap(_.split("\\s+"))
      .filter(_.nonEmpty)
      .filter(word => word.charAt(0).isLetter)

    val pairRDD = cleanedWords.map(word => (word.charAt(0), (word.length, 1)))

    val reduced = pairRDD
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))

    val averages = reduced
      .map { case (letter, (sumLen, count)) =>
        (letter, sumLen.toDouble / count)
      }
      .sortBy(_._2, ascending = false)

    averages.collect().foreach {
      case (letter, avg) => println(s"$letter ${"%.2f".format(avg)}")
    }

    sc.stop()
  }
}
