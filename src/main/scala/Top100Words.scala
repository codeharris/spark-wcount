
import org.apache.spark.SparkContext

object Top100Words {
  def run(sc: SparkContext, inputPath: String, outputPath: String): Unit = {
    val top100 = sc.textFile(inputPath)
      .flatMap(TokenUtils.tokenizeWordsOnly)
      .map((_, 1))
      .reduceByKey(_ + _)
      .map(_.swap)                // (count, word)
      .sortByKey(ascending = false)
      .map(_.swap)                // back to (word, count)
      .take(100)                  // collect top 100

    val formatted = top100.map { case (w, c) => s"($w, $c)" }
    sc.parallelize(formatted, 1).saveAsTextFile(outputPath)
  }
}
