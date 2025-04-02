
import org.apache.spark.SparkContext

object Top100NumberWordPairs {

  // Generate (Number, Word) pairs with m = 1 or 2
  def generateNumberWordPairs(tokens: Seq[String]): Seq[((String, String), Int)] = {
    val buffer = scala.collection.mutable.ArrayBuffer[((String, String), Int)]()
    for (i <- tokens.indices) {
      if (i + 1 < tokens.length && TokenUtils.isNumber(tokens(i)) && TokenUtils.isWord(tokens(i + 1)))
        buffer += (((tokens(i), tokens(i + 1)), 1))

      if (i + 2 < tokens.length && TokenUtils.isNumber(tokens(i)) && TokenUtils.isWord(tokens(i + 2)))
        buffer += (((tokens(i), tokens(i + 2)), 1))
    }
    buffer
  }

  def run(sc: SparkContext, inputPath: String, outputPath: String): Unit = {
    val top100Pairs = sc.textFile(inputPath)
      .flatMap(line => generateNumberWordPairs(TokenUtils.tokenizeWordsAndNumbers(line)))
      .reduceByKey(_ + _)
      .map { case (pair, count) => (count, pair) }
      .sortByKey(ascending = false)
      .take(100)

    val formatted = top100Pairs.map { case (count, (number, word)) => s"(($number, $word), $count)" }
    sc.parallelize(formatted, 1).saveAsTextFile(outputPath)
  }
}
