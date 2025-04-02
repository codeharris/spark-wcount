
import org.apache.spark.SparkContext

object WordPairs1000 {

  // Generate normalized word pairs with distance m <= 2
  def generateWordPairs(tokens: Seq[String]): Seq[((String, String), Int)] = {
    val buffer = scala.collection.mutable.ArrayBuffer[((String, String), Int)]()
    for {
      i <- tokens.indices
      j <- (i + 1) to (i + 2).min(tokens.length - 1)
      w1 = tokens(i)
      w2 = tokens(j)
      orderedPair = if (w1 < w2) (w1, w2) else (w2, w1)
    } {
      buffer += ((orderedPair, 1))
    }
    buffer
  }

  def run(sc: SparkContext, inputPath: String, outputPath: String): Unit = {
    val wordPairsWithCount1000 = sc.textFile(inputPath)
      .flatMap(line => generateWordPairs(TokenUtils.tokenizeWordsOnly(line)))
      .reduceByKey(_ + _)
      .filter { case (_, count) => count == 1000 }
      .map { case ((w1, w2), c) => s"(($w1, $w2), $c)" }

    wordPairsWithCount1000.saveAsTextFile(outputPath)
  }
}
