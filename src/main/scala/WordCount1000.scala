
import org.apache.spark.SparkContext

object WordCount1000 {
  def run(sc: SparkContext, inputPath: String, outputPath: String): Unit = {
    val wordsWithCount1000 = sc.textFile(inputPath)
      .flatMap(TokenUtils.tokenizeWordsOnly)
      .map((_, 1))
      .reduceByKey(_ + _)
      .filter { case (_, count) => count == 1000 }
      .map { case (w, c) => s"($w, $c)" }

    wordsWithCount1000.saveAsTextFile(outputPath)
  }
}
