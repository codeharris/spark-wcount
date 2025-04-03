import org.apache.spark.{SparkConf, SparkContext}

object WordCountTop {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Correct arguments: <input-dir> <output-dir>")
      System.exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("WordCountTop"))
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val start = System.currentTimeMillis()

    val WORD_PATTERN = "^[a-z_\\-]{6,24}$".r

    val topWords = sc.textFile(args(0))
      .flatMap { line =>
        val cleaned = line.replaceAll("[()]", "").trim // remove parentheses
        val parts = cleaned.split(",", 2)
        if (parts.length == 2) {
          val word = parts(0).trim
          val countStr = parts(1).trim
          if (WORD_PATTERN.pattern.matcher(word).matches()) {
            try Some((word, countStr.toInt))
            catch { case _: NumberFormatException => None }
          } else None
        } else None
      }
      .sortBy({ case (_, count) => -count })
      .take(100)

    sc.parallelize(topWords)
      .map { case (word, count) => s"$word\t$count" }
      .coalesce(1)
      .saveAsTextFile(args(1))

    val end = System.currentTimeMillis()
    println(s"Execution Time: ${end - start} ms")

    sc.stop()
  }
}

