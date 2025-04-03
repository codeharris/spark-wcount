import org.apache.spark.{SparkConf, SparkContext}

object SparkWordPairs {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Correct arguments: <input-directory> <output-directory>")
      System.exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("SparkWordPairs"))
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val startTime = System.currentTimeMillis()

    val WORD_PATTERN = "^[a-z_\\-]{6,24}$".r

    val wordPairs = sc.textFile(args(0))
      .flatMap(line => {
        val tokens = line
          .toLowerCase
          .split("[^a-z0-9_\\-]+") // tokenization
          .filter(token => WORD_PATTERN.pattern.matcher(token).matches()) // only words

        // Generate word pairs using m ≤ 2 window
        tokens.indices.flatMap(i =>
          (i + 1 to (i + 2).min(tokens.length - 1)).map { j =>
            val w1 = tokens(i)
            val w2 = tokens(j)
            val pair = if (w1 < w2) s"$w1:$w2" else s"$w2:$w1"
            (pair, 1)
          }
        )
      })
      .reduceByKey(_ + _)
      .map { case (pair, count) => s"$pair\t$count" }
      .coalesce(1)

    wordPairs.saveAsTextFile(args(1))

    val endTime = System.currentTimeMillis()
    println(s"Job Execution Time: ${endTime - startTime} ms")

    sc.stop()
  }
}
