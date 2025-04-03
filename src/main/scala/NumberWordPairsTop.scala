
import org.apache.spark.{SparkConf, SparkContext}

object NumberWordPairsTop {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Correct arguments: <input-dir> <output-dir>")
      System.exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("NumberWordPairsTop"))
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val start = System.currentTimeMillis()

    val WORD_PATTERN = "^[a-z_\\-]{6,24}$".r
    val NUMBER_PATTERN = "^-?[0-9]+([.,][0-9]+)?$".r

    val topPairs = sc.textFile(args(0))
      .flatMap(line => {
        val tokens = line
          .toLowerCase
          .split("[^a-z0-9.,_\\-]+")
          .filter(_.nonEmpty)

        // Generate number → word pairs (within m ≤ 2)
        tokens.indices.flatMap(i =>
          (i + 1 to (i + 2).min(tokens.length - 1)).flatMap { j =>
            val t1 = tokens(i)
            val t2 = tokens(j)
            if (NUMBER_PATTERN.pattern.matcher(t1).matches() &&
              WORD_PATTERN.pattern.matcher(t2).matches()) {
              Some((s"$t1:$t2", 1))
            } else None
          }
        )
      })
      .reduceByKey(_ + _)
      .sortBy({ case (_, count) => -count })
      .take(100)

    sc.parallelize(topPairs)
      .map { case (pair, count) => s"$pair\t$count" }
      .coalesce(1)
      .saveAsTextFile(args(1))

    val end = System.currentTimeMillis()
    println(s"Execution Time: ${end - start} ms")

    sc.stop()
  }
}
