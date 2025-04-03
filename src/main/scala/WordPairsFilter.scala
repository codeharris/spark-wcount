import org.apache.spark.{SparkConf, SparkContext}

object WordPairsFilter {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Correct arguments: <input-directory> <output-directory>")
      System.exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("WordPairsFilter"))
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val startTime = System.currentTimeMillis()

    val filteredPairs = sc.textFile(args(0))
      .flatMap { line =>
        val parts = line.trim.split("\t")
        if (parts.length == 2) {
          try {
            val count = parts(1).toInt
            Some((parts(0), count))
          } catch {
            case _: NumberFormatException => None
          }
        } else None
      }
      .filter { case (_, count) => count == 1000 }
      .map { case (pair, count) => s"$pair\t$count" }
      .coalesce(1)

    filteredPairs.saveAsTextFile(args(1))

    val endTime = System.currentTimeMillis()
    println(s"Job Execution Time: ${endTime - startTime} ms")

    sc.stop()
  }
}
