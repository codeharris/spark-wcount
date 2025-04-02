import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount1000 {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Correct arguments: <input-directory> <output-directory>")
      System.exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("SparkWordCount"))

    // old Hadoop trick: make sure we can recursively scan directories for text files!
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val textFile = sc.textFile(args(0))
    val counts = textFile
      .flatMap(line => line.split("\\s+")) // changed from split(" ") to handle all whitespace
      .flatMap(TokenUtils.tokenizeWordsOnly)
      .map((_, 1))
      .reduceByKey(_ + _)
      .filter { case (_, count) => count == 1000 }
      .coalesce(1)

    counts.saveAsTextFile(args(1))

    sc.stop()
  }
}
