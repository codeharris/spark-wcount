import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import java.util.regex.Pattern

object SparkWordCount {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Correct arguments: <input-directory> <output-directory>")
      System.exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("SparkWordCount"))

    // old Hadoop trick: make sure we can recursively scan directories for text files!
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

     val WORD_PATTERN = Pattern.compile("^[a-z_\\-]{6,24}$")
     val NUMBER_PATTERN = Pattern.compile("^-?[0-9]+([.,][0-9]+)?$", Pattern.MULTILINE)


    val textFile = sc.textFile(args(0))
    val counts = textFile.flatMap(line => line.toLowerCase().split("[^a-z0-9.,_\\-]+"))
      .filter(word => WORD_PATTERN.matcher(word).matches() || (NUMBER_PATTERN.matcher(word).matches() && word.length >= 4 && word.length <= 16)  )
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .coalesce(1)
    counts.saveAsTextFile(args(1))

    sc.stop()
  }
}
