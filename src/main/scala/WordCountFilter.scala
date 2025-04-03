import org.apache.spark.{SparkConf, SparkContext}
// import org.apache.spark.SparkContext._ // Often imported, but not strictly necessary for this code

import java.util.regex.Pattern // For using Java's Pattern
import scala.util.Try         // For safely trying to convert String to Int

object WordCountFilter {

  def main(args: Array[String]) {

    // 1. Argument Checking (Same style as SparkWordCount)
    if (args.length < 2) {
      System.err.println("Correct arguments: <input-directory> <output-directory>")
      System.err.println("Input directory should contain the output files from a previous WordCount job (format: word<tab>count).")
      System.exit(1)
    }
    val inputPath = args(0)
    val outputPath = args(1)

    // 2. Spark Context Setup (Same style as SparkWordCount)
    val sc = new SparkContext(new SparkConf().setAppName("WordCountFilter"))

    // Optional: Set recursive directory scanning (if input might be nested)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    // 3. Define Patterns (Similar style, only need WORD_PATTERN here)
    val WORD_PATTERN: Pattern = Pattern.compile("^[a-z_-]{6,24}$")

    // Record start time (optional, but good practice)
    val startTime = System.currentTimeMillis()

    try {
      // 4. RDD Processing Chain
      val textFile = sc.textFile(inputPath) // Read lines from WordCount output

      val filteredCounts = textFile.flatMap { line =>
          // Process each line: Split, Validate, Parse, Filter bad lines
          val parts = line.toLowerCase.split("\t") // EXPECTING TAB SEPARATOR from standard Hadoop WordCount output
          if (parts.length == 2) {
            val word = parts(0)
            val countStr = parts(1)
            if (WORD_PATTERN.matcher(word).matches()) {
              Try(countStr.toInt).toOption.map(count => (word, count))
            } else None
          } else None
        } // Result: RDD[(String, Int)] containing only valid word-count pairs
        .filter { case (_, count) => count == 1000 } // Apply the count filter
        .map { case (word, count) => s"$word\t$count" } // Format back to "word<tab>count"
        .coalesce(1) // Optional: combine to one output file

      // 5. Save Results
      filteredCounts.saveAsTextFile(outputPath)
      println(s"Successfully filtered words with count 1000 to $outputPath")

    } catch {
      case e: Exception =>
        System.err.println(s"An error occurred during Spark processing: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      val endTime = System.currentTimeMillis()
      val duration = endTime - startTime
      println(s"Job Execution Time: $duration ms")

      // 6. Stop Spark Context
      sc.stop()
    }
  }
}
