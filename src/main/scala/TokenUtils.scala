import scala.util.matching.Regex

object TokenUtils {
  val wordPattern: Regex = "^[a-z_-]{6,24}$".r
  val numberPattern: Regex = "^-?([0-9]+[.,]?[0-9]*|[0-9]*[.,]?[0-9]+)$".r

  def isWord(token: String): Boolean = wordPattern.findFirstIn(token).isDefined
  def isNumber(token: String): Boolean = numberPattern.findFirstIn(token).isDefined && token.length >= 4 && token.length <= 16

  def tokenizeWordsOnly(line: String): Seq[String] =
    line.toLowerCase.replaceAll("[^a-z0-9.,_-]+", " ")
      .split("\\s+").map(_.replaceAll("^-+(?=[a-z])", "")).filter(isWord)

  def tokenizeWordsAndNumbers(line: String): Seq[String] =
    line.toLowerCase.replaceAll("[^a-z0-9.,_-]+", " ")
      .split("\\s+").map(_.replaceAll("^-+(?=[a-z])", "")).filter(t => isWord(t) || isNumber(t))
}
