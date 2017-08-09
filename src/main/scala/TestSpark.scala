import org.apache.spark.{SparkConf, SparkContext}

object TestSpark {

  def main(args: Array[String]) {
    val logFile = "./testspark.txt"
    val conf = new SparkConf().setAppName("test spark").setMaster("local")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("h")).count()
    val numBs = logData.filter(line => line.contains("j")).count()
    println("Lines with h: %s, Lines with j: %s".format(numAs, numBs))
  }

}