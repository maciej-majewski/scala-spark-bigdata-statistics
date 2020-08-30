package scalaspark.bigdata

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

object BigDataApp {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SummaryStatisticsApp").setMaster("local[*]")
    val sc = new SparkContext(conf)

    import org.apache.log4j._
    Logger.getLogger("org").setLevel(Level.ERROR)

    val observations = sc.parallelize(
      Seq(
        Vectors.dense(5.0, 50.0, 500.0),
        Vectors.dense(10.0, 100.0, 1000.0),
        Vectors.dense(1.0, 20.0, 30.5),
        Vectors.dense(8.0, 80.5, 200.0)
      )
    )

    // column summary statistics:
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
    println("Mean: " + summary.mean)  // a dense vector containing the mean value for each column
    println("Variance: " + summary.variance)  // column-wise variance
    println("Nonzeros: " + summary.numNonzeros)  // number of nonzeros in each column

    sc.stop()

  }
}
