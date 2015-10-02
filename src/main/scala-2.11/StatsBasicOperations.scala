import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by hastimal on 10/1/2015.
 * //Refrence
 * https://books.google.com/books?id=2eptBgAAQBAJ&pg=PA114&lpg=PA114&dq=val+stats:StatCounter+stats()&source=bl&ots=oStDrm1tIX&sig=c9YYjYsE663bLlbGB3D8vesUa8Y&hl=en&sa=X&ved=0CDUQ6AEwBGoVChMIlNOK6bWiyAIVwYQNCh1a5gqs#v=onepage&q=val%20stats%3AStatCounter%20stats()&f=false
 */


// define main method (scala entry point)
object StatsBasicOperations  {
  System.setProperty("hadoop.home.dir","F:\\winutils")
  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("StatsBasicOperations").setMaster("local[2]").set("spark.executor.memory","4g")
    val sc = new SparkContext(conf)

    // do stuff

    println("Hello, See below all operations!")
    // Get the text file from filesystem
    val lines = sc.textFile("src/main/resources/sampleInputFiles/sample.txt").map(_.toInt)
    //calling stats available in Math Library

    val stats = lines.stats()
    val count= stats.count
    println("Total numbers counted    "+count)
    val sum = stats.sum
    println("Sum of numbers    "+sum)
    val mean= stats.mean
    println("Mean of all is     "+mean)
    val max = stats.max
    println("Maximum number is     "+max)
    val min = stats.min
    println("Minimum number is     "+min)
    val variance = stats.variance
    println("Variance of numbers is    "+variance)
    val std = stats.variance
    println("Standard deviation is    "+std)
    val arry = lines.toArray()
    val sorted = scala.util.Sorting.quickSort(arry)
    //finding median
    var median: Double = 0.0
    val middle = ((arry.length) / 2)
    if(arry.length % 2 == 0){
      val medianA = arry(middle)
      val medianB = arry(middle-1)
      median = (medianA + medianB).toFloat / 2
    } else{
      median = arry(middle + 1).toFloat
    }
    println("Median of numbers is      "+median)

    //finding mode
    var t = 0;
      for( i <- 0 until arry.length){
        for( j <- 1 until arry.length - i){
        if(arry(j-1) > arry(j)){
            t = arry(j-1)
            arry(j-1) = arry(j)
            arry(j) = t
        }
      }
    }

    var mode = arry(0)
    var temp = 1
    var temp2 = 1
    for( i <- 1 until arry.length){
      if(arry(i-1) == arry(i)){
        temp = temp + 1
      }
      else {
        temp = 1
      }
      if(temp >= temp2){
        mode = arry(i)
        temp2 = temp
      }
    }
    println("Mode of numbers is       "+mode)
   //Stopping spark
    sc.stop()

  }
}
