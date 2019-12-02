// test code

// Import Spark Context
import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._ 

// Code to clean flight data
object CleanFlightData1 {   
  def main(args: Array[String]) {    

      // Create spark context          
      val sc = new SparkContext()

      // Create an RDD for all flight data stored on HDFS
      val flightRDD=sc.textFile("hdfs:///user/asb862/flightData/*")

      // Create flightSplitRDD by Spliting the data on ','
      val flightSplitRDD = flightRDD.map(line=>line.split(","))

      // Create fightParsedRDD with only required columns from the entire flight data
      val flightParsedRDD = flightSplitRDD.map(record=>record(0)+","+record(1)+","+record(2)+","+record(3)+","+record(7)+","+record(10)+","+record(11)+","+record(12)+","+record(13)+","+record(14)+","+record(15)+","+record(16)+","+record(19))       
      
      // Filter data with missing columns
      val flightFilteredRDD = flightParsedRDD.filter(record => !(record.split(",").contains("")))

      // Remove all cancelled flights to get data of delayed flights only
      val flightFilteredCancelledRDD = flightFilteredRDD.filter(record => !(record.split(',')(11).toFloat==1.00))

      // Remove records with length<13 columns
      val flightCleanedRDD = flightFilteredCancelledRDD.filter(line=>line.split(",").length==13)

      // Filter data for desired airports
      val flightSelectedAirportsRDD = flightCleanedRDD.filter(line=>line.contains("JFK") | line.contains("EWR") | line.contains("LAX") | line.contains("ORD") | line.contains("DFW") | line.contains("MCO"))
      
      // Save data as cleanFlightData.csv file on HDFS
      flightSelectedAirportsRDD.saveAsTextFile("cleanFlightData.csv")


      val arr1 = Array(0,1,2,3,7,10,11,12,13,14,15,16,19)
      for(elem <- arr1) { if(elem==0||elem==1||elem==2||elem==13||elem==15||elem==16||elem==19){ var col:org.apache.spark.rdd.RDD[Float]=flightRDD.map(line=>line.split(',')).filter(line=> !(line.contains(""))).map(x=>x(elem).toFloat); println("Col"+elem+" Min "+col.min); println("Col"+elem+" Max "+col.max); val dist:org.apache.spark.rdd.RDD[Float] = col.distinct ; } else {var col:org.apache.spark.rdd.RDD[String]=flightRDD.map(line=>line.split(',')).map(x=>x(elem)); println("Col"+elem+" Min "+col.min); println("Col"+elem+" Max"+col.max); val dist:org.apache.spark.rdd.RDD[String] = col.distinct ;  val len:org.apache.spark.rdd.RDD[Int] = dist.map(line=>line.length()); println("Max String Length "+len.max) } }
      // Stop spark

      sc.stop()
  } 
}
