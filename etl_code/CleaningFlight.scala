// Code for Cleaning the flight data

// Import Spark Context
import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._ 

// Code to clean flight data
object CleanFlightData {   
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
      val flightSelectedAirportsRDD = flightCleanedRDD.filter(line=>line.contains("JFK") | line.contains("SFO") | line.contains("LAX") | line.contains("ORD") | line.contains("DFW"))

      // Change the data to correct format for joining      
      val flightSelectedAirportsParsedRDD  = flightSelectedAirportsRDD.map(line=>line.split(",")).map(record=>record(1)+"/"+record(2)+"/"+record(0).takeRight(2)+","+record(4)+","+record(8)+","+record(10)+","+record(12))     
      
      // Save data as cleanFlightData.csv file on HDFS
      flightSelectedAirportsParsedRDD.saveAsTextFile("cleanFlightDataToJoin.csv")

      // Stop spark

      sc.stop()
  } 
}
