import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._ 
object WordCount1 {   
  def main(args: Array[String]) {    
      if(args.length<0) {
      	System.exit(1)
      }
          
      val sc = new SparkContext()
      val flightRDD=sc.textFile("hdfs:///user/asb862/flightData/*")
      val flightSplitRDD = flightRDD.map(line=>line.split(","))
      val flightParsedRDD = flightSplitRDD.map(record=>record(0)+","+record(1)+","+record(2)+","+record(3)+","+record(7)+","+record(10)+","+record(11)+","+record(12)+","+record(13)+","+record(14)+","+record(15)+","+record(16)+","+record(19))       
      val flightFilteredRDD = flightParsedRDD.filter(record => !(record.split(",").contains("")))
      val flightFilteredCancelledRDD = flightFilteredRDD.filter(record => !(record.split(',')(11).toFloat==1.00))
      val flightCleanedRDD = flightFilteredCancelledRDD.filter(line=>line.split(",").length==13)
      val flightSelectedAirportsRDD = flightCleanedRDD.filter(line=>line.contains("JFK") | line.contains("EWR") | line.contains("LAX") | line.contains("ORD") | line.contains("DFW") | line.contains("MCO"))
      flightSelectedAirportsRDD.saveAsTextFile("cleanFlightData.csv")
      sc.stop()
  } 
}
