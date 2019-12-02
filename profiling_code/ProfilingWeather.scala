// Code for Profiling weather data

// Import Spark Context
import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._ 
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
//val sqlContext = new SQLContext(sc)

//import sqlContext._;

// Code to profile flight data
object ProfileWeatherData {   
  def main(args: Array[String]) {    

      // Create spark context          
      val sc = new SparkContext()
      
      // Create sql context
      val sqlContext = new SQLContext(sc)
      import sqlContext._;

      // Create an RDD for all cleaned weather data stored on HDFS
      val weatherRDD=sc.textFile("hdfs:///user/asb862/weather_data/cleaned_weather")
 
      // Create a dataframe

      val df = sqlContext.read.format("csv").option("inferSchema","true").load("/user/asb862/weather_data/cleaned_weather")
      
      //val df = sqlContext.load("com.databricks.spark.csv", Map("path" -> "weather_datasets/*.csv"))
   
      // Give proper name to columns
      val df2 = df.withColumn("DATE",df("_c0").cast(StringType)).withColumn("AVG_WIND",df("_c1").cast(DoubleType)).withColumn("RAIN",df("_c2").cast(DoubleType)).withColumn("SNOW",df("_c3").cast(DoubleType)).withColumn("MIN_TEMP",df("_c4").cast(IntegerType)).withColumn("MAX_TEMP",df("_c5").cast(IntegerType)).withColumn("AIRPORT_NAME",df("_c6").cast(StringType)).drop("_c0").drop("_c1").drop("_c2").drop("_c3").drop("_c4").drop("_c5").drop("_c6")

      // Print schema with data type of each column
      print(df2.printSchema())

      // Use only desired columns from dataset
      val arr1 = Array(0,1,2,3,4,5,6)

      // Array of column names
      val colnames = Array("DATE","AVG_WIND","RAIN","SNOW","MAX_TEMP","MIN_TEMP","AIRPORT_CODE")

      var i=(-1)
 
      // Get the min, max values for each column and max string length for specific columns 
     
     // Print the profiling information 
     for(elem <- arr1) { 
        if(elem==4||elem==5) {
           i=i+1;var col:org.apache.spark.rdd.RDD[Int]=weatherRDD.map(line=>line.split(',')).filter(line=> !(line.contains(""))).map(x=>x(elem).toInt); 
           println("Col:"+colnames(i)+" Min "+col.min); 
           println("Col:"+colnames(i)+" Max "+col.max); 
           val dist:org.apache.spark.rdd.RDD[Int] = col.distinct; 
        } else if(elem==1||elem==2||elem==3) {
           i=i+1;
           var col:org.apache.spark.rdd.RDD[Double]=weatherRDD.map(line=>line.split(',')).filter(line=> !(line.contains(""))).map(x=>x(elem).toFloat); 
           println("Col:"+colnames(i)+" Min "+col.min); 
           println("Col:"+colnames(i)+" Max "+col.max); 
           val dist:org.apache.spark.rdd.RDD[Double] = col.distinct ; 
        } else if(elem==1){ 
          i=i+1;
          var col:org.apache.spark.rdd.RDD[String]=weatherRDD.map(line=>line.split(',')).map(x=>x(elem)); 
          println("Col:"+colnames(i)+" Min "+col.min); 
          println("Col:"+colnames(i)+" Max"+col.max); 
          val dist:org.apache.spark.rdd.RDD[String] = col.distinct ;  
          val len:org.apache.spark.rdd.RDD[Int] = dist.map(line=>line.length()); 
          println("\nMax String Length for Col:"+colnames(i)+" is "+len.max) 
        } 
     }

      // Stop spark

      sc.stop()
  } 
}
