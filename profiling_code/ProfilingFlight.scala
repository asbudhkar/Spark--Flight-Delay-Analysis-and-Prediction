// Code for Profiling flight data

// Import Spark Context
import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._ 
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
//val sqlContext = new SQLContext(sc)

//import sqlContext._;

// Code to profile flight data
object ProfileFlightData {   
  def main(args: Array[String]) {    

      // Create spark context          
      val sc = new SparkContext()
      
      // Create sql context
      val sqlContext = new SQLContext(sc)
      import sqlContext._;
      // Create an RDD for all flight data stored on HDFS
      val flightRDD=sc.textFile("hdfs:///user/asb862/flightData/*")
 
      // Create a dataframe

      val df = sqlContext.read.format("csv").option("inferSchema","true").load("/user/asb862/fightData/*.csv")
      
      //val df = sqlContext.load("com.databricks.spark.csv", Map("path" -> "fightData/*.csv"))
   
      // Give proper name to columns
      val df2 = df
        .withColumn("YEAR",df("_c0").cast(IntegerType))
        .withColumn("MONTH",df("_c1").cast(IntegerType))
        .withColumn("DAY_OF_MONTH",df("_c2").cast(IntegerType))
        .withColumn("DAY_OF_WEEK",df("_c3").cast(IntegerType))
        .withColumn("ORIGIN",df("_c7").cast(StringType))
        .withColumn("DEST",df("_c10").cast(StringType))
        .withColumn("ORIGIN",df("_c7").cast(StringType))
        .withColumn("CRS_DEP_TIME",df("_c11").cast(StringType))
        .withColumn("DEP_TIME",df("_c12").cast(StringType))
        .withColumn("DEP_DELAY",df("_c13").cast(DoubleType))
        .withColumn("ARR_TIME",df("_c14").cast(StringType))
        .withColumn("ARR_DELAY",df("_c15").cast(DoubleType))
        .withColumn("CANCELLED",df("_c16").cast(DoubleType))
        .withColumn("DISTANCE",df("_c19").cast(DoubleType))
        .drop("_c0").drop("_c1").drop("_c2").drop("_c3").drop("_c4").drop("_c5").drop("_c6").drop("_c7").drop("_c8").drop("_c9").drop("_c10").drop("_c11").drop("_c12").drop("_c13").drop("_c14").drop("_c15").drop("_c16").drop("_c17").drop("_c18").drop("_c19").drop("_c20")


      // Print schema with data type of each column
      print(df2.printSchema())

      // Use only desired columns from dataset
      val arr1 = Array(0,1,2,3,7,10,11,12,13,14,15,16,19)

      val colnames = Array("YEAR","MONTH","DAY_OF_MONTH","DAY_OF_WEEK","ORIGIN","DEST","CRS_DEP_TIME","DEP_TIME","DEP_DELAY","ARR_TIME","ARR_DELAY","CANCELLED","DISTANCE")

      var i=(-1)
 
      // Get the min, max values for each column and max string length for specific columns 
      
     for(elem <- arr1) { 
        if(elem==0||elem==1||elem==2||elem==3) {
           i=i+1;var col:org.apache.spark.rdd.RDD[Int]=flightRDD.map(line=>line.split(',')).filter(line=> !(line.contains(""))).map(x=>x(elem).toInt); 
           println("Col:"+colnames(i)+" Min "+col.min); 
           println("Col:"+colnames(i)+" Max "+col.max); 
           val dist:org.apache.spark.rdd.RDD[Int] = col.distinct; 
        } else if(elem==13||elem==15||elem==16||elem==19) {
           i=i+1;var col:org.apache.spark.rdd.RDD[Double]=flightRDD.map(line=>line.split(',')).filter(line=> !(line.contains(""))).map(x=>x(elem).toFloat); 
           println("Col:"+colnames(i)+" Min "+col.min); 
           println("Col:"+colnames(i)+" Max "+col.max); 
           val dist:org.apache.spark.rdd.RDD[Double] = col.distinct ; 
        } else { 
          i=i+1;var col:org.apache.spark.rdd.RDD[String]=flightRDD.map(line=>line.split(',')).map(x=>x(elem)); 
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
