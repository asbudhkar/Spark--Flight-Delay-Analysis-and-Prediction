// Test code
// Import required libraries

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._ 
import org.apache.spark.rdd._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors   
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}


object AnalyticCode {                                                                                                  
def main(args: Array[String]) {

val sc = new SparkContext()    

// Create RDD of cleaned flight data
val flightRDD=sc.textFile("hdfs:///user/asb862/cleanFlightJoin.csv")

// Create Tuple RDD with key and features for join
val flightRDDTuple = flightRDD.map(line=>line.split(",")).map(record => (record(0)+":"+record(1),(record(2)+","+record(3)+","+record(4))))

// Create RDD of cleaned weather data
val weatherRDD = sc.textFile("file:///home/asb862/weather_data/cleaned_weather/*")

// Create Tuple RDD with key and features for join
val weatherRDDTuple = weatherRDD.map(line=>line.split(",")).map(record=>("("+record(0)+":"+"\""+record(6)+"\"",record(1)+","+record(2)+","+record(3)+","+record(4)+","+record(5)))

// Join weather and flight data
val joinedData = weatherRDDTuple.join(flightRDDTuple)

//joinedData.saveAsTextFile(JoinedData2.csv)

//val joinedData1 = sc.textFile("hdfs:///home/asb862/JoinedData2.csv")

// filter records of 2017
val rdd17 = joinedData.filter(line=>line._1.contains("18"))

// filter records of 2018
val rdd18 = joinedData.filter(line=>line._1.contains("17"))

//rdd18.saveAsTextFile("joinedRDD12018.csv")

//rdd17.saveAsTextFile("joinedRDD12017.csv")

val rdd2017 = sc.textFile("joinedRDD12017.csv")

val rdd2018 = sc.textFile("joinedRDD12018.csv")

// extract features from rdd
val rdd12017 = rdd2017.map(line=>line.split(",")).map(record=>record(1)+","+record(2)+","+record(3)+","+record(4)+","+record(5)+","+record(6)+","+record(7)+","+record(8))   

// extract features from rdd
val rdd12018 = rdd2018.map(line=>line.split(",")).map(record=>record(1)+","+record(2)+","+record(3)+","+record(4)+","+record(5)+","+record(6)+","+record(7)+","+record(8))

// clean features rdd
val r1=rdd12017.map(line=>line.replaceAll("\\(*\\)*",""))

// clean features rdd
val r2=rdd12018.map(line=>line.replaceAll("\\(*\\)*",""))  

//r1.saveAsTextFile("features2017.csv")
//r2.saveAsTextFile("features2018.csv")

// create a rdd of double array
val rdd1 = r1.map(line=>line.split(",")).map(record=>Array(record(0).toDouble,record(1).toDouble,record(2).toDouble,record(3).toDouble,record(4).toDouble,record(5).toDouble,record(6).toDouble,record(7).toDouble))   

val rdd2 = r2.map(line=>line.split(",")).map(record=>Array(record(0).toDouble,record(1).toDouble,record(2).toDouble,record(3).toDouble,record(4).toDouble,record(5).toDouble,record(6).toDouble,record(7).toDouble))   

// label data with delay or on time (1,0) for training
def labeledData(vals: Array[Double]): LabeledPoint = {  
LabeledPoint(if (vals(5).toDouble>=20) 1.0 else 0.0, Vectors.dense(vals))
}

val rdd1labeled = rdd1.map(labeledData)

val rdd2labeled = rdd2.map(labeledData)


rdd1labeled.cache

rdd2labeled.cache

// run logistic regression on training data
val logisticRegressionModel = new LogisticRegressionWithLBFGS().setNumClasses(2).run(rdd1labeled)

// predict the labels for test data
val labelsLogisticRegression = rdd2labeled.map { case LabeledPoint(label, features) => val prediction =  logisticRegressionModel.predict(features) 
(prediction, label) }

// calculate accuracy
def testModel(labels: RDD[(Double, Double)]) : Tuple2[Array[Double], Array[Double]] = {                               
 val truePositive = labels.filter(r => r._1==1 && r._2==1).count.toDouble                                                  
 val trueNegative = labels.filter(r => r._1==0 && r._2==0).count.toDouble                                                
 val falsePostive = labels.filter(r => r._1==1 && r._2==0).count.toDouble                                                  
 val falseNegative = labels.filter(r => r._1==0 && r._2==1).count.toDouble                                                 
val accuracy = (truePositive+trueNegative) / (truePositive+trueNegative+falsePostive+falseNegative)
new Tuple2(Array(truePositive, trueNegative, falsePostive, falseNegative), Array( accuracy))       }      


// Get accuracy 
val (counts, metrics) = testModel(labelsLogisticRegression)                   

println("\n Logistic Regression Metrics  \n Accuracy = %.2f percent".format( metrics (0)*100))

println("\n True Positives = %.2f, True Negatives = %.2f, False Positives = %.2f, False Negatives = %.2f".format(counts (0), counts (1), counts (2), counts(3)))       

sc.stop()            
 }       
}
