// Code for Analytic

// Import required libraries

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._ 
import org.apache.spark.rdd._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors   
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.classification.{SVMModel,SVMWithSGD}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._ 
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._     
import org.apache.spark.sql.types._ 
import org.apache.spark.ml.feature.Bucketizer  
import org.apache.spark.sql.SparkSession

object AnalyticCode3 {                                                                                                  
def main(args: Array[String]) {

val sc = new SparkContext()    

// Create sql context
val sqlContext = new SQLContext(sc)

// Create spark session
val spark = SparkSession.builder().getOrCreate()    
import spark.implicits._

// Create RDD of cleaned flight data
val flightRDD=sc.textFile("hdfs:///user/asb862/cleanFlightDataToJoin.csv")

// Create Tuple RDD with key and features for join
val flightRDDTuple = flightRDD.map(line=>line.split(",")).map(record => ("("+record(0)+":"+record(1),(record(2)+","+record(3)+","+record(4))))

// Create RDD of cleaned weather data
val weatherRDD = sc.textFile("file:///home/asb862/weather_data/cleaned_weather/*")

// Create Tuple RDD with key and features for join
val weatherRDDTuple = weatherRDD.map(line=>line.split(",")).map(record=>("("+record(0)+":"+"\""+record(6)+"\"",record(1)+","+record(2)+","+record(3)+","+record(4)+","+record(5)))

// Join weather and flight data
val joinedData = weatherRDDTuple.join(flightRDDTuple)

joinedData.saveAsTextFile("joinedDataToAnalyse.csv")

// Get data
val joinedData1 = sc.textFile("hdfs:///user/asb862/joinedDataToAnalyse.csv")

// Get features RDD for SQL analytics
val rddFeatures = joinedData1.map(line=>line.split(",")).map(record=>record(0).split(":")(0)+","+record(0).split(":")(1)+","+record(1)+","+record(2)+","+record(3)+","+record(4)+","+record(5)+","+record(6)+","+record(7)+","+record(8)+","+(if(record(6).toDouble>15){1}else{0}))

// Clean data
val r1FeaturesCleaned=rddFeatures.map(line=>line.replaceAll("\\(*\\)*",""))    

// Save featuresCleaned data 
r1FeaturesCleaned.saveAsTextFile("featuresCleaned")
 
// Create data frame
val dfFeatures = sqlContext.read.format("csv").option("inferSchema","true").load("featuresCleaned")  

// Add column names to data frame
val df2Features = dfFeatures.withColumn("Date",dfFeatures("_c0").cast(StringType)).withColumn("Airport_Code",dfFeatures("_c1").cast(StringType)).withColumn("Avg_Wind",dfFeatures("_c2").cast(DoubleType)).withColumn("Prec",dfFeatures("_c3").cast(DoubleType)).withColumn("Snow",dfFeatures("_c4").cast(DoubleType)).withColumn("Max_Temp",dfFeatures("_c5").cast(IntegerType)).withColumn("Min_Temp",dfFeatures("_c6").cast(IntegerType)).withColumn("Dep_delay",dfFeatures("_c7").cast(DoubleType)).withColumn("Arr_delay",dfFeatures("_c8").cast(DoubleType)).withColumn("Dist",dfFeatures("_c9").cast(DoubleType)).withColumn("Label",dfFeatures("_c10").cast(IntegerType)).drop("_c0").drop("_c1").drop("_c2").drop("_c3").drop("_c4").drop("_c5").drop("_c6").drop("_c7").drop("_c8").drop("_c9").drop("_c10")       

// Register as hive table for analytic
df2Features.registerTempTable("data") 

//airport wise delays

// Delayed records per airport
val airportWiseDelays = sqlContext.sql("SELECT Airport_Code,sum(Label) AS count FROM data GROUP BY Airport_Code")     

// Total records
val airportWiseDelays1 = sqlContext.sql("SELECT Airport_Code,count(*) AS count FROM data GROUP BY Airport_Code")  
sqlContext.sql("DROP TABLE IF EXISTS asb862.airporttotalrecords")  

// Save as Hive Table for visualization in Tableau
airportWiseDelays1.write.saveAsTable("asb862.airporttotalrecords")

// Get percentage delay for each airport
val airportWiseDelays2 = sqlContext.sql("SELECT Airport_Code,(sum(Label)/count(*))*100 AS count FROM data GROUP BY Airport_Code ORDER BY 2 DESC")     

sqlContext.sql("DROP TABLE IF EXISTS asb862.aperdelays")

// Save as Hive table for visualization 
airportWiseDelays2.write.saveAsTable("asb862.aperdelays")
// month wise delay for each airport

// Add month column
val df2FeaturesWithMonth = df2Features.withColumn("month", from_unixtime(unix_timestamp($"Date", "MM/dd/yy"), "MMMMM"))

// Create table
df2FeaturesWithMonth.registerTempTable("monthdata")    

// Create RDD for monthWise Delays for all airports
val monthWiseDelays = sqlContext.sql("SELECT Airport_Code,month,sum(Label) AS count FROM monthdata GROUP BY Airport_Code,month ORDER BY 3 DESC")

sqlContext.sql("DROP TABLE IF EXISTS asb862.mon1delays")

// Save as Hive table
monthWiseDelays.write.saveAsTable("asb862.mon1delays")

// Month wise delays for JFK airport
val monthWiseDelaysJFK = monthWiseDelays.filter(row=>row(0)=="JFK")

sqlContext.sql("DROP TABLE IF EXISTS asb862.monJFKdelays")
     
monthWiseDelaysJFK.write.saveAsTable("asb862.monJFKdelays")  

// Month wise delays for ORD airport
val monthWiseDelaysORD = monthWiseDelays.filter(row=>row(0)=="ORD")

sqlContext.sql("DROP TABLE IF EXISTS asb862.monORDdelays")     

monthWiseDelaysORD.write.saveAsTable("asb862.monORDdelays")  

// Month wise delays for SFO airport     
val monthWiseDelaysSFO = monthWiseDelays.filter(row=>row(0)=="SFO")  

sqlContext.sql("DROP TABLE IF EXISTS asb862.monSFOdelays")     

monthWiseDelaysSFO.write.saveAsTable("asb862.monSFOdelays")

// Month wise delays for LAX airport     
val monthWiseDelaysLAX = monthWiseDelays.filter(row=>row(0)=="LAX") 

sqlContext.sql("DROP TABLE IF EXISTS asb862.monLAXdelays")     

monthWiseDelaysLAX.write.saveAsTable("asb862.monLAXdelays") 

// Month wise delays for DFW airport     
val monthWiseDelaysDFW = monthWiseDelays.filter(row=>row(0)=="DFW")  

sqlContext.sql("DROP TABLE IF EXISTS asb862.monDFWdelays")     

monthWiseDelaysDFW.write.saveAsTable("asb862.monDFWdelays")

// Percentwise delays for each month
val monthWiseDelays1 = sqlContext.sql("SELECT Airport_Code,month,(sum(Label)/count(*))*100 AS count FROM monthdata GROUP BY Airport_Code,month ORDER BY 3 DESC")

sqlContext.sql("DROP TABLE IF EXISTS asb862.monPerdelays") 

monthWiseDelays1.write.saveAsTable("asb862.monPerdelays")

// dist wise delay for each airport

// Create range of Dist
val splits = Range.Double(0,5500,250).toArray 

val bucketizer = new Bucketizer() .setInputCol("Dist").setOutputCol("Dist_range_id").setSplits(splits)

// Transform data to add distance range column
val df3 = bucketizer.transform(df2Features)

df3.registerTempTable("rangedata")      

// Distance wise delays
val distDelays = df3.filter(line=>line.get(10)==1).groupBy("Dist_range_id").count 

// Sort according to count for visualization
distDelays.sort("count")

val distDelay = df3.groupBy("Dist_range_id").count.sort("count") 

// Create RDD of distance range and count of delays observed in particular range
val distWiseDelays = sqlContext.sql("SELECT Dist_range_id,(sum(Label)/count(*))*100 AS count FROM rangedata GROUP BY Dist_range_id ORDER BY 2 DESC")

//distWiseDelays.collect

sqlContext.sql("DROP TABLE IF EXISTS asb862.distdelays")  

distWiseDelays.write.saveAsTable("asb862.distdelays")


// filter records of 2017
val rdd17 = joinedData.filter(line=>line._1.contains("17"))

// filter records of 2018
val rdd18 = joinedData.filter(line=>line._1.contains("18"))

//rdd18.saveAsTextFile("joinedRDD0Data2018.csv")

//rdd17.saveAsTextFile("joinedRDD0Data2017.csv")

//val rdd2017 = sc.textFile("joinedRDD0Data2017.csv")

//val rdd2018 = sc.textFile("joinedRDD0Data2018.csv")

// Get joined Data for 2018
val rdd12018 = rdd18.map(line=>line._2).map(record=>record._1+","+record._2)    

// Get joined Data for 2017
val rdd12017 = rdd17.map(line=>line._2).map(record=>record._1+","+record._2)    

// Extract features from rdd
//val rdd12017 = rdd2017.map(line=>line.split(",")).map(record=>record(1)+","+record(2)+","+record(3)+","+record(4)+","+record(5)+","+record(6)+","+record(7)+","+record(8))   

// Extract features from rdd
//val rdd12018 = rdd2018.map(line=>line.split(",")).map(record=>record(1)+","+record(2)+","+record(3)+","+record(4)+","+record(5)+","+record(6)+","+record(7)+","+record(8))

// Clean features rdd
val r1=rdd12017.map(line=>line.replaceAll("\\(*\\)*",""))

// Clean features rdd
val r2=rdd12018.map(line=>line.replaceAll("\\(*\\)*",""))  

//r1.saveAsTextFile("features2017.csv")
//r2.saveAsTextFile("features2018.csv")

// Create a rdd of double array
val rdd1 = r1.map(line=>line.split(",")).map(record=>Array(record(5).toDouble,record(6).toDouble,record(0).toDouble,record(1).toDouble,record(2).toDouble,record(3).toDouble,record(4).toDouble,record(7).toDouble))   

val rdd2 = r2.map(line=>line.split(",")).map(record=>Array(record(5).toDouble,record(6).toDouble,record(0).toDouble,record(1).toDouble,record(2).toDouble,record(3).toDouble,record(4).toDouble,record(7).toDouble))   

// Label data with delay or on time (1,0) for training
def labeledData(vals: Array[Double]): LabeledPoint = {  
LabeledPoint(if (vals(0).toDouble>=15) 1.0 else 0.0, Vectors.dense(vals.drop(1)))
}

// Get rdds of labeled data for train and test Train data is of year 2017 and Test data is of year 2018
val rddTrainlabeled = rdd1.map(labeledData)

val rddTestlabeled = rdd2.map(labeledData)


// Cache RDD
rddTrainlabeled.cache

// Cache RDD
rddTestlabeled.cache

// Normalize data
val scalerFactor = new StandardScaler(withMean = true, withStd = true).fit(rddTrainlabeled.map(x => x.features))

val normalizedTrainData = rddTrainlabeled.map(x => LabeledPoint(x.label, scalerFactor.transform(Vectors.dense(x.features.toArray))))

normalizedTrainData.cache

val normalizedTestData = rddTestlabeled.map(x => LabeledPoint(x.label, scalerFactor.transform(Vectors.dense(x.features.toArray))))

normalizedTestData.cache

// Run logistic regression on training data
val logisticregressionModel = new LogisticRegressionWithLBFGS().setNumClasses(2).run(rddTrainlabeled)

logisticregressionModel.save(sc, "lrm_model4.model")

val logisticRegressionModel = LogisticRegressionModel.load(sc, "lrm_model4.model")

// Random data to test
val test1 = sc.parallelize(Array(Array(10,10,10.4,0,0,74,62,1023)))
val test1l = test1.map(labeledData)

val test2 = sc.parallelize(Array(Array(20,20,12.2,10,0,40,30,500)))
val test2l = test2.map(labeledData)

// predict the labels for test data
val labelsLogisticRegression = rddTestlabeled.map { case LabeledPoint(label, features) => val prediction =  logisticRegressionModel.predict(features) 
(prediction, label) }

// Testing Random data 
val out1 = test1l.map { case LabeledPoint(label, features) => val prediction =  logisticRegressionModel.predict(features) 
(prediction, label) }

val out2 = test2l.map { case LabeledPoint(label, features) => val prediction =  logisticRegressionModel.predict(features) 
(prediction, label) }

out1.collect

out2.collect

// calculate accuracy
def testModel(labels: RDD[(Double, Double)]) : Tuple2[Array[Double], Array[Double]] = {                               
 val truePositive = labels.filter(r => r._1==1 && r._2==1).count.toDouble                                                  
 val trueNegative = labels.filter(r => r._1==0 && r._2==0).count.toDouble                                                
 val falsePostive = labels.filter(r => r._1==1 && r._2==0).count.toDouble                                                  
 val falseNegative = labels.filter(r => r._1==0 && r._2==1).count.toDouble                                                 
val accuracy = (truePositive+trueNegative) / (truePositive+trueNegative+falsePostive+falseNegative)
new Tuple2(Array(truePositive, trueNegative, falsePostive, falseNegative), Array( accuracy))       }      


// Get accuracy 
val (counts1, metrics1) = testModel(labelsLogisticRegression)                   

println("\n Logistic Regression Model: Accuracy = %.4f percent".format( metrics1 (0)*100))

println("\n True Positives = %.3f, True Negatives = %.3f, False Positives = %.3f, False Negatives = %.3f".format(counts1 (0), counts1 (1), counts1 (2), counts1(3)))

// SVM model
val svm = new SVMWithSGD()

// Set iterations
svm.optimizer.setNumIterations(100).setRegParam(1.0).setStepSize(1.0)

// Train SVM model
val svmmodel= svm.run(rddTrainlabeled)


// Save trained model
svmmodel.save(sc, "svm_model.model")

// Load saved model
val SVMmodel = SVMModel.load(sc, "svm_model.model")

// Random test data
val test1svm = sc.parallelize(Array(Array(0,1,10.4,0,0,74,62,1023)))
val test1svml = test1.map(labeledData)

val test2svm = sc.parallelize(Array(Array(20,20,12.2,10,0,40,30,500)))
val test2svml = test2.map(labeledData)


// Predict the labels for test data
val labelsSVM = rddTestlabeled.map { case LabeledPoint(label, features) => val prediction =  SVMmodel.predict(features) 
(prediction, label) }

// Testing random data
val out1svm = test1svml.map { case LabeledPoint(label, features) => val prediction =  SVMmodel.predict(features) 
(prediction, label) }

val out2svm = test2svml.map { case LabeledPoint(label, features) => val prediction =  SVMmodel.predict(features) 
(prediction, label) }

out1svm.collect

out2svm.collect


// Get accuracy 
val (counts2, metrics2) = testModel(labelsSVM)                   

println("\n Support Vector Machine: Accuracy = %.3f percent".format( metrics2 (0)*100))

println("\n True Positives = %.3f, True Negatives = %.3f, False Positives = %.3f, False Negatives = %.3f".format(counts2(0), counts2 (1), counts2(2), counts2(3)))       

/*
// Normalize data
val scalerFactor = new StandardScaler(withMean = true, withStd = true).fit(rddTrainlabeled.map(x => x.features))
val normalizedTrainData = rddTrainlabeled.map(x => LabeledPoint(x.label, scalerFactor.transform(Vectors.dense(x.features.toArray))))
normalizedTrainData.cache

val normalizedTestData = rdd2labeled.map(x => LabeledPoint(x.label, scalerFactor.transform(Vectors.dense(x.features.toArray))))
normalizedTestData.cache
*/

// Decision tree model
val numberOfClasses = 2
val cateFeatures = Map[Int, Int]()

// Using gini index
val index = "gini"

var maxdepth = 5
var maxbins = 50

// Train decision tree model
val decisionTreeModel = DecisionTree.trainClassifier(normalizedTrainData, numberOfClasses, cateFeatures, index, maxdepth, maxbins)

// Prediction
val labels_decisionTree = normalizedTestData.map { 
    case LabeledPoint(label, features) =>
    val prediction = decisionTreeModel.predict(features)
    (prediction, label)
}

// Get accuracy
val (counts3,metrics3) = testModel(labels_decisionTree)

println("\n Decision Tree Metrics: Accuracy = %.3f percent".format( metrics3(0)*100))

println("\n True Positives = %.3f, True Negatives = %.3f, False Positives = %.3f, False Negatives = %.3f".format(counts3(0), counts3(1), counts3(2), counts3(3)))

// Random forest model

val numberOfTrees = 20 
val featureStrategy = "auto" 
maxdepth = 5
maxbins = 64

// Train Random forest model
val randomForestModel = RandomForest.trainClassifier(rddTrainlabeled, numberOfClasses, cateFeatures,
  numberOfTrees, featureStrategy, index, maxdepth, maxbins)

// Save trained model    
randomForestModel.save(sc, "rf_model.model")     
   

// Predict labels for test data
val labels_randomForest = rddTestlabeled.map { dataPoint =>
  val prediction = randomForestModel.predict(dataPoint.features)
  (dataPoint.label, prediction)
}

// Get accuracy
val (counts4, metrics4) = testModel(labels_randomForest)
println("\n Random Forest Metrics: Accuracy = %.3f percent".format( metrics4(0)*100))
println("\n True Positives = %.3f, True Negatives = %.3f, False Positives = %.3f, False Negatives = %.3f".format(counts4(0), counts4(1), counts4(2), counts4(3)))


sc.stop()            
 }       
}

