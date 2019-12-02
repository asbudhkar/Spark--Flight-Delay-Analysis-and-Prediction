// Code for Actuation/Remediation

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
import org.apache.spark.sql.types._ 
import org.apache.spark.sql.SQLContext


object Actuation {                                                                                                  

// Main function
def main(args: Array[String]) {

// read file name
val filename = args(0)

// create spark context
val sc = new SparkContext()    

// create sqlcontext
val sqlContext = new SQLContext(sc) 

// create RDD from input file
val input = sc.textFile(filename.toString)

// Get the features
val inputFeatures=input.map(line=>line.split(",")).map(record=>Array(record(0).toDouble,record(1).toDouble,record(2).toDouble,record(3).toDouble,record(4).toDouble,record(5).toDouble,record(6).toDouble,record(7).toDouble))  

// Load the saved logistic regression model
val logisticRegressionModel = LogisticRegressionModel.load(sc, "lrm_model4.model")

// Code for labeling
def labeledData(vals: Array[Double]): LabeledPoint = {  
LabeledPoint(if (vals(0).toDouble>=15) 1.0 else 0.0, Vectors.dense(vals.drop(1)))
}

// get labled data
val testData = inputFeatures.map(labeledData)

// predict the labels for test data
val outData = testData.map { case LabeledPoint(label, features) => val prediction =  logisticRegressionModel.predict(features) 
(prediction, label) } 

// Save result to file
outData.saveAsTextFile("lro")

// Get the predicted label
val labelPredicted = outData.map(line=>(1,line._1.toDouble))

// Get the input features
val inputData=input.map(line=>line.split(",")).map(record=>(1,Array(record(0).toDouble,record(1).toDouble,record(2).toDouble,record(3).toDouble,record(4).toDouble,record(5).toDouble,record(6).toDouble,record(7).toDouble)))

// Join to get all features and predicted label
val join = inputData.join(labelPredicted)

// Get the result
val res = join.map(line=>line._2).map(line=>line._1(0)+","+line._1(1)+","+line._1(2)+","+line._1(3)+","+line._1(4)+","+line._1(5)+","+line._1(6)+","+line._1(7)+","+line._2) 

// Save predicted label
val resOut = outData.map(line=>line._1.toDouble)

resOut.saveAsTextFile("reslabel")

// Save result in file
res.saveAsTextFile("result4")

// For visualization

// Drop table if exists
sqlContext.sql("DROP TABLE IF EXISTS asb862.result2") 

// Create a data frame from result
val df = sqlContext.read.format("csv").option("inferSchema","true").load("result4")

// save the result as table to visualize in Tableau
df.write.saveAsTable("asb862.result2")
print(df.printSchema())  


// Load SVM model
val SVMmodel = SVMModel.load(sc, "svm_model.model")

// predict the labels for test data   
val outDataSVM = testData.map { case LabeledPoint(label, features) => val prediction =  SVMmodel.predict(features)
(prediction, label) }

// Save result to file 
outDataSVM.saveAsTextFile("svm")

// Get the predicted label 
val labelPredictedSVM = outDataSVM.map(line=>(1,line._1.toDouble)) 

// Get the input features
val inputDataSVM=input.map(line=>line.split(",")).map(record=>(1,Array(record(0).toDouble,record(1).toDouble,record(2).toDouble,record(3).toDouble,record(4).toDouble,record(5).toDouble,record(6).toDouble,record(7).toDouble)))       
val joinSVM = inputDataSVM.join(labelPredicted)

// Get the result
val resSVM = joinSVM.map(line=>line._2).map(line=>line._1(0)+","+line._1(1)+","+line._1(2)+","+line._1(3)+","+line._1(4)+","+line._1(5)+","+line._1(6)+","+line._1(7)+","+line._2)

// Save predicted label
val resOutSVM = outDataSVM.map(line=>line._1.toDouble)

// Save result in file 
resSVM.saveAsTextFile("resultSVM") 

// For visualization 

// Drop table if exists  
sqlContext.sql("DROP TABLE IF EXISTS asb862.resultSVM")  

// Create a data frame from result  
val dfsvm = sqlContext.read.format("csv").option("inferSchema","true").load("resultSVM") 

// save the result as table to visualize in Tableau
dfsvm.write.saveAsTable("asb862.resultSVM")   
print(dfsvm.printSchema())       


// Parameters for random forest model
val numberOfClasses = 2  
val cateFeatures = Map[Int, Int]()
val index = "gini"
val numberOfTrees = 10 
val featureSubsetStrategy = "auto" 
val maxdepth = 10
val maxbins = 48

// Load Random forest model
val RFmodel = RandomForestModel.load(sc, "rf_model.model")
 
// predict the labels for test data
val outDataRF = testData.map { case LabeledPoint(label, features) => val prediction =  RFmodel.predict(features) 
(prediction, label) } 

// Save result to file 
outDataRF.saveAsTextFile("rf")
 
// Get the predicted label 
val labelPredictedRF = outDataRF.map(line=>(1,line._1.toDouble))

// Get the input features
val inputDataRF=input.map(line=>line.split(",")).map(record=>(1,Array(record(0).toDouble,record(1).toDouble,record(2).toDouble,record(3).toDouble,record(4).toDouble,record(5).toDouble,record(6).toDouble,record(7).toDouble)))   

val joinRF= inputDataRF.join(labelPredictedRF)                       

// Get the result
val resRF = joinRF.map(line=>line._2).map(line=>line._1(0)+","+line._1(1)+","+line._1(2)+","+line._1(3)+","+line._1(4)+","+line._1(5)+","+line._1(6)+","+line._1(7)+","+line._2)

// Save predicted label 
val resOutRF = outDataRF.map(line=>line._1.toDouble)
 
resOutRF.saveAsTextFile("reslabelRF")

// Save result in file
resRF.saveAsTextFile("resultRF")

// For visualization

// Drop table if exists
sqlContext.sql("DROP TABLE IF EXISTS asb862.resultRF")

// Create a data frame from result
val dfrf = sqlContext.read.format("csv").option("inferSchema","true").load("resultRF")

// save the result as table to visualize in Tableau 
dfrf.write.saveAsTable("asb862.resultRF")    
print(dfrf.printSchema())      

sc.stop()            
 }       
}

