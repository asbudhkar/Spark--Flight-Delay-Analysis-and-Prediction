# Code for running the Analytic script
import os


# Remove already existing saved models and results
os.system("cd src")

os.system("hdfs dfs -rm -r lrm_model4.model")   

os.system("hdfs dfs -rm -r svm_model.model")  

os.system("hdfs dfs -rm -r rf_model.model")  
 
os.system("hdfs dfs -rm -r joinedDataToAnalyse.csv")   

os.system("hdfs dfs -rm -r featuresCleaned")

os.system("hdfs dfs -rm -r lro") 

os.system("hdfs dfs -rm -r result4")

# Run spark submit to perform the Analytic
os.system("spark2-submit --class AnalyticCode3 --master yarn --deploy-mode client  --driver-memory 5G --executor-memory 2G --num-executors 90 target/scala-2.11/sbt_test_2.11-0.1.0-SNAPSHOT.jar myfile.txt") 
