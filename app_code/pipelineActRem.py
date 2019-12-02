# Code for running the actuation/remediation script

import os

os.system("cd src")

# Put the test file in HDFS which contains features for ML model Remove file if already exists
os.system("hdfs dfs -rm -r myfile.txt")   

os.system("hdfs dfs -put ../myfile.txt myfile.txt")  

# Remove saved results
os.system("hdfs dfs -rm -r lro")

os.system("hdfs dfs -rm -r result4")

os.system("hdfs dfs -rm -r svm")

os.system("hdfs dfs -rm -r rf")

os.system("hdfs dfs -rm -r resultSVM")

os.system("hdfs dfs -rm -r resultRF")

os.system("hdfs dfs -rm -r reslabelRF")

os.system("rm ressvm.txt")

os.system("rm resrf.txt")

os.system("rm res.txt")

os.system("hdfs dfs -rm -r reslabel")

# Run the Actuation/Remediation code
os.system("spark2-submit --class Actuation --master yarn --deploy-mode client  --driver-memory 5G --executor-memory 2G --num-executors 90 target/scala-2.11/sbt_test_2.11-0.1.0-SNAPSHOT.jar myfile.txt") 

# Get the result
os.system("hdfs dfs -get reslabel/part-00000 res.txt")
       
os.system("hdfs dfs -cat rf/part-00000") 
