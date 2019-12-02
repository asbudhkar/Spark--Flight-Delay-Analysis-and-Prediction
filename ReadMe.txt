# Files are stored in following directories

1) A zip file SBTProject containing source code for the application, cleaning, profiling, class files, jar, etc. in a /app_code directory
2) Code files for application in /app_code directory provided just to distinguish the application code 
3) Code and commands for data ingestion in a /data_ingest directory
4) ETL in a /etl_code directory
5) Profiling code in a /profiling_code directory
6) Test code and unused code in a /test_code directory
7) Code related to actuation or remediation steps in a /act_rem_code directory

8) SBTProject contains the directories as per the reuirement of sbt 
a) src/scala/main contains all scala code files
b) src/target/scala-2.11 contains jar file and classes
c) src/project contains build files
d) build.sbt is the configuration file

# Inside the SBTProject zip in src/src/main/scala/ 
Profiling, Cleaning, Analytic1, Analytic2 and Actuation/Remediation code is in src/main/scala/ folder in files ProfilingFlight.scala, CleaningFlight.scala, Analytic1.scla, Analytic2.scala, ActRem.scala

Data is stored in HDFS /user/asb862/flightData/ and /home/vsh231/weather_datasets as explained in data ingestion 
Commands in the files can be run on spark2-shell

To run using sbt:

Log in to dumbo
Unzip SBTCode folder. Go in the src folder where build.sbt is present
module load sbt/1.2.8
1) sbt compile

2) sbt package (.jar file gets created in target/scala-2.11/ folder) 

3) For Flight Profiling Code
spark2-submit --class ProfileFlightData target/scala-2.11/sbt_test_2.11-0.1.0-SNAPSHOT.jar (name of jar file created)

3) For Weather Profiling code
Run the commands in the file ProfilingWeather.scala in etl_code folder in Spark REPL shell

4) For Flight Cleaning Code 
spark2-submit --class CleanFlightData target/scala-2.11/sbt_test_2.11-0.1.0-SNAPSHOT.jar

4) For Weather Cleaning Code
Run the commands in the file CleaningWeather.scala in etl_code folder in Spark REPL shell

5i) For Analytic code
python pipelineAnalytic.py
This will save the trained ML models used later for actuation/remediation code

5ii) For Regression Analytic
hdfs -dfs -rm -r RegRes
spark2-submit --class Analytic1Code --master yarn --deploy-mode client  --driver-memory 5G --executor-memory 2G --num-executors 100 target/scala-2.11/sbt_test_2.11-0.1.0-SNAPSHOT.jar 

6) For actuation/remediation - In reposnse to the model prediction of flight delay the user will be informed via web applicaiton whether the flight will be delayed. 
Run the flask application on personal machine localhost to get data from user (see below)

7) For actuation code (myfile.txt file needs to be present in /home/netid. For that refer to running flask application )
python pipelineActRem.py


*) Running flask application (For submitting the code flask directory is already present with virtual env created for testing)
Flask application is provided to get data from user

On personal machine:

1) mkdir flask
2) cd flask
3) create virtual environment - virtualenv flask
4) activate the virtual env - flask/Scripts/activate (For windows)
5) Copy test.py, main.py, templates folder and static folder from flask folder in SBTCode in the flask folder created
6) pip install requests=2.21.0 
   pip install flask

7) NOAA sdk needs to be installed to get weather forecasts
   pip install noaa-sdk

On local machine
8) python main.py
   Run the application on localhost port provided by flask
   Enter data
   When submit is clicked test.py script runs to get weather data and save to file on hdfs in /home/asb862 directory. Please change the netid while testing in test.py file last line.
   It will then wait for user command before showing the result

9) On dumbo 
   Run the actuation code - python pipelineActRem.py. This will test the input and return result

10) On local machine
    Press any key to display the result on the flask applicaiton web page. 
*Please enter the dumbo conenction password to send input and get results.

All intermediate results and sql results are stored in files or hive tables mentioned in the code comments

*) Sequence of operations
First Profiling, Cleaning then Analytic. After running flask application Actuation/Remediation code is to be executed.