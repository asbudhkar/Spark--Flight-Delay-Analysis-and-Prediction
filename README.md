# Spark--Flight-Delay-Analysis-and-Prediction
Big Data project to analyse the factors affecting flight departure delays at few US airports

## Data Sources Used
1. Data Source Used: National Centers for Environmental Information, National Oceanic and Atmospheric Administration Link
https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/
 
2. Data Source Used: Bureau of Transportation Statistics – Airline On-Time Performance Data  Reporting Carrier On-Time Performance (1987-present)
https://transtats.bts.gov/Tables.asp?DB_ID=120&DB_Name=Airline%20On-Time%20Performance%20Data&DB_Short_Name=On-Time

## NYU HPC cluster was used 
## Folders 
### 1. data_ingest
It contains commands to ingest data n HDFS

### 2. etl_code
It contains code for cleaning datasets

### 3. profiling_code
Columns for all datasets were profiled to get min/max values, number of distinct values and max length of each column. 
This is important to find inconsistencies and missing values in the datasets

### 4. app_code
#### Contains code for Analysis
#### Sql queries to understand the data distribution
1. Airport wise total records and delay record
2. Month wise delays for each airport
3. Flight Distance wise delays

#### Machine Learning models using MLlib
1. LogisticRegression
2. Decision Tree
3. Random Forest
4. Support Vector Machine

#### Flask web application
Basic web app to enable the user to check prediction about his upcoming flight

#### NOAA SDK
SDK was used to get forecasts for a given airport and date which were used as features to get prediction about delay

### 5. act_rem_code
A web applicaiton for user was created which takes the flight date-time and airport code and tells the user the chance of flight being delayed. Weather data for the time of flight is scraped from NOAA forecasts.

### 6. ReadMe.txt
It contains instructions to run the code on NYU HPC cluster

## Spark Ecosystem Tool
Scala-Spark, Spark MLlib, Spark SQL


