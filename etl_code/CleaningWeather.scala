import org.apache.spark.SparkContext                                                                                                                                                                                                            object CleanWeather {                                                                                                                                                                                                                           // Main function                                                                                                        def main(args: Array[String]) {                                                                                                                                                                                                                 // create spark context                                                                                                 val sc = new SparkContext() 

// Create RDD using weather for different airports
val jfkrdd = sc.textFile("/user/vsh231/project/weather_datasets/jfk_weather.csv")
val jfkrdd1 = jfkrdd.map(line => line + ",JFK")

val ordrdd = sc.textFile("/user/vsh231/project/weather_datasets/ord_weather.csv")
val ordrdd1 = ordrdd.map(line => line + ",ORD") 

val laxrdd = sc.textFile("/user/vsh231/project/weather_datasets/lax_weather.csv")
val laxrdd1 = laxrdd.map(line => line + ",LAX")

val dfwrdd = sc.textFile("/user/vsh231/project/weather_datasets/dfw_weather.csv")
val dfwrdd1 = dfwrdd.map(line => line + ",DFW")

val sfordd = sc.textFile("/user/vsh231/project/weather_datasets/sfo_weather.csv")
val sfordd1 = sfordd.map(line => line + ",SFO")

// Combine all data in one RDD   
val combined_rdd = jfkrdd1 ++ laxrdd1 ++ ordrdd1 ++ dfwrdd1 ++ sfordd1


//Replace None values with Average or 0 for particular fields
val jfkrdd2 = jfkrdd1.map(line => line.split(","))
val ny_mintemp = Array(26, 29, 36, 45, 54, 64, 69, 69, 61, 50, 42, 31)
val ny_maxtemp = Array(39, 43, 52, 64, 72, 80, 84, 84, 76, 64, 55, 44)
val ny_avgwind = Array(13.42, 13.42, 15.66, 13.42, 11.18, 11.18, 11.18, 11.18, 11.18, 11.18, 13.42, 13.42)

val jfkrddfinal = jfkrdd2.map(line => line(0) + "," + line(1) + "," +
(if(line(2) == "") ny_avgwind(line(1).split("/")(0).toInt - 1) else line(2)) + "," +
(if(line(3) == "") 0 else line(3)) + "," +
(if(line(4) == "") 0 else line(4)) + "," +
(if(line(5) == "") ny_maxtemp(line(1).split("/")(0).toInt - 1) else line(5)) + "," +
(if(line(6) == "") ny_mintemp(line(1).split("/")(0).toInt - 1) else line(6)))

val ordrdd2 = ordrdd1.map(line => line.split(","))
val chi_maxtemp = Array(32, 36, 45, 56, 66, 77, 82, 81, 74, 62, 50, 37)
val chi_mintemp = Array(22, 26, 34, 43, 53, 63, 70, 70, 62, 50, 39, 27)
val chi_avgwind = Array(14.4, 13.5, 13.1, 12.6, 10.9, 9.4, 8.7, 8.7, 10.4, 12.3, 13.7, 13.7)

val ordrddfinal = ordrdd2.map(line => line(0) + "," + line(1) + "," +
(if(line(2) == "") chi_avgwind(line(1).split("/")(0).toInt - 1) else line(2)) + "," +
(if(line(3) == "") 0 else line(3)) + "," +
(if(line(4) == "") 0 else line(4)) + "," +
(if(line(5) == "") chi_maxtemp(line(1).split("/")(0).toInt - 1) else line(5)) + "," +
(if(line(6) == "") chi_mintemp(line(1).split("/")(0).toInt - 1) else line(6)))

val dfwrdd2 = dfwrdd1.map(line => line.split(","))
val dal_maxtemp = Array(56, 61, 69, 77, 84, 92, 96, 96, 89, 79, 67, 58)
val dal_mintemp = Array(37, 41, 49, 56, 65, 73, 77, 77, 69, 58, 47, 39)
val dal_avgwind = Array(10.4, 10.8, 11.5, 11.5, 10.5, 9.3, 8.8, 8.0, 8.5, 9.6, 10.4, 10.2)

val dfwrddfinal = dfwrdd2.map(line => line(0) + "," + line(1) + "," +
(if(line(2) == "") dal_avgwind(line(1).split("/")(0).toInt - 1) else line(2)) + "," +
(if(line(3) == "") 0 else line(3)) + "," +
(if(line(4) == "") 0 else line(4)) + "," +
(if(line(5) == "") dal_maxtemp(line(1).split("/")(0).toInt - 1) else line(5)) + "," +
(if(line(6) == "") dal_mintemp(line(1).split("/")(0).toInt - 1) else line(6)))

val laxrdd2 = laxrdd1.map(line => line.split(","))
val la_maxtemp = Array(68, 69, 70, 73, 74, 79, 83, 85, 83, 79, 73, 68)
val la_mintemp = Array(49, 51, 52, 55, 58, 62, 65, 66, 65, 60, 53, 49)
val la_avgwind = Array(8.4, 7.8, 7.3, 7.2, 6.5, 5.9, 5.4, 5.2, 5.3, 6.2, 7.5, 8.4)

val laxrddfinal = laxrdd2.map(line => line(0) + "," + line(1) + "," +
(if(line(2) == "") la_avgwind(line(1).split("/")(0).toInt - 1) else line(2)) + "," +
(if(line(3) == "") 0 else line(3)) + "," +
(if(line(4) == "") 0 else line(4)) + "," +
(if(line(5) == "") la_maxtemp(line(1).split("/")(0).toInt - 1) else line(5)) + "," +
(if(line(6) == "") la_mintemp(line(1).split("/")(0).toInt - 1) else line(6)))

val sfordd2 = sfordd1.map(line => line.split(","))
val sf_maxtemp = Array(58, 61, 62, 63, 64, 67, 67, 68, 71, 70, 64, 58)
val sf_mintemp = Array(47, 48, 49, 50, 51, 53, 54, 55, 56, 55, 51, 47)
val sf_avgwind = Array(8.2, 8.7, 9.0, 9.3, 9.5, 9.4, 8.6, 8.2, 7.5, 7.2, 7.9, 8.7)

val sforddfinal = sfordd2.map(line => line(0) + "," + line(1) + "," +
(if(line(2) == "") sf_avgwind(line(1).split("/")(0).toInt - 1) else line(2)) + "," +
(if(line(3) == "") 0 else line(3)) + "," +
(if(line(4) == "") 0 else line(4)) + "," +
(if(line(5) == "") sf_maxtemp(line(1).split("/")(0).toInt - 1) else line(5)) + "," +
(if(line(6) == "") sf_mintemp(line(1).split("/")(0).toInt - 1) else line(6)))

val combinedrddfinal = jfkrddfinal ++ ordrddfinal ++ dfwrddfinal ++ laxrddfinal ++ sforddfinal
combinedrddfinal.saveAsTextFile("cleaned_weather")

// Copy the cleaned_weather from hdfs to local filesystem in weather_datasets forlder on dumbo using hdfs dfs -get cleaned_weather 
                                                                       sc.stop()                                                                                                               }                                                                                                                       }      