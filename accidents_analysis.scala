import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.implicits._

val acc_df = spark.read.option("delimiter",",").option("header","true").option("inferschema","true").csv("C:/Users/Excalibur/Desktop/Big_data/Accidents0514.csv").na.drop(how="all")

val cas_df = spark.read.option("delimiter",",").option("header","true").option("inferschema","true").csv("C:/Users/Excalibur/Desktop/Big_data/Casualties0514.csv").na.drop(how="all")

val veh_df = spark.read.option("delimiter",",").option("header","true").option("inferschema","true").csv("C:/Users/Excalibur/Desktop/Big_data/Vehicles0514.csv").na.drop(how="all")

val selected_acc = acc_df.select("Accident_Index","Accident_Severity","Weather_Conditions")

val selected_cas = cas_df.select("Accident_Index","Casualty_Type")
val join = selected_acc.join(selected_cas.dropDuplicates("Accident_Index"), Seq("Accident_Index"), "inner")

import org.apache.spark.sql.functions.udf
val values_udf =  udf((Weather_Conditions: String) => Weather_Conditions match {
                                           case "1" => "Fine no high winds"
                                           case "2" => "Raining no high winds"
										   case "3" => "Snowing no high winds"
										   case "4" => "Fine and high winds"
										   case "5" => "Raining and high winds"
										   case "6" => "Snowing and high winds"
										   case "7" => "Fog or mist"
										   case "8" => "Other"
										   case "9" => "Unknown"
										   case "-1" => "Unknown"
                             })


val values_acc = udf((Accident_Severity: String)=> Accident_Severity match{
                 case "1" => "Fatal"
				 case "2" => "Serious"
				 case "3" => "Slight"
})

val values_cas =  udf((Casualty_Type: String) => Casualty_Type match {
										   case "0" => "Pedestrian"
                                           case "1" => "Cyclist"
                                           case "2" => "Motorcycle 50cc"
										   case "3" => "Motorcycle 125cc"
										   case "4" => "Motorcycle over 125cc"
										   case "5" => "Motorcycle over 500cc"
										   case "8" => "Taxi/Private hire car occupant"
										   case "9" => "Car occupant"
										   case "10" => "Minibus Occupants"
										   case "11" => "Bus Occupants"
										   case "16" => "Horse rider"
										   case "17" => "Agricultural vehicle occupant"
                                           case "18" => "Tram occupant"
										   case "19" => "Goods vehicle occupant"
										   case "20" => "Goods vehicle occupant"
										   case "21" => "Goods vehicle occupant"
										   case "22" => "Mobility scooter rider"
										   case "23" => "Electric motorcycle rider or passenger"
										   case "90" => "Other vehicle occupant"
										   case "97" => "Motorcycle - unknown cc rider or passenger"
										   case "98" => "Goods vehicle (unknown weight) occupant"

})
							 
val accident_analysis = join.withColumn("Weather_Conditions", values_udf($"Weather_Conditions")).withColumn("Accident_Severity", values_acc($"Accident_Severity")).withColumn("Casualty_Type", values_cas($"Casualty_Type"))

accident_analysis.where($"Accident_Severity"==="Fatal").groupBy($"Weather_Conditions").count.sort(desc("count")).show(10)
accident_analysis.where($"Accident_Severity"==="Serious").groupBy($"Weather_Conditions").count.sort(desc("count")).show(10)
accident_analysis.where($"Accident_Severity"==="Slight").groupBy($"Weather_Conditions").count.sort(desc("count")).show(10)

val acc_analysis = accident_analysis.groupBy($"Accident_Severity",$"Weather_Conditions").count
//acc_analysis.coalesce(1).write.option("header","true").csv("C:/Users/Excalibur/Documents/acc_analysis")

//Weather has no influence on the accident severity

//Casuality Analysis

accident_analysis.where($"Accident_Severity"==="Fatal").groupBy($"Casualty_Type").count.sort(desc("count")).show(10)
accident_analysis.where($"Accident_Severity"==="Serious").groupBy($"Casualty_Type").count.sort(desc("count")).show(10)
accident_analysis.where($"Accident_Severity"==="Slight").groupBy($"Casualty_Type").count.sort(desc("count")).show(10)

val cas_analysis = accident_analysis.groupBy($"Accident_Severity",$"Casualty_Type").count
//acc_analysis.coalesce(1).write.option("header","true").csv("C:/Users/Excalibur/Documents/cas_analysis")

//Most casualities include car occupants and pedestrians in all 3 severities

//Time Series Analysis

import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp}

val time_analysis = acc_df.select($"Accident_Severity",$"Date",$"Time",$"Day_of_Week")

//Year wise analysis of accidents

val year_analysis = time_analysis.withColumn("year", from_unixtime(unix_timestamp($"Date", "dd/MM/yy"), "YYYY")).withColumn("month", from_unixtime(unix_timestamp($"Date", "dd/MM/yy"), "MMMM"))

year_analysis.groupBy($"year").count.sort(desc("count")).show(10)

val year_analysis = accident_analysis.groupBy($"year",$"month").count
//year_analysis.coalesce(1).write.option("header","true").csv("C:/Users/Excalibur/Documents/year_analysis")

//The number of accdients have decreases over the years with highest number of accidients appeared in 2005 and lowest in 2013. In year 2014 number of accdients had a slight increase than 2012 and 2013
//Month analysis

year_analysis.groupBy($"month").count.sort(desc("count")).show(10)

//Hour analysis

val hour = year_analysis.select(col("*"), substring_index($"Time", ":", 1).as("hour"))
val hour_analysis = hour.groupBy($"hour").count.sort("count")

//hour_analysis.coalesce(1).write.option("header","true").csv("C:/Users/Excalibur/Documents/hour_analysis")

//Most number of accidents happens between 3 to 6 in the evening and 8 to 9 in the morning.This is more likely due to people most often commute to work and back to home at these hours.

  




