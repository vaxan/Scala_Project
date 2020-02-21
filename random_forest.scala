import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.implicits._

val acc_df = spark.read.option("delimiter",",").option("header","true").option("inferschema","true").csv("C:/Users/Excalibur/Desktop/Big_data/Accidents0514.csv").na.drop(how="all")

val cas_df = spark.read.option("delimiter",",").option("header","true").option("inferschema","true").csv("C:/Users/Excalibur/Desktop/Big_data/Casualties0514.csv").na.drop(how="all")

val veh_df = spark.read.option("delimiter",",").option("header","true").option("inferschema","true").csv("C:/Users/Excalibur/Desktop/Big_data/Vehicles0514.csv").na.drop(how="all")

val selected_acc = acc_df.select($"Accident_Index", $"Accident_Severity", $"Road_Type", $"Number_of_Vehicles", $"Light_Conditions", $"Road_Surface_Conditions", $"Urban_or_Rural_Area")

val selected_cas = cas_df.select($"Accident_Index", $"Casualty_Class", $"Casualty_Severity", $"Casualty_Type")

val selected_veh = veh_df.select($"Accident_Index", $"Vehicle_Type", $"Vehicle_Manoeuvre", $"Skidding_and_Overturning", $"Journey_Purpose_of_Driver")

val joined_all = selected_acc.join(selected_cas.dropDuplicates("Accident_Index"), Seq("Accident_Index"), "inner").join(selected_veh.dropDuplicates("Accident_Index"), Seq("Accident_Index"), "inner")

val featureCol = Array("Road_Type", "Light_Conditions","Road_Surface_Conditions", "Accident_Severity", "Urban_or_Rural_Area", "Casualty_Type", "Vehicle_Type", "Vehicle_Manoeuvre", "Vehicle_Manoeuvre", "Skidding_and_Overturning", "Journey_Purpose_of_Driver")

val assembler = new VectorAssembler().setInputCols(featureCol).setOutputCol("features").setHandleInvalid("skip")

val random_df = assembler.transform(joined_all)

val splitSeed = 5043

val Array(trainingData, testData) = random_df.randomSplit(Array(0.7, 0.3), splitSeed)

val classifier = new RandomForestClassifier().setLabelCol("Casualty_Class").setImpurity("gini").setMaxDepth(5).setNumTrees(20).setFeatureSubsetStrategy("auto").setSeed(5043)

val model = classifier.fit(trainingData)

val predictions = model.transform(testData)

val evaluator = new MulticlassClassificationEvaluator().setLabelCol("Casualty_Class").setPredictionCol("prediction").setMetricName("accuracy")

val accuracy = evaluator.evaluate(predictions)

predictions.groupBy($"prediction",$"Casualty_Class").count.show

model.featureImportances

val importantFeature = Vectors.sparse(11,Array(0,1,3,4,5,6,7,8,9,10),Array(3.443946579881289E-5,9.955376368308783E-6,6.558430156783289E-4,0.021572412058343968,0.9008309723195277,0.04853140524991629,0.014771097389394795,0.006097193549055519,0.004339379149915114,0.0031573024260012246))

importantFeature.toArray.zipWithIndex.map(_.swap).sortBy(-_._2).foreach(a => println(a._1 + " -> " + a._2))
