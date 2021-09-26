package main

import ml.combust.bundle.BundleFile
import ml.combust.mleap.spark.SparkSupport._
import ml.combust.mleap.runtime.MleapSupport._

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.mleap.SparkUtil
import resource._


/*
 * Dataset schema
   Age
   Height
   Weight
   Sex
   Alertness
   Supplimentary
   SPO2
   RR
   T
   SBP
   HR
   LevelAlertness
   LevelSupplimentary
   LevelSPO2
   LevelRR
   LevelT
   LevelSBP
   LevelHR
   Status
   SendNotification
   Results
*/

object VitalSigns {

  case class Values(age: Integer, height: Double, weight: Double, sex: String, alertness: String, supplimentary: String, spo2: Integer, rr: Integer, t: Double, sbp: Integer,hr: Integer, levelalertness: String, levelsupplimentary: String, levelspo2: String, levelrr: String, levelt: String, levelsbp: String, levelhr: String, status: String, notification: String, results: String)
  val schema = StructType(Array(
    StructField("age", IntegerType, true),
    StructField("height", DoubleType, true),
    StructField("weight", DoubleType, true),
    StructField("sex", StringType, true),
    StructField("alertness", StringType, true),
    StructField("supplimentary", StringType, true),
    StructField("spo2", IntegerType, true),
    StructField("rr", IntegerType, true),
    StructField("t", DoubleType, true),
    StructField("sbp", IntegerType, true),
    StructField("hr", IntegerType, true),
    StructField("levelalertness", StringType, true),
    StructField("levelsupplimentary", StringType, true),
    StructField("levelspo2", StringType, true),
    StructField("levelrr", StringType, true),
    StructField("levelt", StringType, true),
    StructField("levelsbp", StringType, true),
    StructField("levelhr", StringType, true),
    StructField("status", StringType, true),
    StructField("notification", StringType, true),
    StructField("results", StringType, true)
  ))

  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder().appName("vitalsigns").getOrCreate()

    import spark.implicits._

    // we split our data 80% for training 20% for testing
    // csv path and file names are hard coded, the data sets are not found in the same repo as they are not open sourced 
    val train: Dataset[Values] = spark.read.option("inferSchema", "false").schema(schema).csv("/home/lzuccarelli/Projects/sparkml-projects/data/vs-bigml-80.csv").as[Values]
    train.take(1)
    train.cache
    println(train.count)

    val test: Dataset[Values] = spark.read.option("inferSchema", "false").schema(schema).csv("/home/lzuccarelli/Projects/sparkml-projects/data/vs-bigml-20.csv").as[Values]
    test.take(2)
    println(test.count)
    test.cache

    train.printSchema()
    train.show
    train.createOrReplaceTempView("values")
    spark.catalog.cacheTable("values")

    train.groupBy("status").count.show
    val fractions = Map("normal" -> 1.0, "critical" -> 1.0, "warning" -> 1.0)
    //Here we're keeping all instances of the Status=critical class, but downsampling the Status=normal class to a fraction.
    val strain = train.stat.sampleBy("status", fractions, 36L)

    strain.groupBy("status").count.show
    // The following fields can be dropped. 
    // 1. They are not used in the NEWS matrix 
    // 2. Fields that are used supplimentary oxygen and alertness are all the same in this sample (N and A)
    // 3. The level fields are used for status reporting
    val ntrain = strain.drop("height").drop("weight").drop("alertness").drop("supplimentary").drop("levelalertness").drop("levelsupplimentary").drop("levelsbp").drop("levelrr").drop("levelspo2").drop("levelt").drop("levelhr").drop("notification").drop("results")
    println(ntrain.count)
    ntrain.show

    val ipindexer = new StringIndexer().setInputCol("sex").setOutputCol("isex")
    val labelindexer = new StringIndexer().setInputCol("status").setOutputCol("label")
    val featureCols = Array("age","isex","sbp", "rr", "spo2", "t", "hr")

    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")

    val dTree = new DecisionTreeClassifier().setLabelCol("label").setFeaturesCol("features")

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(ipindexer,labelindexer, assembler, dTree))
    // Search through decision tree's maxDepth parameter for best model
    val paramGrid = new ParamGridBuilder().addGrid(dTree.maxDepth, Array(2, 3, 4, 5, 6, 7)).build()

    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("prediction")

    // Set up 3-fold cross validation
    val crossval = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(3)

    val cvModel = crossval.fit(ntrain)

    val bestModel = cvModel.bestModel
    println("The Best Model and Parameters:\n--------------------")
    println(bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages(3))
    bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages(3).extractParamMap

    val treeModel = bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages(3).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)

    val predictions = cvModel.transform(test)
    val accuracy = evaluator.evaluate(predictions)
    evaluator.explainParams()

    val predictionAndLabels = predictions.select("prediction", "label").rdd.map(x =>
      (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    println("area under the precision-recall curve: " + metrics.areaUnderPR)
    println("area under the receiver operating characteristic (ROC) curve : " + metrics.areaUnderROC)

    println(metrics.fMeasureByThreshold())

    val result = predictions.select("label", "prediction", "probability")
    result.show

    val lp = predictions.select("label", "prediction")
    val counttotal = predictions.count()
    val correct = lp.filter($"label" === $"prediction").count()
    val wrong = lp.filter(not($"label" === $"prediction")).count()
    val ratioWrong = wrong.toDouble / counttotal.toDouble
    val ratioCorrect = correct.toDouble / counttotal.toDouble
    val truep = lp.filter($"prediction" === 0.0).filter($"label" === $"prediction").count() / counttotal.toDouble
    val truen = lp.filter($"prediction" === 1.0).filter($"label" === $"prediction").count() / counttotal.toDouble
    val falsep = lp.filter($"prediction" === 1.0).filter(not($"label" === $"prediction")).count() / counttotal.toDouble
    val falsen = lp.filter($"prediction" === 0.0).filter(not($"label" === $"prediction")).count() / counttotal.toDouble

    println("counttotal : " + counttotal)
    println("correct : " + correct)
    println("wrong: " + wrong)
    println("ratio wrong: " + ratioWrong)
    println("ratio correct: " + ratioCorrect)
    println("ratio true positive : " + truep)
    println("ratio false positive : " + falsep)
    println("ratio true negative : " + truen)
    println("ratio false negative : " + falsen)

    println("wrong: " + wrong)

    val equalp = predictions.selectExpr(
      "double(round(prediction)) as prediction", "label",
      """CASE double(round(prediction)) = label WHEN true then 1 ELSE 0 END as equal"""
    )
    equalp.show

      
    // create zip model for scoring   
    implicit val context = SparkBundleContext().withDataset(predictions)

    for(bf <- managed(BundleFile("jar:file:/home/lzuccarelli/Data/vitalsign.model.zip"))) {
      cvModel.writeBundle.save(bf)(context).get
    }

  }

  // System.exit(0)
}

