package bigdata.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.RegressionMetrics


object headwaysRegression {
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("headwaysClustering")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
    
      
    // Read data 
    val df = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("../bus_headways_inseconds.csv")
    
    df.show()
    df.printSchema()
    
    df.createOrReplaceTempView("headway")
    
    val formatted_data = spark
      .sql("""
      SELECT 
        unix_timestamp(to_timestamp(subseqbusarrival) ) - unix_timestamp(to_timestamp(firstbusarrival)) AS headway, 
        unix_timestamp(to_timestamp(firstbusarrival)) AS firstbus_unix
      FROM headway
      WHERE ServiceNo = "12"
      """)
      
      formatted_data.show()
      
      val assembler = new VectorAssembler()
      .setInputCols(Array("firstbus_unix"))
      .setOutputCol("features")
      
     val lr = new LinearRegression()
      .setMaxIter(100)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFeaturesCol("features")
      .setLabelCol("headway")
      
    val pipeline = new Pipeline().setStages(Array(assembler,lr))

    val Array(training, test) = formatted_data.randomSplit(Array(0.8, 0.2), seed = 12345)
    
    val pipelineModel = pipeline.fit(formatted_data)
    //val pipelineModel = pipeline.fit(training)
    
    
    
    //val fullPredictions = pipelineModel.transform(test).cache()
    //val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
    //val labels = fullPredictions.select("headway").rdd.map(_.getDouble(0))
    //val RMSE = new RegressionMetrics(predictions.zip(labels)).rootMeanSquaredError
    //println(s" ROOT: $RMSE")  
    
    
    val lrModel = pipelineModel.stages.last.asInstanceOf[LinearRegressionModel]
    
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")

    println(s"r2: ${trainingSummary.r2}")
    
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    
    

    
//    val formatted_data = df.withColumn("formatted_arrival", col("firstbusarrival").cast("timestamp"))
//    formatted_data.printSchema()
//    
//    val formatted_data2 = formatted_data.withColumn("formatted_arrival2", col("formatted_arrival").getTime())
//    
//    formatted_data.show()
    
//   
    
//    val linearRegression = new LinearRegression()
//    val linearRegressionModel = linearRegression.fit(train)
//    
    //input: time
    //y:  headways

//    val dfWithDate = df.withColumn("date", to_date($"dateString")
  }
}