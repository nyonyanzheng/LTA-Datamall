package bigdata.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.functions.{window, column, desc, col}
import org.apache.spark.sql.functions.{sum, count, avg, expr, concat, lit}
import org.apache.spark.sql.functions._

object bus_headway {
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("bus_headway")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()

    val df = spark.read.json("../bus_arrival_20190411_Normalizedv2.json")

//    df.show()    
//    df.printSchema()
    
    df.createOrReplaceTempView("busarrival")
    
    // busarrival1
    // Converts NextBusArrival and timestamp from String to timestamp format
    // Calculate the time difference in seconds between nextbus1 and timestamp
    // Create ID field which is BusStopCode + ServiceNo
    
    val busarrival1 = spark
      .sql("""
      SELECT 
        BusStopCode, 
        ServiceNo, 
        to_timestamp(NextBusArrival1) AS nextbus1, 
        to_timestamp(NextBusArrival2) AS nextbus2, 
        to_timestamp(NextBusArrival3) AS nextbus3, 
        to_timestamp(timestamp) AS timestamp2, 
        unix_timestamp(NextBusArrival1) - unix_timestamp(timestamp) AS ts_nextbus, 
        concat(BusStopCode,"-",ServiceNo) as ID
      FROM busarrival
      ORDER BY 
        BusStopCode, 
        ServiceNo, 
        timestamp2
      """)
    
    println("printing busarrival1") 
    busarrival1.show()
    
    busarrival1.printSchema()

    busarrival1.createOrReplaceTempView("busarrival1")
    
    // busarrival2
    // Create a rank field ("row") based on the time difference 
    // between next bus and timestamp. This figure will decrease until the
    // current bus arrived and has a sudden bus increase after that. 
    
    val busarrival2 = spark
    .sql("""
      SELECT
        ID,
        BusStopCode, 
        ServiceNo,
        nextbus1,
        timestamp2,
        ts_nextbus,  
        RANK() OVER (PARTITION BY ID ORDER BY timestamp2) AS row
      FROM busarrival1
      """) 
    
    println("printing busarrival2")   
    busarrival2.show()
    
    busarrival2.createOrReplaceTempView("busarrival2")
    
    // busarrival3
    // Creates row_dif column based on ts_nextbus field to see where the 
    // sudden increase (indicating it's a difference bus) happened. 
    
    val busarrival3 = spark
    .sql("""
      SELECT 
        t1.ID AS ID,
        t1.BusStopCode AS BusStopCode, 
        t1.ServiceNo AS ServiceNo, 
        t1.nextbus1 AS nextbus1, 
        t1.timestamp2 AS timestamp2,
        t1.ts_nextbus AS ts_nextbus1, 
        t2.ts_nextbus AS ts_nextbus2, 
        t1.row as row1, 
        t2.row as row2,
        (t2.ts_nextbus - t1.ts_nextbus) AS row_diff
      FROM busarrival2 as t1
      INNER JOIN busarrival2 as t2
      ON t1.ID = t2.ID and t1.row = t2.row+1
      """)
    
    println("printing busarrival3")  
    busarrival3.show()
    
    busarrival3.createOrReplaceTempView("busarrival3")
    
    // busarrival4 
    // Creates a lookup table to look up the bus arrival time. 
    
    val busarrival4 = spark
    .sql("""
      SELECT
        ID,
        BusStopCode, 
        ServiceNo, 
        row2 AS row
      FROM busarrival3 
      WHERE row_diff < 0
      """)
    
    println("printing busarrival4") 
    busarrival4.show()
      
    busarrival4.createOrReplaceTempView("busarrival4")
    
    // busarrival5
    // Using the lookup table, the arrival time for each bus is determined
    
    val busarrival5 = spark
    .sql("""
      SELECT 
        t2.ID AS ID,
        t2.BusStopCode, 
        t2.ServiceNo,
        t2.nextbus1 AS nextbusarrival,
        t2.timestamp2 AS timestamp, 
        RANK() OVER (PARTITION BY t2.ID ORDER BY t2.nextbus1) as row
      FROM busarrival4 AS t1 
      LEFT JOIN busarrival3 AS t2
      ON t1.ID = t2.ID and t1.row = t2.row1
      WHERE t2.ID IS NOT NULL
      ORDER BY
        t1.ID, 
        timestamp2
      """) 
    
    println("printing busarrival5") 
    busarrival5.show() 
    
    busarrival5.createOrReplaceTempView("busarrival5")
    
    // busarrival6
    // To calculate the headway between subsequent busses
    
    val busarrival6 = spark
    .sql("""
      SELECT 
        t1.ID AS ID , 
        t1.BusStopCode AS BusStopCode,
        t1.ServiceNo AS ServiceNo, 
        t2.nextbusarrival AS firstbusarrival, 
        t1.nextbusarrival AS subseqbusarrival, 
        t2.timestamp AS timestamp, 
        unix_timestamp(t1.nextbusarrival) - unix_timestamp(t2.nextbusarrival) AS headway
      FROM busarrival5 as t1
      INNER JOIN busarrival5 as t2
      ON t1.ID  = t2.ID and t1.row = t2.row+1
      ORDER BY
        t1.ID,
        timestamp
      """)
    
    println("printing busarrival6")   
    busarrival6.show() 
    
    busarrival6.createOrReplaceTempView("busarrival6")
    
    // To calculate average headway by BusService
    
    val headway_byService = spark
    .sql("""
      SELECT
        ServiceNo, 
        round(avg(headway), 0) as Average_Headway
      FROM busarrival6
      GROUP BY ServiceNo 
      """) 
    headway_byService.show()
    
    // To calculate average headway by BusStopNumber
    
    val headway_byBusStop = spark
    .sql("""
      SELECT
        BusStopCode, 
        round(avg(headway), 0) as Average_Headway
      FROM busarrival6
      GROUP BY BusStopCode
      """)
    headway_byBusStop.show()
    
    
    busarrival6.coalesce(1)
     .write
     .format("csv")
     .option("header", "true")
     .save("../bus_headways.csv")
    
    spark.stop()
    }
}