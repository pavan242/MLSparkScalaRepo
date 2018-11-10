package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

object ConsumptionDF {
   def main(args: Array[String]): Unit = {
   val spark = SparkSession.builder().master("local[*]").appName("Nike Sales DF").getOrCreate()
  
   spark.sparkContext.setLogLevel("WARN")
  
   import spark.implicits._
  
   val salSchema = StructType(Array(
       StructField("saleID",IntegerType),
       StructField("netSales",DoubleType),
       StructField("salesUnits",IntegerType),
       StructField("storeID",IntegerType),
       StructField("dateID",IntegerType),
       StructField("productID",LongType)
       ))

   val calSchema = StructType(Array(
       StructField("dateID",IntegerType),
       StructField("dateYear",IntegerType),
       StructField("weekNumber",IntegerType)
       ))
  
   val salesDF = spark.read.schema(salSchema).option("header", "true").csv("data/sales.csv").
                 select("saleID", "storeID", "dateID", "productID", "salesUnits", "netSales")
   //salesDF.show()
                
   val calendarRDD = spark.sparkContext.textFile("data/calendar.csv").
                     filter(!_.contains("datekey")).map {
                     line => val p = line.split(",").map(_.trim)
                     Row(p(0).toInt, p(2).toInt, p(3).toInt)
                     }
   val calendarDF = spark.createDataFrame(calendarRDD, calSchema)
   //calendarDF.show()

   val joinedSalesGroup =  salesDF.as("s").join(calendarDF.as("c")).
                            where($"s.dateID" === $"c.dateID").drop($"c.dateID").
                            select(concat(lit("Y"), ($"c.dateYear"%1000).alias("Year").cast(StringType), lit("W"), ($"c.weekNumber").cast(StringType)).alias("ID"), $"s.storeID", $"s.productId", $"s.salesUnits", $"s.netSales")
                            //select(($"c.dateYear"%1000).alias("ID"), $"s.storeID", $"s.productId", $"s.salesUnits", $"s.netSales")
   /*val joinedSalesGroup =  salesDF.join(calendarDF, salesDF("dateID") === calendarDF("dateID"), "inner").
                                     drop(calendarDF("dateID"))*/
   //val df = joinedSalesGroup.select(($"dateYear"%1000).alias("Year"), $"weekNumber", $"storeID", $"productId", $"salesUnits", $"netSales")
   //val df = joinedSalesGroup.select(concat(lit("Y"), ($"dateYear"%1000).alias("Year").cast(StringType), lit("W"), ($"weekNumber").cast(StringType)).alias("ID"), $"storeID", $"productId", $"salesUnits", $"netSales")

   joinedSalesGroup.show()
   //df.show()
   spark.stop()
  }
}