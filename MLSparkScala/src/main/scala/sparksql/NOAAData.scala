package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

/*
 * NOAA data from ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/  in the by_year directory
 */

object NOAAData {
  def main(args: Array[String]): Unit = {
  val spark = SparkSession.builder().master("local[*]").appName("NOAA Data").getOrCreate()
  import spark.implicits._
  
  spark.sparkContext.setLogLevel("WARN")
  
  val tschema = StructType(Array(
      StructField("sid",StringType),
      StructField("date",DateType),
      StructField("mtype",StringType),
      StructField("value",DoubleType)
      ))
  val data2018 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("data/2018.csv").cache()
//  data2018.show()
//  data2018.schema.printTreeString()
  
  val sschema = StructType(Array(
      StructField("sid", StringType),
      StructField("lat", DoubleType),
      StructField("lon", DoubleType),
      StructField("name", StringType)
      ))
  val stationRDD = spark.sparkContext.textFile("data/ghcnd-stations.txt").map { line =>
    val id = line.substring(0, 11)
    val lat = line.substring(12, 20).toDouble
    val lon = line.substring(21, 30).toDouble
    val name = line.substring(41, 71)
    Row(id, lat, lon, name)
  }
  val stations = spark.createDataFrame(stationRDD, sschema).cache()
  
  val tmax2018 = data2018.filter($"mtype" === "TMAX").limit(1000).drop("mtype").withColumnRenamed("value", "tmax")
  val tmin2018 = data2018.filter('mtype === "TMIN").limit(1000).drop("mtype").withColumnRenamed("value", "tmin")
  val combinedTemps2018 = tmax2018.join(tmin2018, Seq("sid", "date"))
  val dailyTemp2018 = combinedTemps2018.select('sid, 'date, ('tmax + 'tmin)/20*1.8+32 as "tave")
  val stationTemp2018 = dailyTemp2018.groupBy('sid).agg(avg('tave) as "tave")
  val joinedData2018 = stationTemp2018.join(stations, "sid")
  joinedData2018.show()

  spark.stop()
}
}