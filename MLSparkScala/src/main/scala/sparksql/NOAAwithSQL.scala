package sparksql

import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row

/*
 * NOAA data from ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ in the by_year directory
 */

object NOAAwithSQL {
  def main(args: Array[String]): Unit = {
  val spark = SparkSession.builder().master("local[*]").appName("NOAA Data").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val tschema = StructType(Array(
    StructField("sid", StringType),
    StructField("date", DateType),
    StructField("mtype", StringType),
    StructField("value", DoubleType)))
  val data2018 = spark.read.schema(tschema).option("dateFormat", "yyyyMMdd").csv("data/2018.csv").cache()
  //  data2018.show()
  //  data2018.schema.printTreeString()

  val sschema = StructType(Array(
    StructField("sid", StringType),
    StructField("lat", DoubleType),
    StructField("lon", DoubleType),
    StructField("name", StringType)))
  val stationRDD = spark.sparkContext.textFile("data/ghcnd-stations.txt").map { line =>
    val id = line.substring(0, 11)
    val lat = line.substring(12, 20).toDouble
    val lon = line.substring(21, 30).toDouble
    val name = line.substring(41, 71)
    Row(id, lat, lon, name)
  }
  val stations = spark.createDataFrame(stationRDD, sschema).cache()

  data2018.createOrReplaceTempView("data2018")
  stations.createOrReplaceTempView("stations")
  val pureSQL = spark.sql("""
    Select * FROM
      (SELECT sid, AVG((tmax+tmin)/20*1.8+32) as tave
      FROM
        (SELECT sid, date, value as tmax FROM data2018 WHERE mtype="TMAX" LIMIT 1000000)
        JOIN
        (SELECT sid, date, value as tmin FROM data2018 WHERE mtype="TMIN" LIMIT 1000000)
        USING (sid, date)
      GROUP BY sid)
    JOIN stations USING (sid)
    """)
  pureSQL.show()

    }
}