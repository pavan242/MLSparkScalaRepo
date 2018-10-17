package sparkml

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.LinearRegression
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/*
 * NOAA data from ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/  in the by_year directory
 */

case class Station(sid: String, lat: Double, lon: Double, elev: Double, name: String)
case class NOAAData(sid: String, date: java.sql.Date, measure: String, value: Double)
case class ClusterData(num: Int, lat: Double, lon: Double, latstd: Double, lonstd: Double,
  tmax: Double, tmin: Double, tmaxstd: Double, tminstd: Double, precip: Double,
  tmaxSeasonalVar: Double, tminSeasonalVar: Double)

object NOAAClustering {
  //  Future {
  def main(args: Array[String]): Unit = {
  val spark = SparkSession.builder().master("local[*]").appName("NOAA Data").getOrCreate()
  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  val stations = spark.read.textFile("data/ghcnd-stations.txt").map { line =>
    val id = line.substring(0, 11)
    val lat = line.substring(12, 20).trim.toDouble
    val lon = line.substring(21, 30).trim.toDouble
    val elev = line.substring(31, 37).trim.toDouble
    val name = line.substring(41, 71)
    Station(id, lat, lon, elev, name)
  }.cache()

  val stationsVA = new VectorAssembler().setInputCols(Array("lat", "lon")).setOutputCol("location")
  val stationsWithLoc = stationsVA.transform(stations)
  //  stationsWithLoc.show()

  val kMeans = new KMeans().setK(2000).setFeaturesCol("location").setPredictionCol("cluster")
  val stationClusterModel = kMeans.fit(stationsWithLoc)

  val stationsWithClusters = stationClusterModel.transform(stationsWithLoc)
  stationsWithClusters.show()

  //  println(kMeans.explainParams())

  val data2017 = spark.read.schema(Encoders.product[NOAAData].schema).
    option("dateFormat", "yyyyMMdd").csv("data/2017.csv")

  val joinedData = data2017.join(stationsWithClusters, "sid").cache()

  def calcSeasonalVar(df: DataFrame): Double = {
    val withDOYinfo = df.withColumn("doy", dayofyear('date)).
      withColumn("doySin", sin('doy / 365 * 2 * math.Pi)).
      withColumn("doyCos", cos('doy / 365 * 2 * math.Pi))
    val linearRegData = new VectorAssembler().setInputCols(Array("doySin", "doyCos")).
      setOutputCol("doyTrig").transform(withDOYinfo).cache()
    val linearReg = new LinearRegression().setFeaturesCol("doyTrig").setLabelCol("value").
      setMaxIter(10).setPredictionCol("pmaxTemp")
    val linearRegModel = linearReg.fit(linearRegData)
    math.sqrt(linearRegModel.coefficients(0) * linearRegModel.coefficients(0) +
      linearRegModel.coefficients(1) * linearRegModel.coefficients(1))
  }

  def calcClusterData(df: DataFrame, cluster: Int): Option[ClusterData] = {
    println("Calc for cluster "+cluster)
    val filteredData = df.filter('cluster === cluster).cache()
    val tmaxs = filteredData.filter('measure === "TMAX").cache()
    val tmins = filteredData.filter('measure === "TMIN").cache()
    val precips = filteredData.filter('measure === "PRCP").cache()
    val cd = if (tmaxs.count() < 20 || tmins.count() < 20 || precips.count() < 3) None else {
      val latData = filteredData.agg(avg('lat) as "lat", stddev('lat) as "latstd")
      val lat = latData.select('lat).as[Double].first()
      val latstd = latData.select('latstd).as[Double].first()
      val lonData = filteredData.agg(avg('lon) as "lon", stddev('lon) as "lonstd")
      val lon = lonData.select('lon).as[Double].first()
      val lonstd = lonData.select('lonstd).as[Double].first()
      val tmaxData = tmaxs.agg(avg('value) as "tmax", stddev('value) as "tmaxstd")
      val tmax = tmaxData.select('tmax).as[Double].first()
      val tmaxstd = tmaxData.select('tmaxstd).as[Double].first()
      val tminData = tmins.agg(avg('value) as "tmin", stddev('value) as "tminstd")
      val tmin = tminData.select('tmin).as[Double].first()
      val tminstd = tminData.select('tminstd).as[Double].first()
      val precip = precips.agg(avg('value) as "precip").select('precip).as[Double].first()
      val tmaxSeasonalVar = calcSeasonalVar(tmaxs)
      val tminSeasonalVar = calcSeasonalVar(tmins)
      Some(ClusterData(cluster, lat, lon, latstd, lonstd, tmax, tmin, tmaxstd, tminstd,
        precip, tmaxSeasonalVar, tminSeasonalVar))
    }
    filteredData.unpersist()
    tmaxs.unpersist()
    tmins.unpersist()
    precips.unpersist()
    cd
  }

  val clusterData = (0 until 2000).par.flatMap(i => calcClusterData(joinedData, i)).seq
  val clusterDS = spark.createDataset(clusterData)
  clusterDS.show()
    
  spark.stop()
    }
}