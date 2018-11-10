package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class Sales(dateID: Int, storeID: Int, productID: Long, salesUnits: Int, netSales: Double)
case class Calendar(dateID: Int, dateYear: Int, weekNumber: Int)
case class Product(productID: Long, division: String, gender: String, category: String)
case class Store(storeID: Int, country: String)

object ConsumptionRDD {
  def main (args: Array[String]):Unit = {
  val conf = new SparkConf().setAppName("Nike Sales RDD").setMaster("local[*]")
  val sc = new SparkContext(conf)
  
  sc.setLogLevel("WARN")
  
  // Read the csv file and convert into an RDD of type Sales
  val salesRDD = sc.textFile("data/sales.csv").
                 filter(!_.contains("netSales")).map { 
                       line => val p = line.split(",").map(_.trim)
                       Sales(p(4).toInt, p(3).toInt, p(5).toLong, p(2).toInt, p(1).toDouble)
                 }//.cache()
  //salesRDD.take(5) foreach println
  
  // Convert the Sales RDD into a PairRDD by using a map function (2 ways to do this)
  //val salesGroup = salesRDD.map(d => d.dateID -> (d.storeID, d.productID, d.salesUnits, d.netSales))
  val salesGroup = salesRDD.map(d => (d.dateID, (d.storeID, d.productID, d.salesUnits, d.netSales)))
  //salesGroups.take(5) foreach println
  //val num = salesGroups.count()
  //println(s"Count = $num")
  
  // Read the csv file and convert into an RDD of type Calendar
  val calendarRDD = sc.textFile("data/calendar.csv").
                    filter(!_.contains("datekey")).map {
                          line => val p = line.split(",").map(_.trim)
                          Calendar(p(0).toInt, p(2).toInt, p(3).toInt)
                    }//.cache()
  //calendarRDD.take(5) foreach println
  
  // Convert the Calendar RDD into a PairRDD by using a map function
  val calendarGroup = calendarRDD.map(d => d.dateID -> (d.dateYear, d.weekNumber))
  //calendarGroup.take(5) foreach println
  
  // Join Sales and Calendar PairRDDs on dataID and then create a key witht he combination of YW, StoreID and ProdID
  //val joinedSalesGroup =  salesGroup.join(calendarGroup).mapValues({case ((a,b,c,d),(e,f)) => (e,f,a,b,c,d)})
  val joinedSalesGroup =  salesGroup.join(calendarGroup).map(
                                     { case (k,((a,b,c,d),(e,f))) => ("Y"+e%1000+"_W"+f,a,b) -> (c,d)})
  //joinedSalesGroup.take(5) foreach println
  
  //val weeklySalesGroup = joinedSalesGroup.map({case (a,(b,c,d,e,f,g)) => ("Y"+b%1000+"_W"+c,d,e) -> (f,g)})
  //val weeklySalesGroup = joinedSalesGroup.map({case (a,(b,c,d,e,f,g)) => (b.toString()+c.toString(),d,e) -> (f,g)})
  //weeklySalesGroup.take(20) foreach println
  
  //Find the aggregated sum of sales for the above(YW, Sal, Prod) key combination
  //Also make the store ID as the new key for the results
  //val aggWeeklySalesGroup =  weeklySalesGroup.reduceByKey({case((a1,b1),(a2,b2)) => (a1+a2, (b1+b2) - (b1+b2)%0.01)})
  //val aggWeeklySalesGroup =  weeklySalesGroup.
  //                           reduceByKey({case((a1,b1),(a2,b2)) => (a1+a2, Math.round((b1+b2)*100.0)/100.0)}).
  //                           map({case((a,b,c),(d,e)) => (b)->(a,c,d,e)})
  /*val aggWeeklySalesGroup =  joinedSalesGroup.aggregateByKey(0->0.0)({case((c,s),(a,b)) => (c+a,s+b)},
                             {case((c1,s1),(c2,s2)) => (c1+c2,Math.round((s1+s2)*100.0)/100.0)}).
                             map({case((a,b,c),(d,e)) => (b)->(a,c,d,e)})*/
  val aggWeeklySalesGroup =  joinedSalesGroup.reduceByKey(
                             {case((a1,b1),(a2,b2)) => (a1+a2, Math.round((b1+b2)*100.0)/100.0)}).
                             map({case((a,b,c),(d,e)) => (b)->(a,c,d,e)})
  
  val storeRDD = sc.textFile("data/store.csv").filter(!_.contains("storeid")).map { line =>
    val p = line.split(",").map(_.trim)
    Store(p(0).toInt, p(2))
  }.cache()
  //storeRDD.take(5) foreach println
  val storeRDDGroup = storeRDD.map(d => d.storeID -> d.country)
  
  // Join the aggregated results and store on store Id and then map them to product id as key
  val aggConsumpStoreGroup = aggWeeklySalesGroup.join(storeRDDGroup).
                             map({case(a,((b,c,d,e),f)) => c -> (b+"_"+f,d,e)})
  //aggConsumpStoreGroup.take(5) foreach println
  
  val productRDD = sc.textFile("data/product.csv").filter(!_.contains("productid")).map { line =>
    val p = line.split(",").map(_.trim)
    Product(p(0).toLong, p(1), p(2), p(3))}.
    map(d => d.productID -> (d.division, d.gender, d.category))
  //}.cache()
  //productRDD.take(5) foreach println
  //val productRDDGroup = productRDD.map(d => d.productID -> (d.division, d.gender, d.category))
  
  //val consumptionGroup = aggConsumpStoreGroup.join(productRDDGroup).
  val consumptionGroup = aggConsumpStoreGroup.join(productRDD).
                         map({case(a,((b,c,d),(e,f,g))) => b+"_"+e+"_"+f+"_"+g+", "+c+", "+d})
                         //map({case(a,((b,c,d),(e,f,g))) => b+"_"+e+"_"+f+"_"+g -> (c, d)})

  //consumptionGroup.take(10) foreach println
  //consumptionGroup.saveAsTextFile("/home/pavan/Data/output")
  consumptionGroup.repartition(1).saveAsTextFile("/home/pavan/Data/output")
  
  sc.stop()
 }
}