package main.scala

object ConsumptionSqlDF {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    
    val spark = SparkSession.builder().master("local[*]").appName("Nike Sales SQL DF").getOrCreate()
  
    spark.sparkContext.setLogLevel("WARN")
  
    import spark.implicits._
    import org.apache.spark.sql.types._
    val salesDF = spark.sqlContext.read.format("com.databricks.spark.csv").
                        option("header", "true").load("data/sales.csv").
                        withColumn("store", $"StoreID".cast(IntegerType)).
                        withColumn("dateId", $"DateID".cast(IntegerType)).
                        withColumn("productId", $"ProductID".cast(LongType)).
                        withColumn("salesUnits", $"SalesUnits".cast(IntegerType)).
                        withColumn("netSales", $"NetSales".cast(DoubleType)).
                        drop($"saleId").
                        drop($"storeId")
    salesDF.registerTempTable("SalesTable")
    //salesDF.printSchema()
    //salesDF.show()
    
    val calendarDF = spark.sqlContext.read.format("com.databricks.spark.csv").
                          option("header", "true").load("data/calendar.csv").
                          withColumn("datekey", $"datekey".cast(IntegerType)).
                          withColumn("datecalendaryear", $"datecalendaryear".cast(IntegerType)).
                          withColumn("weeknumberofseason", $"weeknumberofseason".cast(IntegerType)).
                          drop($"datecalendarday")
    calendarDF.registerTempTable("CalendarTable")
    //calendarDF.printSchema()
    //calendarDF.show()
    
    val joinedSalesGroup =  spark.sqlContext.sql("select CONCAT('Y', datecalendaryear%1000, '_W', weeknumberofseason) as ID, store as storeId, productId, salesUnits, netSales from SalesTable join CalendarTable on dateId = datekey")
    joinedSalesGroup.registerTempTable("SalesGroupTable")
    //joinedSalesGroup.printSchema()
    //joinedSalesGroup.show()
    
    val aggConsumptionGroup = spark.sqlContext.sql("select ID, storeId, productId, sum(salesUnits) as SalesUnits, round(sum(netSales), 2) as NetSales from SalesGroupTable group by ID, storeId, productId")
    aggConsumptionGroup.registerTempTable("AggTable")
    //aggConsumptionGroup.printSchema()
    //aggConsumptionGroup.show()
    
    val storeDF = spark.sqlContext.read.format("com.databricks.spark.csv").
                          option("header", "true").load("data/store.csv").
                          withColumn("storeid", $"storeid".cast(IntegerType))
    storeDF.registerTempTable("StoreTable")
    //storeDF.printSchema()
    //storeDF.show()

    val productDF = spark.sqlContext.read.format("com.databricks.spark.csv").
                          option("header", "true").load("data/product.csv").
                          withColumn("productid", $"productid".cast(LongType))
    productDF.registerTempTable("ProductTable")
    //productDF.printSchema()
    //productDF.show()

    val consumptionDF = spark.sqlContext.sql("select CONCAT(ID, '_', country, '_', division, '_', gender, '_', category) as ID, salesUnits, netSales from AggTable join StoreTable on AggTable.storeid = StoreTable.storeid join ProductTable on AggTable.productId = ProductTable.productId")
    consumptionDF.printSchema()
    consumptionDF.show()
  }
}