from pyspark.sql import SparkSession

# Entry point to the API
spark_session = SparkSession.builder.master("local").appName("API test").getOrCreate()
df = spark_session.read.\
    option("inferSchema", "true").\
    option("header", "true").\
    csv("C:\\Users\\prshn\\Downloads\\file.csv")
#df.show()
#df.printSchema()
#df.groupBy("ResourceName").count().show()

store = spark_session.sparkContext.textFile("C:\\Users\\prshn\\Desktop\\store.csv")
store_sales = spark_session.sparkContext.textFile("C:\\Users\\prshn\\Desktop\\store_sales.csv")
store.collect()
store_sales.collect()
