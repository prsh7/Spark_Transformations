from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Q9").getOrCreate()

# csv contains data for 2 schools, and their 2 classes (10th and 11th), and their 2 students.
# there are 3 subjects total, so considering total marks obtained to find topper of each class from each school
store_sales_df = spark.read.option("header", "true").csv("C:\Users\prshn\Desktop\store_sales.csv")
store_sales_df.show()
store_sales_df.createOrReplaceTempView("store_sales")

store_df = spark.read.option("header", "true").csv("C:\Users\prshn\Desktop\store.csv")
store_df.show()
store_df.createOrReplaceTempView("store")

#store_sales_df.join(store_df, store_sales_df.SS_STORE_SK == store_df.S_STORE_SK, how="inner").show()
spark.sql("SELECT ss.SS_SOLD_DATE_SK, ss.SS_SOLD_TIME_SK, ss.SS_ITEM_SK, s.S_STORE_SK, s.S_STORE_ID, s.S_STORE_NAME  "
          "FROM store_sales ss "
          "JOIN store s "
          "ON ss.SS_STORE_SK = s.S_STORE_SK").show()