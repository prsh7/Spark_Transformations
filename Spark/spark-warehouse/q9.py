from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Q9").getOrCreate()

# csv contains data for 2 schools, and their 2 classes (10th and 11th), and their 2 students.
# there are 3 subjects total, so considering total marks obtained to find topper of each class from each school
raw_df = spark.read.option("header", "true").csv("C:\Users\prshn\Downloads\school.csv")
print("### Raw file dataframe ### ")
raw_df.show()
raw_df.createOrReplaceTempView("school")


temp1 = spark.sql("SELECT schoolid, classid, studentid, SUM(marks) AS total "
                  "FROM school GROUP BY schoolid, classid, studentid")
temp1.createOrReplaceTempView("temp1")

temp2 = spark.sql("SELECT schoolid, classid, studentid, total, rank() OVER(PARTITION BY schoolid, classid "
                                                                      "ORDER BY total DESC) as rank from temp1")
temp2.createOrReplaceTempView("temp2")

print("Topper student from each school and each class")
spark.sql("SELECT * FROM temp2 where rank=1 order by schoolid").show()
