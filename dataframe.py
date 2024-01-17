from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("DistribuiraniSistemi").getOrCreate()

path = "kupovina.csv"

df = spark.read.option("header", "false").csv(path)

df = (
    df.withColumnRenamed("_c0", "customerId")
    .withColumnRenamed("_c1", "productId")
    .withColumnRenamed("_c2", "price")
)

df = df.withColumn("price", df["price"].cast("float"))

total_amount_by_customer = df.groupBy("customerId").agg(
    sum("price").alias("total_amount")
)

total_amount_by_customer.show()

total_rows = total_amount_by_customer.count()
total_amount_by_customer.show(total_rows)
