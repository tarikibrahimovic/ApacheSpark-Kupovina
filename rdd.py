from pyspark import SparkConf, SparkContext
import os

# Set the PYSPARK_PYTHON environment variable
os.environ[
    "PYSPARK_PYTHON"
] = "D:\\DesktopApps\\ASKupovina\\askupovina\\Scripts\\python.exe"

conf = SparkConf().setAppName("DistribuiraniSistemi").setMaster("local[*]")
sc = SparkContext(conf=conf)
rdd = sc.textFile("kupovina.csv")


def parse_line(line):
    fields = line.split(",")
    customer_id = int(fields[0])
    amount = float(fields[2])
    return (customer_id, amount)


parsed_lines = rdd.map(parse_line)
total_amount_by_customer = parsed_lines.reduceByKey(lambda x, y: x + y)

results = total_amount_by_customer.collect()
for result in results:
    print(result)
