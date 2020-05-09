from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sql("SELECT * FROM hive_lab.employee").show()
