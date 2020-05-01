from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
c = SparkConf()

spark = SparkSession\
    .builder.appName('abc')\
    .enableHiveSupport()\
    .getOrCreate()
#spark = SparkSession\
#    .builder\
#    .appName('abc')\
#    .config(conf=c)\
#    .enableHiveSupport()\
#    .getOrCreate()
#spark = SparkSession\
#    .builder\
#    .master("yarn")\
#    .appName('abc')\
#    .config(conf=c)\
#    .enableHiveSupport()\
#    .getOrCreate()
#spark = SparkSession\
#   .builder\
#   .appName("SparkApp")\
#   .master("yarn")\
#   .config("spark.submit.deployMode","cluster")\
#   .enableHiveSupport()\
#   .getOrCreate()

print(spark.sql('SELECT * FROM hive_lab.employee').show())

