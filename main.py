from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Make sure Spark is configured to use Hive
spark = SparkSession.builder \
    .appName("Load from Hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Load data from the Hive table "itineraries" into a DataFrame
df = spark.sql("SELECT * FROM itineraries")

# Show the first few records
df.show()

