from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

# Make sure Spark is configured to use Hive
spark = SparkSession.builder \
    .appName("Load from Hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Load data from the Hive table "itineraries" into a DataFrame
df = spark.sql("SELECT * FROM itineraries")

# Show the first few records
df.show()




# 2nd block in zeppeline

# Get user input for starting airport and travel time
starting_airport = "ATL"  # Example input
max_travel_duration = 3  # Maximum travel duration in hours

# Load the data into a DataFrame from Hive
df = spark.sql("SELECT * FROM itineraries")

# Extract hours and minutes from the travelDuration column
df = df.withColumn(
    "travelDurationHours",
    F.when(
        df["travelDuration"].contains("H"),
        F.regexp_extract(df["travelDuration"], r"(\d+)H", 1).cast("double")
    ).otherwise(0) +
    F.when(
        df["travelDuration"].contains("M"),
        F.regexp_extract(df["travelDuration"], r"(\d+)M", 1).cast("double") / 60
    ).otherwise(0)
)

# Filter the data based on the starting airport and travel duration
df_filtered = df.filter(
    (df['startingAirport'] == starting_airport) &
    (df['travelDurationHours'] <= max_travel_duration)
)

# Sort by totalFare to find the lowest cost destinations
df_sorted = df_filtered.orderBy('totalFare')

# Show the top 10 destinations with their fare and duration
df_sorted.select('destinationAirport', 'totalFare', 'travelDurationHours').show(10)
