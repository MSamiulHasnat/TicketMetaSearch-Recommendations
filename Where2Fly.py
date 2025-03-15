%pyspark

from pyspark.sql.functions import udf, col, min as spark_min, avg as spark_avg
from pyspark.sql.types import IntegerType
import re

# UDF to convert travelDuration from formats like 'PT2H30M' to minutes
def convert_duration_to_minutes(duration_str):
    hours_minutes = re.findall(r'\d+', duration_str)
    if hours_minutes:
        hours = int(hours_minutes[0])
        minutes = int(hours_minutes[1]) if len(hours_minutes) > 1 else 0
        return hours * 60 + minutes
    return 0

convert_duration_udf = udf(convert_duration_to_minutes, IntegerType())

# List of starting airports to process
departures = ["JFK", "OAK", "LGA", "BOS", "EWR", "DEN", "IAD", "CLT", "MIA", "DFW", "SFO", "ORD", "DTW", "LAX", "ATL", "PHL"]

def process_flights():
    # Load the itineraries data from Hive
    itineraries_df = spark.sql("SELECT startingAirport, destinationAirport, totalFare, travelDuration FROM itineraries_b")
    
    # Filter to only include the specified departure airports
    itineraries_df = itineraries_df.filter(itineraries_df.startingAirport.isin(departures))
    
    # Convert travelDuration to minutes
    itineraries_df = itineraries_df.withColumn("travelDurationInMinutes", convert_duration_udf(col("travelDuration")))
    
    # Group by starting and destination airports and calculate:
    # - lowestFare: minimum totalFare
    # - averageFare: average totalFare
    # - avgTravelDurationInMinutes: average travel duration (in minutes)
    aggregated_df = itineraries_df.groupBy("startingAirport", "destinationAirport").agg(
        spark_min("totalFare").alias("lowestFare"),
        spark_avg("totalFare").alias("averageFare"),
        spark_avg("travelDurationInMinutes").alias("avgTravelDurationInMinutes")
    )
    
    # Convert average travel duration from minutes to hours
    aggregated_df = aggregated_df.withColumn("travelTimeInHours", col("avgTravelDurationInMinutes") / 60)
    
    # Select the required columns for the output
    final_df = aggregated_df.select("startingAirport", "destinationAirport", "lowestFare", "averageFare", "travelTimeInHours")
    
    # Show a sample of the output 
    z.show(final_df)
    
    # Convert each row into an SQL INSERT statement.
    # For example, assuming the target table is 'flights_table':
    # def row_to_sql(row):
    #     return "INSERT INTO flights_table (startingAirport, destinationAirport, lowestFare, averageFare, travelTimeInHours) VALUES ('{0}', '{1}', {2}, {3}, {4});".format(
    #         row.startingAirport, row.destinationAirport, row.lowestFare, row.averageFare, row.travelTimeInHours
    #     )
    
    # sql_rdd = final_df.rdd.map(row_to_sql)
    
    # Save the DataFrame as a JSON file in HDFS
    final_df.coalesce(1).write.json("/output/whereToFly/WhereToFly_SamiulTest.json")

# Run the function to process the data and save the output
process_flights()

# Where_to_fly