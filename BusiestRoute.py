%pyspark

from pyspark.sql import functions as F

# Load the itineraries data
itineraries_df = spark.sql("SELECT startingAirport, destinationAirport FROM itineraries_b")

# Group by startingAirport and destinationAirport, and count the number of flights for each route
busiest_routes_df = itineraries_df.groupBy("startingAirport", "destinationAirport") \
                                  .agg(F.count("*").alias("numFlights"))

# Sort the routes by the number of flights in descending order and get the top 10 busiest routes
top_10_busiest_routes_df = busiest_routes_df.orderBy(F.col("numFlights").desc()).limit(20)

# Show the top 10 busiest routes
z.show(top_10_busiest_routes_df)
