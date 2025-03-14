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
# Implementation of "Where to Fly"
%pyspark

# Define a function for the "Where to Fly" feature with budget and travel time in the output

def where_to_fly_with_budget_and_time(departure_location, travel_time, budget):
    # Load the itineraries data from Hive
    itineraries_df = spark.sql("SELECT startingAirport, destinationAirport, totalFare, travelDuration FROM itineraries_b")
    
    # Limit to the first 2000 rows
    itineraries_df = itineraries_df.limit(2000)
    
    # Filter the data based on the departure location
    filtered_df = itineraries_df.filter(itineraries_df.startingAirport == departure_location)
    
    # Convert travelDuration (which is in format PTxHxM) to minutes to make comparisons easier
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType
    import re

    # UDF to convert travel duration from PT2H30M to minutes
    def convert_duration_to_minutes(duration_str):
        hours_minutes = re.findall(r'\d+', duration_str)
        if hours_minutes:
            hours = int(hours_minutes[0])
            minutes = int(hours_minutes[1]) if len(hours_minutes) > 1 else 0
            return hours * 60 + minutes
        return 0
    
    # Register the UDF
    convert_duration_udf = udf(convert_duration_to_minutes, IntegerType())
    
    # Apply the UDF to convert travelDuration to minutes
    filtered_df = filtered_df.withColumn("travelDurationInMinutes", convert_duration_udf(filtered_df.travelDuration))
    
    # Filter the results based on the travel time input (in minutes)
    filtered_df = filtered_df.filter(filtered_df.travelDurationInMinutes <= travel_time)
    
    # Filter the results based on the budget
    filtered_df = filtered_df.filter(filtered_df.totalFare <= budget)
    
    # Calculate the minimum fare and travel duration for each destination
    result_df = filtered_df.groupBy("destinationAirport").agg(
        {"totalFare": "min", "travelDurationInMinutes": "min"}
    )
    
    # Rename the aggregated columns for clarity
    result_df = result_df.withColumnRenamed("min(totalFare)", "lowestFare") \
                         .withColumnRenamed("min(travelDurationInMinutes)", "travelTimeInMinutes")
    
    # Limit the result to the first 100 rows
    result_df = result_df.limit(100) # Show in non-graphical table
    z.show(result_df) # Show in graphical table
    
    # Write the result to a JSON file
    # result_df.coalesce(1).write.option("header", "true").json("/output/whereToFly/alt.json")

# Example usage:
where_to_fly_with_budget_and_time('ATL', 240, 300)  # For example, departing from ATL, max travel time of 240 minutes, and a max budget of 300






# 3rd Block of Zeppelin
# Implementation of "When to Fly"
# Define the starting and destination airports
starting_airport = "JFK"  # Starting airport
destination_airport = "ORD"  # Destination airport

# Load data into a DataFrame from Hive
df = spark.sql("SELECT * FROM itineraries")

# Convert flightDate and searchDate to DateType
df = df.withColumn("flightDate", F.to_date("flightDate", "MM/dd/yyyy"))
df = df.withColumn("searchDate", F.to_date("searchDate", "MM/dd/yyyy"))

# Extract hours and minutes from travelDuration to calculate travel hours
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

# Filter the data based on the starting and destination airports
df_filtered = df.filter(
    (df['startingAirport'] == starting_airport) &
    (df['destinationAirport'] == destination_airport)
)

# Group by flightDate to calculate the lowest fare and minimum travel duration for each flight date
df_grouped = df_filtered.groupBy('flightDate').agg(
    F.min('totalFare').alias('lowestFare'),
    F.min('travelDurationHours').alias('minTravelDuration')
)

# Remove duplicates for lowestFare (ensure distinct lowest fare only)
df_distinct = df_grouped.dropDuplicates(['lowestFare'])

# Sort by lowestFare (ascending order)
df_sorted = df_distinct.orderBy('lowestFare')

# Show the results: Flight date, Lowest fare, and Minimum travel duration
df_sorted.select('flightDate', 'lowestFare', 'minTravelDuration').show(10)











#4th section of zepplin
%pyspark

from pyspark.sql.functions import col, datediff, to_date, month, dayofweek
from pyspark.sql import functions as F

# Load the itineraries data (assumes data is in 'itineraries_b' table in Hive)
itineraries_df = spark.sql("SELECT flightDate, totalFare, startingAirport, destinationAirport FROM itineraries_b")

# Convert flightDate to date type if needed
itineraries_df = itineraries_df.withColumn("flightDate", to_date(col("flightDate"), "yyyy-MM-dd"))

# Assuming bookingDate is available, we'll add a bookingDate column for lead time calculation (you'll need to adjust this based on actual column availability)
# For now, let's assume you have a column `bookingDate` that is available and that it is in the same format as flightDate.

# For the purpose of this example, let's generate a hypothetical `bookingDate` column (you will replace this with actual data from your source)
itineraries_df = itineraries_df.withColumn("bookingDate", to_date(F.lit('2022-04-01'), "yyyy-MM-dd"))  # Placeholder bookingDate

# Calculate booking lead time (days between booking and flight date)
itineraries_df = itineraries_df.withColumn("bookingLeadTime", datediff(col("flightDate"), col("bookingDate")))

# 1. Analyze how prices vary with booking lead time
booking_lead_time_df = itineraries_df.groupBy("bookingLeadTime").agg(F.avg("totalFare").alias("avgFare"))

# Show the result of average fare by booking lead time
# booking_lead_time_df.show()
z.show(booking_lead_time_df)

# 2. Analyze fares by month
itineraries_df = itineraries_df.withColumn("month", month(col("flightDate")))

monthly_fares_df = itineraries_df.groupBy("month").agg(F.avg("totalFare").alias("avgFare"))

# Show the result of average fare by month
# monthly_fares_df.show()
z.show(monthly_fares_df)

# 3. Analyze fares by day of the week
itineraries_df = itineraries_df.withColumn("dayOfWeek", dayofweek(col("flightDate")))

day_of_week_fares_df = itineraries_df.groupBy("dayOfWeek").agg(F.avg("totalFare").alias("avgFare"))

# Show the result of average fare by day of the week
# day_of_week_fares_df.show()
#Check in
z.show(day_of_week_fares_df)


# Finding the Best Time to Buy Tickets


