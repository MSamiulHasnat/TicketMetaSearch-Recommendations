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
%pyspark

from pyspark.sql.functions import dayofweek, col, date_format
from pyspark.sql.types import StringType

# Define a function for the "When to Fly" feature with parameter names matching previous code
def when_to_fly_with_time(startingAirport, destinationAirport, travelTime):
    # Load the itineraries data from Hive
    itineraries_df = spark.sql("SELECT startingAirport, destinationAirport, totalFare, flightDate, travelDuration FROM itineraries_b")
    
    # Filter the data based on the departure and destination airports
    filtered_df = itineraries_df.filter((itineraries_df.startingAirport == startingAirport) & 
                                        (itineraries_df.destinationAirport == destinationAirport))
    
    # Convert travelDuration (which is in format PTxHxM) to minutes
    from pyspark.sql.functions import udf
    import re
    from pyspark.sql.types import IntegerType
    
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
    filtered_df = filtered_df.filter(filtered_df.travelDurationInMinutes <= travelTime)
    
    # Group by flightDate and calculate the minimum fare for each date
    result_df = filtered_df.groupBy("flightDate").agg(
        {"totalFare": "min"}
    )
    
    # Rename the columns for clarity
    result_df = result_df.withColumnRenamed("min(totalFare)", "lowestFare")
    
    # Convert the flightDate to the day of the week (Monday, Tuesday, etc.)
    result_df = result_df.withColumn("dayOfWeek", date_format(col("flightDate"), "EEEE"))
    
    # Show the result: flight date, day of the week, and lowest fare
    result_df.show()

# Example usage:
when_to_fly_with_time('ATL', 'BOS', 240)  # For example, departing from ATL to BOS, travel time of 240 minutes

# Implemented When to fly











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


