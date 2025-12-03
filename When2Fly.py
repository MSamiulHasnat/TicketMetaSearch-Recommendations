%pyspark

from pyspark.sql.functions import dayofweek, col, date_format
from pyspark.sql.types import StringType
import re

# Define a function for the "When to Fly" feature with parameter names matching previous code
def when_to_fly_with_time_as_sql(startingAirport, destinationAirport, travelTime):
    # Load the itineraries data from Hive
    itineraries_df = spark.sql("SELECT startingAirport, destinationAirport, totalFare, flightDate, travelDuration FROM itineraries_b")
    
    # Filter the data based on the departure and destination airports
    filtered_df = itineraries_df.filter((itineraries_df.startingAirport == startingAirport) & 
                                        (itineraries_df.destinationAirport == destinationAirport))
    
    # Convert travelDuration (which is in format PTxHxM) to minutes
    from pyspark.sql.functions import udf
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
    result_df = filtered_df.groupBy("startingAirport", "destinationAirport", "flightDate").agg(
        {"totalFare": "min"}
    )
    
    # Rename the columns for clarity
    result_df = result_df.withColumnRenamed("min(totalFare)", "lowestFare")
    
    # Convert the flightDate to the day of the week (Monday, Tuesday, etc.)
    result_df = result_df.withColumn("dayOfWeek", date_format(col("flightDate"), "EEEE"))
    
    # Show the result
    result_df.show()
    
    final_df.coalesce(1).write.json("/output/whereToFly/WhenToFly_SamiulTest.json")

    
    # # Convert the dataframe rows to SQL INSERT statements
    # def row_to_sql(row):
    #     return f"INSERT INTO flights_table (startingAirport, destinationAirport, flightDate, dayOfWeek, lowestFare) " \
    #           f"VALUES ('{row.startingAirport}', '{row.destinationAirport}', '{row.flightDate}', '{row.dayOfWeek}', {row.lowestFare});"
    
    # sql_rdd = result_df.rdd.map(row_to_sql)
    
    # # Save the SQL statements as a text file with a .sql extension in HDFS
    # # sql_rdd.coalesce(1).saveAsTextFile("/output/whereToFly/WhereToFly_As_SQL.sql")

# Example usage:
when_to_fly_with_time_as_sql('ATL', 'BOS', 240)  # For example, departing from ATL to BOS, travel time of 240 minutes

# Implemented When to fly