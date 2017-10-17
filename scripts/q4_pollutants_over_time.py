"""
Get the measure of each pollutant by state by month and year.

Output folder: q4_pollutants_over_time
"""

# Import what we need from PySpark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create a spark session
spark = SparkSession.builder.appName("Q4 Pollutants Over Time").getOrCreate()


# Create a dataframe from the source csv file
df = spark.read.csv("/tmp/data/epa_hap_daily_summary.csv",
                    header=True,
                    mode="DROPMALFORMED")
    
"""
Group the data by parameter_name, state and the month in date_local, 
then sum the arithmetic_mean

Schema:

root
 |-- parameter_name: string (nullable = true)
 |-- state_name: string (nullable = true)
 |-- month: integer (nullable = true)
 |-- year: integer (nullable = true)
 |-- arithmetic_mean: double (nullable = true)
"""
grouped = df.groupBy('parameter_name',
                     'state_name',
                     F.year('date_local').alias('year'),
                     F.month('date_local').alias('month')).agg(F.sum('arithmetic_mean').alias('arithmetic_mean'))
                     
grouped.printSchema()

# Write the Spark dataframe to csv files
grouped.write.option("header", "true").csv("/tmp/data/q4_pollutants_over_time")
