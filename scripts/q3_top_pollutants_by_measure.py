"""
Get the top pollutants by measure.

Output folder: q3_top_pollutants_by_measure
"""

# Import what we need from PySpark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create a spark session
spark = SparkSession.builder.appName("Q2 Most Polluted County").getOrCreate()


# Create a dataframe from the source csv file
df = spark.read.csv("/tmp/data/epa_hap_daily_summary.csv",
                    header=True,
                    mode="DROPMALFORMED")
    
"""
Group the data by parameter_name and state, and sum the arithmetic_mean

Schema:

root
 |-- parameter_name: string (nullable = true)
 |-- units_of_measure: string (nullable = true)
 |-- arithmetic_mean: double (nullable = true)
"""
grouped = df.groupBy('parameter_name', 'units_of_measure').agg(F.sum('arithmetic_mean').alias('arithmetic_mean'))

# Write the Spark dataframe to csv files
grouped.write.option("header", "true").csv("/tmp/data/q3_top_pollutants_by_measure")
