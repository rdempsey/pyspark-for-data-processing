"""
Get the county and state with the largest measured quantity of each potential
pollutant.

Output folder: q2_most_polluted_county
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
 |-- state_name: string (nullable = true)
 |-- county_name: string (nullable = true)
 |-- arithmetic_mean: double (nullable = true)
"""
grouped = df.groupBy('parameter_name', 'state_name', 'county_name').agg(F.sum('arithmetic_mean').alias('arithmetic_mean'))

"""
Take the grouped data and get the max arithmetic_mean for each parameter_name

Schema:

root
 |-- parameter_name: string (nullable = true)
 |-- arithmetic_mean: double (nullable = true)
"""
grouped_param = grouped.groupBy('parameter_name').agg(F.max('arithmetic_mean').alias('arithmetic_mean'))

"""
Join the grouped_param dataframe back to the grouped dataframe using the
arithmetic_mean and parameter_name columns. This produces our final result.

Schema:

root
 |-- arithmetic_mean: double (nullable = true)
 |-- parameter_name: string (nullable = true)
 |-- state_name: string (nullable = true)
 |-- county_name: string (nullable = true)
"""
joined = grouped_param.join(grouped, ["arithmetic_mean","parameter_name"])
# Uncomment to print the schema
# joined.printSchema()

# Write the Spark dataframe to csv files
joined.write.option("header", "true").csv("/tmp/data/q2_most_polluted_county")
