"""
Read the CSV file and get a distinct count of records for each value in the
state_name field, show the results on the console and save them to a single csv
file.

Output: q1_most_polluted_state.csv file in the data folder
"""

# Import what we need from PySpark
from pyspark.sql import SparkSession
import pandas as pd

# Create a spark session
spark = SparkSession.builder.appName("Q1 Most Polluted State").getOrCreate()

df = spark.read.csv("/tmp/data/epa_hap_daily_summary.csv",
                    header=True,
                    mode="DROPMALFORMED")
                    
# Spark inported all datatypes as strings
# Convert the arithmetic_mean to a float so we can sum it
df = df.withColumn("arithmetic_mean", df["arithmetic_mean"].cast("float"))
                    
# Group by state_name
# Pivot on parameter_name
df_new = (df.groupby("state_name")
            .pivot("parameter_name")
            .sum("arithmetic_mean"))
    
# View the schema of the dataframe
# df_new.printSchema()

# Write the Spark dataframe to csv files
# df_new.write.option("header", "true") \
#       .format('csv') \
#       .save("/tmp/data/q1_most_polluted_state")
    
# Convert the dataframe to a Pandas dataframe
# The new DF will have 54 rows and 41 columns
# Since this is small it's okay
pandas_df = df_new.toPandas()

# Set the state_name column as the index
pandas_df.set_index('state_name', inplace=True)

# Get the max value of each column
max_vals_df = pandas_df.max().to_frame()
max_vals_df = max_vals_df.rename(columns={0: 'sum_measurements'})

# Get the state for the max value of each column
max_idx_df = pandas_df.idxmax().to_frame()
max_idx_df = max_idx_df.rename(columns={0: 'state'})

# Combine the dataframes
max_df = max_vals_df.join(max_idx_df, how='inner')

# Save the combined dataframe to a csv file
max_df.to_csv("/tmp/data/q1_most_polluted_state.csv")
