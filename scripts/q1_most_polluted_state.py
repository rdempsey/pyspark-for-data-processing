"""
Read the CSV file and get a distinct count of records for each value in the
state_name field, show the results on the console and save them to a single csv
file.

Expected output:

+--------------+--------------------+-----+
|    state_name|      parameter_name|count|
+--------------+--------------------+-----+
|       Montana|     Nickel PM2.5 LC|21339|
|         Texas|    Arsenic PM2.5 LC|24611|
|  Rhode Island|  12-Dichloropropane| 6863|
|          Ohio|             Benzene| 6178|
|South Carolina|   Trichloroethylene| 1488|
|       Georgia|  Chromium (TSP) STP| 3608|
|    Washington|        13-Butadiene| 2266|
|     Louisiana|        Formaldehyde| 1875|
|    New Mexico|Carbon tetrachloride|  143|
|          Utah|Carbon tetrachloride|  968|
|          Ohio|cis-13-Dichloropr...|  712|
|    California|        Acetaldehyde|28919|
|       Indiana|1122-Tetrachloroe...| 9884|
|      Kentucky|    Mercury PM10 STP| 1562|
|      Oklahoma| Manganese (TSP) STP| 3297|
|  Rhode Island|  Manganese PM2.5 LC| 1565|
|        Kansas| Ethylene dichloride|  106|
|    California|        13-Butadiene|30699|
|  Rhode Island|        Formaldehyde| 4683|
|South Carolina|   Chromium PM10 STP| 1691|
+--------------+--------------------+-----+
only showing top 20 rows
"""

# Import what we need from PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

# Create a spark session
spark = SparkSession.builder.appName("Q1 Most Polluted State").getOrCreate()

df = spark.read.csv("/tmp/data/epa_hap_daily_summary.csv",
                    header=True,
                    mode="DROPMALFORMED")
                    
# Get the count of records grouped by the state_name and parameter name
df_new = df.groupby("state_name", "parameter_name").count()

# Show the results
df_new.show()

# Write the dataframe to csv files
df_new.write.option("header", "true") \
            .format('csv') \
            .save("/tmp/data/q1_most_polluted_state")
