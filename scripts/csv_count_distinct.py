"""
Read the CSV file and perform an aggregate count of distinct values on the 
parameter_name column.

Expected output:

+-----+
|count|
+-----+
|   41|
+-----+
"""

# Import what we need from PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

# Create a spark session
spark = SparkSession.builder.appName("Count Distinct").getOrCreate()

df = spark.read.csv("/tmp/data/epa_hap_daily_summary.csv",
                    header=True,
                    mode="DROPMALFORMED")
                    
# Show the distinct count of the parameter_name field
df.agg(countDistinct(col("parameter_name")).alias("count")).show()
