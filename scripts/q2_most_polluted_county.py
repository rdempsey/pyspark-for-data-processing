"""
Read the CSV file and get a distinct count of records for each value in the
county_name field, show the results on the console and save them to a single
csv file.

Expected output:

+------------+--------------+--------------------+-----+
| county_name|    state_name|      parameter_name|count|
+------------+--------------+--------------------+-----+
|  Miami-Dade|       Florida|   Chromium PM2.5 LC| 3101|
|       Essex|      New York| Ethylene dichloride| 1408|
|  Washington|         Maine|    Arsenic PM2.5 LC| 2366|
|        Lane|        Oregon|     Nickel PM10 STP|  360|
|  Chittenden|       Vermont|cis-13-Dichloropr...| 2203|
|     Hidalgo|         Texas|Carbon tetrachloride| 1884|
|       Butte|    California|   Chromium PM2.5 LC|  917|
|  Monongalia| West Virginia|        13-Butadiene|  325|
|     Suffolk|      New York|   Trichloroethylene|  356|
|  New Castle|      Delaware| Manganese (TSP) STP|  883|
|      Winona|     Minnesota|        Acetaldehyde|   59|
|     Henrico|      Virginia|        Acetaldehyde| 1410|
|      Carson|         Texas|  Ethylene dibromide|  189|
|     Baldwin|       Alabama|Carbon tetrachloride|   86|
|Chesterfield|South Carolina|      Vinyl chloride| 1369|
|       Tulsa|      Oklahoma|        Acetaldehyde| 1721|
|     Sanders|       Montana|    Arsenic PM2.5 LC| 1829|
|Philadelphia|  Pennsylvania|    Cadmium PM2.5 LC| 2807|
|     Windham|       Vermont|cis-13-Dichloropr...|  482|
|   San Mateo|    California|   Trichloroethylene|  790|
+------------+--------------+--------------------+-----+
only showing top 20 rows
"""

# Import what we need from PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

# Create a spark session
spark = SparkSession.builder.appName("Q2 Most Polluted County").getOrCreate()

df = spark.read.csv("/tmp/data/epa_hap_daily_summary.csv",
                    header=True,
                    mode="DROPMALFORMED")
                    
# Get the count of records grouped by the county_name and parameter name
df_new = df.groupby("county_name", "state_name", "parameter_name").count()

# Show the results
df_new.show()

# Write the dataframe to csv files
df_new.write.option("header", "true") \
            .format('csv') \
            .save("/tmp/data/q2_most_polluted_county")
