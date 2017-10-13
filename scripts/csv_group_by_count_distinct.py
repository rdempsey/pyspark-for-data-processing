"""
Read the CSV file and get a distinct count of records for each value in the
parameter_name field, show the results on the console and save them to a 
single csv file.

Expected output:

+--------------------+------+
|      parameter_name| count|
+--------------------+------+
|             Benzene|469375|
|Chromium VI (TSP) LC|   948|
|  12-Dichloropropane|198818|
|Acrolein - Unveri...| 91268|
| Tetrachloroethylene|245750|
|       Lead PM10 STP| 57289|
|     Nickel PM10 STP| 46572|
|    Nickel (TSP) STP|119639|
|   Chromium PM10 STP| 45137|
|  Ethylene dibromide|195493|
|    Mercury PM10 STP| 12528|
|  Chromium (TSP) STP|119733|
|cis-13-Dichloropr...|182596|
|      Vinyl chloride|222726|
|   Trichloroethylene|237081|
|       Lead PM2.5 LC|600171|
|          Chloroform|245517|
|  Manganese PM10 STP| 47263|
|        Acetaldehyde|169218|
|  Beryllium PM2.5 LC|  1565|
+--------------------+------+
"""

# Import what we need from PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

# Create a spark session
spark = SparkSession.builder.appName("Group By Count Distinct").getOrCreate()

df = spark.read.csv("/tmp/data/epa_hap_daily_summary.csv",
                    header=True,
                    mode="DROPMALFORMED")
                    
# Get the distinct count of records grouped by parameter_name
df_new = df.groupby("parameter_name").count().distinct()

# Show the results
df_new.show()

# Write the dataframe to a single csv file
# This isn't performant on a huge data but is a good example for a single node
df_new.coalesce(1).write.format('csv').save("/tmp/data/parameter_names_and_counts")
