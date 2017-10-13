"""
Copy the data from the test file into a new one.
"""

# Import what we need from PySpark
from pyspark import SparkContext, SparkConf

# Create a basic configuration
conf = SparkConf().setAppName("myTestCopyApp")

# Create a SparkContext using the configuration
sc = SparkContext(conf=conf)

# Read the source text file
words = sc.textFile("/tmp/data/copy_test_data.txt")

# Save the output as one or more text files
words.saveAsTextFile("/tmp/data/test_copy_result_single")

# Uncomment to create a single output file
# words.coalesce(1).saveAsTextFile("/tmp/data/test_copy_result_multiple")