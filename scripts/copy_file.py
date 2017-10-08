"""
When run in Spark, this will copy the test file and create a second one
containing the same contents as the first.
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