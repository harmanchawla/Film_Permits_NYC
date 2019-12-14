import sys
import time
import json
from pyspark import SparkSession
 
from pyspark.sql.types import StringType, ArrayType, IntegerType
from pyspark.sql.functions import lit, col, udf


flatten = lambda l: [item for sublist in l for item in sublist]
getZipCodesAsList =  udf(flatten, ArrayType(IntegerType()))



if __name__ == "__main__":

	spark = SparkSession.builder.getOrCreate()
	sc = spark.sparkContext

	# import the main dataset