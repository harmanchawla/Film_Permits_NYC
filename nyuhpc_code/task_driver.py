'''
	Main file for the project. 
	1. Performs basic transformations and operation. 
	2. Calls other modules that need to be run on the class. 


	Files Produced:
	permits-by-borough.csv, permits-by-type.csv, borough-deep-view.csv, popularity.csv
	duration-by-type.csv, duration-by-borough.csv
	

'''

import sys
import time
import json
import datetime 
import itertools

from seasonality import seasonality

from dateutil.parser import parse

from pyspark import SparkSession
from pyspark.sql.types import StringType, ArrayType, IntegerType, DoubleType
from pyspark.sql.functions import lit, col, udf, mean, explode

''' UDF to flattern nested zipcodes'''
# flatten = lambda l: [item for sublist in l for item in sublist]
# getZipCodesAsList =  udf(flatten, ArrayType(IntegerType()))

''' UDF to calculate the shooting time'''
shootingTime = udf(shootDuration, DoubleType())
def shootDuration(startTime, endTime):
	''' 
		Returns the hours passed between startTime and endTime
	'''
	date1 = parse(str(startTime), fuzzy=True) # gives a datetime object
	date2 = parse(str(endTime), fuzzy=True)

	durationObj = date2-date1 #returns a timedelta object

	durationSeconds = durationObj.total_seconds()
	durationHours = durationSeconds/3600
	if durationHours < 0:
		return 0.0
	return  durationHours


def _zipCodes(df):
	''' Get the zip codes as a separate rdd '''
	# flatten = lambda l: [item for sublist in l for item in sublist]
	df_rdd = df.rdd.map(lambda x: x["ZipCode(s)"].split(",")).map(lambda x: [l.strip() for l in x])
	nested_zip = df_rdd.collect()
	zipCodeList = list(itertools.chain.from_iterable(nested_zip))
	return list(set(zipCodeList))
	

def _get_year(dateString):
	'''  convert the date into a datetime object and return the year as string '''
	dateObj = parse(dateString, fuzzy=True)
	try:
		year = dateObj.year
		if year > 2000 and year < 2030:
			return year
		else: 
			return 0
	except:
		return 0

''' UDF for extracting the year from the date '''
getYear = udf(_get_year, IntegerType()) 


def _string_to_list(element):
	''' Takes a string of zip codes and converts them into an array of zip codes'''
	element = element.split(",")
	element = [s.strip() for s in element]
	return element

_change_zip = udf(_string_to_list, ArrayType(StringType()))



def popularity(df):
	''' Number of shooting permits issued in a zip code over the years '''
	# zipCodes = _zipCodes(df.select("ZipCode(s)")) # list of zip codes 
	df = df.withColumn("startYear", getYear("startDateTime")).withColumn("Zipcodes", _change_zip("ZipCode(s)"))
	temp_df = df.select("startYear", explode("Zipcodes").alias("Zipcode"))
	zip_popularity = temp_df.groupBy("Zipcode").count().orderBy("count", ascending=False)
	zip_popularity.write.csv("overall-popularity.csv")
	
	years = list(range(2019, 2020))
	for year in years:
		df_year = temp_df.filter(col("startYear")==year)
		df_year = df_year.groupBy("Zipcode").count()
		
	return temp_df


def get_analyis(df):
	''' 
		@Args: Dataframe
		Accepts a Dataframe.
		@Returns a dataframe with added columns and transformations required for 
		future analysis
	'''

	df = df.withColumn("Duration (in Hours)", shootingTime("StartDateTime", "EndDateTime")) # add a duration column

	# distribution of film permits by borough
	temp_df = df.groupBy("Borough").count().orderBy("count", ascending=False)
	print("Distribution of film permits by borough")
	temp_df.show() # see output 1 in output.md
	temp_df.write.csv("permits-by-borough.csv", header=False)

	# distibution of film permits by type
	temp_df = df.groupBy("EventType").count().orderBy("count", ascending=False)
	print("Distibution of film permits by type")
	temp_df.show() # see output 2 in output.md
	temp_df.write.csv("permits-by-type.csv")

	# distribution by type and borough
	temp_df = df.groupBy(["Borough", "EventType"]).count().orderBy(["Borough", "count"], ascending=False)
	temp_df.show() # See output 3 in output.md
	temp_df.write.csv("borough-deep-view.csv")

	# popularity of a neighborhood
	temp_df = df.select("ZipCode(s)")
	temp_df = popularity(temp_df)
	print("Popularity of a neighbourhood")
	temp_df.show()
	temp_df.write.csv("popularity.csv")

	# average duration of permits by type
	temp_df = df.select("EventType", "Duration (in Hours)").groupBy("EventType").agg(mean("Duration (in Hours)").alias("Avg Duration"))
	temp_df = temp_df.orderBy("Avg Duration", ascending=False)
	print("Average Duration by Type")
	temp_df.show() # See output 4 in output.md
	temp_df.write.csv("duration-by-type.csv")
	
	# average duration of permits by borough
	temp_df = df.select("Borough", "Duration (in Hours)").groupBy("Borough").agg(mean("Duration (in Hours)").alias("Avg Duration")).orderBy("Avg Duration", ascending=False)
	print("Average Duration by Borough")
	temp_df.show() # See output 5 in output.md
	temp_df.write.csv("duration-by-borough.csv")


	return df


if __name__ == "__main__":

	spark = SparkSession.builder.getOrCreate()
	sc = spark.sparkContext

	start = time.time()

	# import the main dataset
	film_permits = spark.read.csv("/user/hsc367/film_permits.csv", header=True, inferSchema=True)
	# get basic analysis for the data 
	get_analyis(film_permits)
	# extract the nested zip codes from the column for the web crawler
	
	seasonality(spark, df)

	end = time.time()
	print("Time taken: {}".format(end-start))
