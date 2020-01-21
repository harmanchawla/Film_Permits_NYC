'''
	Main file for the project. 
	1. Performs basic transformations and operation. 
	2. Calls other modules that need to be run on the class. 


	Files Produced:
	permits-by-borough.csv, permits-by-type.csv, borough-deep-view.csv, popularity.csv
	duration-by-type.csv, duration-by-borough.csv, overall-popularity.csv, opularity-year-2010...2019.csv,

'''
import os
import sys
import time
import json
import datetime
import itertools

from seasonality import get_seasonality

from dateutil.parser import parse

from pyspark.sql.types import StringType, ArrayType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import lit, col, udf, mean, explode
from pyspark.sql import SparkSession

HADOOP_EXE = '/usr/bin/hadoop'
HADOOP_LIBPATH = '/opt/cloudera/parcels/CDH/lib'
HADOOP_STREAMING = 'hadoop-mapreduce/hadoop-streaming.jar'

hfs = "{} fs".format(HADOOP_EXE)

''' UDF to calculate the shooting time'''
def shootDuration(startTime, endTime):
	''' 
		Returns the hours passed between startTime and endTime
	'''
	date1 = parse(str(startTime), fuzzy=True)  # gives a datetime object
	date2 = parse(str(endTime), fuzzy=True)

	durationObj = date2-date1  # returns a timedelta object

	durationSeconds = durationObj.total_seconds()
	durationHours = durationSeconds/3600
	if durationHours < 0:
		return 0.0
	return durationHours

shootingTime = udf(shootDuration, DoubleType())

def _zipCodes(df):
	''' Get the zip codes as a separate rdd '''
	# flatten = lambda l: [item for sublist in l for item in sublist]
	df_rdd = df.rdd.map(lambda x: x["ZipCode(s)"].split(
		",")).map(lambda x: [l.strip() for l in x])
	nested_zip = df_rdd.collect()
	zipCodeList = list(itertools.chain.from_iterable(nested_zip))
	return list(set(zipCodeList))


def _get_year(dateString):
	'''  convert the date into a datetime object and return the year as string '''
	try:
		dateObj = parse(dateString, fuzzy=True)
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
	df = df.withColumn("startYear", getYear("StartDateTime")).withColumn(
		"Zipcodes", _change_zip("ZipCode(s)"))
	temp_df = df.select("startYear", explode("Zipcodes").alias("Zipcode"))

	zip_popularity = temp_df.groupBy(
		"Zipcode").count().orderBy("count", ascending=False)
	zip_popularity.show(10)
	try:
		cmd = "{} -rm -r overall-popularity.csv".format(hfs)
		os.system(cmd)
	except:
		print("Failed to remove the file or the file doesn't exist.")
		pass
	try:
		print("Writing file: overall-popularity.csv")
		zip_popularity.write.csv("overall-popularity.csv")
	except:
		print("Failed to write the file.")
		pass

	years = list(range(2011, 2020))
	for year in years:
		df_year = temp_df.filter(col("startYear") == year)
		df_year = df_year.groupBy("Zipcode").count()
		try:
			cmd = "{} -rm -r popularity-year-{}.csv".format(hfs, year)
			os.system(cmd)
		except:
			print("Failed to remove or file not found")
			pass
		try:
			print("Writing file: popularity-year-{}.csv".format(year))
			df_year.write.csv("popularity-year-{}.csv".format(year))
		except:
			print("Failed to write file: popularity-year-{}.csv".format(year))
			pass

	return df


def get_analyis(df):
	''' 
		@Args: Dataframe
		Accepts a Dataframe.
		@Returns a dataframe with added columns and transformations required for 
		future analysis
	'''
	try:
		cmd = "{} -rm -r permits-by-borough.csv".format(hfs)
		os.system(cmd)
		cmd = "{} -rm -r duration-by-borough.csv".format(hfs)
		os.system(cmd)
		cmd = "{} -rm -r permits-by-type.csv".format(hfs)
		os.system(cmd)
		cmd = "{} -rm -r borough-deep-view.csv".format(hfs)
		os.system(cmd)
		cmd = "{} -rm -r duration-by-type.csv".format(hfs)
		os.system(cmd)

	except:
		print("File not removed.")
		pass

	#
	df = df.withColumn("Duration (in Hours)", shootingTime(
		"StartDateTime", "EndDateTime"))  # add a duration column

	# ONE: distribution of film permits by borough
	temp_df = df.groupBy("Borough").count().orderBy("count", ascending=False)
	print("Distribution of film permits by borough")
	temp_df.show()  # see output 1 in output.md

	try:
		print("Writig File: permits-by-borough.csv")
		temp_df.write.csv("permits-by-borough.csv", header=False)
	except:
		print("Failed to write file: permits-by-borough.csv")
		pass

	# TWO: distibution of film permits by type
	temp_df = df.groupBy("EventType").count().orderBy("count", ascending=False)
	print("Distibution of film permits by type")
	temp_df.show()  # see output 2 in output.md
	try:
		print("Writing file: permits-by-type.csv")
		temp_df.write.csv("permits-by-type.csv")
	except:
		print("Failed to write file: permits-by-type.csv")
		pass

	# THREE: distribution by type and borough
	temp_df = df.groupBy(["Borough", "EventType"]).count().orderBy(
		["Borough", "count"], ascending=False)
	temp_df.show()  # See output 3 in output.md

	try:
		print("Writing file: borough-deep-view.csv")
		temp_df.write.csv("borough-deep-view.csv")
	except:
		print("Failed to write: borough-deep-view.csv")
		pass

	# FOUR: popularity of a neighborhood
	popularity(df)

	# FIVE: average duration of permits by type
	temp_df = df.select("EventType", "Duration (in Hours)").groupBy(
		"EventType").agg(mean("Duration (in Hours)").alias("Avg Duration"))
	temp_df = temp_df.orderBy("Avg Duration", ascending=False)
	print("Average Duration by Type")
	temp_df.show()  # See output 4 in output.md

	try:
		print("Writing file: duration-by-type.csv")
		temp_df.write.csv("duration-by-type.csv")
	except:
		print("Failed to  write: duration-by-type.csv")
		pass

	# SIX: average duration of permits by borough
	temp_df = df.select("Borough", "Duration (in Hours)").groupBy("Borough").agg(mean(
		"Duration (in Hours)").alias("Avg Duration")).orderBy("Avg Duration", ascending=False)
	print("Average Duration by Borough")
	temp_df.show()  # See output 5 in output.md

	try:
		print("Writing file: duration-by-borough.csv")
		temp_df.write.csv("duration-by-borough.csv")
	except:
		print("Failed to write: duration-by-borough.csv")
		pass

	return df


if __name__ == "__main__":

	spark = SparkSession.builder.getOrCreate()
	sc = spark.sparkContext

	start = time.time()

	# import the main dataset
	film_permits = spark.read.csv(
		"/user/hsc367/film_permits.csv", header=True, inferSchema=True)
	# get basic analysis or tfhe data
	get_analyis(film_permits)

	# get count of valid permits between 2010-2020
	df = film_permits
	# df_cols = map_cols(df)
	dic = get_seasonality(spark, df.select("StartDateTime", "EndDateTime"))

	end = time.time()
	print("Time taken: {}".format(end-start))
	print("Files produced: permits-by-borough.csv, permits-by-type.csv, borough-deep-view.csv, popularity.csv, duration-by-type.csv, duration-by-borough.csv, overall-popularity.csv, popularity-year-2010...2019.csv, ")
