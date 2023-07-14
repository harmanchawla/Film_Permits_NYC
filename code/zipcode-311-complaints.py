import sys
import time
import json
from pyspark import SparkSession
 
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit, col, udf

def checkInt(element):
    '''
        Check if an element is integer
        Regex: start to end just digits
    '''
    try: 
        if value := int(element):
            if len(str(value)) == 5:
            	return "True"
    except:
        pass
    try:
        reInt = "^[0-9]*$"
        if re.search(reInt, element):
            return "True"
    except:
        pass
    return "False"


def get_df_for_year(df, year):
    return df.filter(col("Created Date").like(f"%/{year}%")) 

def get_df_for_borough(df, borough):
    return df.filter(col("Borough")==borough)

def get_variation_with_years(df_years):
    for year, df_year in df_years.items():
        df = df_year.groupBy(["Complaint Type"]).count().orderBy("count", ascending=False)
        df.show(5)
        df = df.select("Complaint Type", "count")
        df.write.csv(f"/user/hsc367/task3-year-{year}.csv")
        print(f"File saved to: /user/hsc367/task3-year-{year}.csv")

def get_variation_with_neighborhood(df_years):
    for year, df_year in df_years.items():
        df = df_years.filer(col("Zip")=="True")
        df = df.groupBy(["Zip Code", "Complaint Type"]).count().orderBy("count", ascending=False)
        df.show(5)
        df.write.csv(f"/user/hsc367/task3-neighborhoods-{year}.csv")
        print(f"File saved to: /user/hsc367/task3-neighborhoods-{year}.csv")


def get_variations_with_borough(df_boroughs):
    for borough, df in df_boroughs.items():
        df = df.groupBy(["Complaint Type"]).count().orderBy("count", ascending=False)
        df.show(5)
        df = df.select("Complaint Type", "count")
        df.write.csv(f"/user/hsc367/task3-year-{year}.csv")
        print(f"File saved to: /user/hsc367/task3-year-{year}.csv")

if __name__ == "__main__":

    # initiate spark and spark context

    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    # start time 
    start_time = time.time()

    # import the dataset
    _311_reports_df = spark.read.csv("/user/hm74/NYCOpenData/erm2-nwe9.tsv.gz", header='True', inferSchema='True', sep='\t')
    df = _311_reports_df 

    df_boroughs = {}
    checkZipCode = udf(checkInt, StringType())

    # filter and keep only attributes that we need for our analysis
    # [type of compliant, compliant id, date open, date close, response time*, borough, zipcode, neighborhood, ]
    df = df.select("Unique Key", "Created Date", "Closed Date", "Complaint Type", "Incident Zip", "Status", "Borough", "X Coordinate (State Plane)", "Y Coordinate (State Plane)", "Latitude", "Longitude", "Location")
    df = df.withColumn("Zip", checkZipCode("Incident Zip"))

    df_years = {i: get_df_for_year(df, i) for i in range(2010, 2020, 1)}
    # similarly for boroughs 
    # borough = df.select("Borough").distinct().collect()
    # boroughs = []
    # for element in borough:
    # 	boroughs.append(element.borough)

    # for element in boroughs:
    # 	df_boroughs[element] = get_df_for_borough(df, borough)

    # variation with years 
    #get_variation_with_years(df_years)

    # variation with neighborhoods
    get_variation_with_neighborhood(df_years)

    # correlation with tourist attrations


    # end time
    print("Time taken: ", time.time() - start)

