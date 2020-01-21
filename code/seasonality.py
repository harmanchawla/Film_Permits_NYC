import sys
import time
import json
import datetime
import itertools

from dateutil.parser import parse
from datetime import datetime, timedelta


from pyspark.sql.types import StringType, ArrayType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import lit, col, udf, mean, explode, to_timestamp
from pyspark.sql import SparkSession

def check_leap_year(year):
    '''  Check if a given year is a leap year '''
    if (year % 4) == 0:
        if (year % 100) == 0:
            if (year % 400) == 0:
                return True
            else:
                return False
        else:
            return True
    else:
        return False


def _init_dict():
    ''' Initialize a dictionary. Key: Datetime object, value: 0'''
    master_dct = {}
    for year in range(2011, 2022):
        for month in range(1, 13):
            if month == 2:
                if check_leap_year(year):
                    for date in range(1, 30):
                        str_date = "{}.{}.{}".format(month, date, year)
                        dateObj = datetime.strptime(str_date, "%m.%d.%Y")
                        dateObj = datetime.date(dateObj)
                        master_dct[dateObj] = 0
                else:
                    for date in range(1, 29):
                        str_date = "{}.{}.{}".format(month, date, year)
                        dateObj = datetime.strptime(str_date, "%m.%d.%Y")
                        dateObj = datetime.date(dateObj)
                        master_dct[dateObj] = 0
            elif month in [1, 3, 5, 7, 8, 10, 12]:
                for date in range(1, 32):
                    str_date = "{}.{}.{}".format(month, date, year)
                    dateObj = datetime.strptime(str_date, "%m.%d.%Y")
                    dateObj = datetime.date(dateObj)
                    master_dct[dateObj] = 0
            else:
                for date in range(1, 31):
                    str_date = "{}.{}.{}".format(month, date, year)
                    dateObj = datetime.strptime(str_date, "%m.%d.%Y")
                    dateObj = datetime.date(dateObj)
                    master_dct[dateObj] = 0
    return master_dct


def _valid_permits_by_date(df):
    ''' 
        Accepts a df with start and end date. 
        Returns a dictioanry with dates as key and valid permits as values
    '''
    # initialize master dict
    master_dct = _init_dict()
    #print(master_dct)

    start = df.select("startDate").rdd.map(
        lambda x: x.startDate).sortBy(lambda x: x)
    start = start.collect()
    print("Start Dates collected")

    end = df.select("endDate").rdd.map(lambda x: x.endDate).sortBy(lambda x: x)
    end = end.collect()
    print("End Dates collected")

    i, j = 0, 0
    while i < len(start) and j < len(end):
        print(i)
        if start[i] <= end[j]:
            master_dct[start[i]] += 1
            i = i + 1
        else:
            endDate = end[j]
            endDate += timedelta(days=1)
            master_dct[endDate] -= 1
            j = j + 1

    while i < len(start):
        print(i)
        master_dct[start[i]] += 1
        i = i + 1

    while j < len(end):
        print(j)
        endDate = end[j]
        endDate += timedelta(days=1)
        master_dct[endDate] -= 1
        j = j + 1

    return master_dct



def _convert_to_dates(element):
    from dateutil.parser import parse
    try:
        date = parse(element)
        if date.year > 2022 or date.year < 2008:
            return parse('01-01-0001')
        else:
            return date
    except:
        return parse('01-01-0001')


_get_dates = udf(_convert_to_dates, DateType())


def get_seasonality(spark, df):
    ''' Produces a json file with key as dates and values as number of valid permits'''
    sc = spark.sparkContext
    # df_times = df.select(to_timestamp("StartDateTime", 'yyyy-MM-dd').alias("startDate"), to_timestamp("EndDateTime", 'yyyy-MM-dd').alias("endDate"))

    df_times = df.withColumn("startDate", _get_dates("StartDateTime")).withColumn(
        "endDate", _get_dates("EndDateTime"))
    df_times = df_times.select("startDate", "endDate")
    df_times.printSchema()
    df_times.show(5)

    dict_times = _valid_permits_by_date(df_times)
    new_dict = {}

    for key, value in dict_times.items():
        str_date = key.strftime("%d/%m/%Y")
        new_dict[str_date] = abs(value)

    with open('seasonality.json', 'w') as json_file:
        json.dump(new_dict, json_file)

    return dict_times

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    df = spark.read.csv("/user/hsc367/film_permits.csv",
                        header=True, inferSchema=True)
    df.select("StartDateTime", "EndDateTime").show()
    dic = get_seasonality(spark, df.select("StartDateTime", "EndDateTime"))
