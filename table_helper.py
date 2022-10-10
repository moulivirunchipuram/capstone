import pandas as pd
import seaborn as sns
import datetime as dt
import numpy as np

from pyspark.sql.functions import avg

from pyspark.sql.functions import col, udf, dayofmonth, dayofweek, month, year, weekofyear,date_format
from pyspark.sql.functions import monotonically_increasing_id
#from pyspark.sql.types import *

from pyspark.sql.functions import date_add as d_add
from pyspark.sql.types import DoubleType, StringType, IntegerType, FloatType
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql import Row



import util as util


def create_status(input_df, output_path):
    """
        Get status info from fact and write to parquet
        
        :param input_df: dataframe of input data.
        :param output_path: path to write data to.
        :return: status dimension dataframe
    """    
    
    
    output_df = input_df.withColumn("status_flag_id", monotonically_increasing_id()) \
                .select(["status_flag_id", "entdepa", "entdepd", "matflag"]) \
                .withColumnRenamed("entdepa", "arrival_flag")\
                .withColumnRenamed("entdepd", "departure_flag")\
                .withColumnRenamed("matflag", "match_flag")\
                .dropDuplicates(["arrival_flag", "departure_flag", "match_flag"])
    
    util.output_to_parquet_file(output_df, output_path, "status")
    
    return output_df

def create_time(input_df, output_path):   
    """
        Get time info from fact and extract year, month, day, week, weekday
        and write to parquet
        
        :param input_df: dataframe of input data.
        :param output_path: path to write data to.
        :return: time dimension dataframe
    """
    
    from datetime import datetime, timedelta
    from pyspark.sql import types as T
    
    def convert_datetime(x):
        try:
            start = datetime(1960, 1, 1)
            return start + timedelta(days=int(x))
        except:
            return None
    
    udf_datetime_from_sas = udf(lambda x: convert_datetime(x), T.DateType())


    output_df = input_df.select(["arrdate"])\
                .withColumn("arrival_date", udf_datetime_from_sas("arrdate")) \
                .withColumn('day', F.dayofmonth('arrival_date')) \
                .withColumn('month', F.month('arrival_date')) \
                .withColumn('year', F.year('arrival_date')) \
                .withColumn('week', F.weekofyear('arrival_date')) \
                .withColumn('weekday', F.dayofweek('arrival_date'))\
                .select(["arrdate", "arrival_date", "day", "month", "year", "week", "weekday"])

    
    util.output_to_parquet_file(output_df, output_path, "time")
    
    return output_df


def create_visa(input_df, output_path):
    """
        Get all visa details create dataframe and write data into parquet files.
        visa_id is the unique identifier
        
        :param input_df: dataframe of input data.
        :param output_data: path to write data to.
        :return: dataframe representing visa dimension
    """
    
    output_df = input_df.withColumn("visa_id", monotonically_increasing_id()) \
                .select(["visa_id","i94visa", "visatype", "visapost"]) \
                .dropDuplicates(["i94visa", "visatype", "visapost"])
    
    util.output_to_parquet_file(output_df, output_path, "visa")
    
    return output_df


def create_state(input_df, output_path):
    """
        Get state specific data and create dataframe and write data into parquet files.
        Here we will group the information by state code to compute sum and average 
        Rename the columns from Xxxx Yyyy to xxx_yyy
        Drop rows with null values
        
        :param input_df: dataframe of input data.
        :param output_data: path to write data to.
        :return: dataframe representing state dimension
    """
    
    output_df = input_df.select(["State Code", "State", "Median Age", "Male Population", "Female Population", "Total Population", "Average Household Size",\
                          "Foreign-born", "Race", "Count"])\
                .withColumnRenamed("State","state")\
                .withColumnRenamed("State Code", "state_code")\
                .withColumnRenamed("Median Age", "median_age")\
                .withColumnRenamed("Male Population", "male_population")\
                .withColumnRenamed("Female Population", "female_population")\
                .withColumnRenamed("Total Population", "total_population")\
                .withColumnRenamed("Average Household Size", "avg_household_size")\
                .withColumnRenamed("Foreign-born", "foreign_born")\
                .withColumnRenamed("Race", "race")\
                .withColumnRenamed("Count", "count")
    
    output_df = output_df.groupBy("state_code","state").agg(
                mean('median_age').alias("median_age"),\
                sum("total_population").alias("total_population"),\
                sum("male_population").alias("male_population"), \
                sum("female_population").alias("female_population"),\
                sum("foreign_born").alias("foreign_born"), \
                sum("avg_household_size").alias("average_household_size")
                ).dropna()
    
    util.output_to_parquet_file(output_df, output_path, "state")
    
    return output_df


def create_airport(input_df, output_path):
    """
        Collect airport data, create dataframe and write data into parquet files.
        
        :param input_df: dataframe of input data.
        :param output_data: path to write data to.
        :return: dataframe representing airport dimension
    """
    
    output_df = input_df.select(["ident", "type", "iata_code", "name", "iso_country", "iso_region", "municipality", "gps_code", "coordinates", "elevation_ft"])\
                .dropDuplicates(["ident"])
    
    util.output_to_parquet_file(output_df, output_path, "airport")
    
    return output_df


def create_temperature(input_df, output_path):
    """
        Collect temperature data, get average temperature
        by grouping by country
        
        :param input_df: dataframe of input data.
        :param output_data: path to write data to.
        :return: dataframe representing temperature dimension
    """
    print("creating temperature table data")
    output_df = input_df.groupBy("Country","country").agg(
                mean('AverageTemperature').alias("average_temperature"),\
                mean("AverageTemperatureUncertainty").alias("average_temperature_uncertainty")
            ).dropna()\
            .withColumn("temperature_id", monotonically_increasing_id()) \
            .select(["temperature_id", "country", "average_temperature", "average_temperature_uncertainty"])
    
    util.output_to_parquet_file(output_df, output_path, "temperature")
    
    return output_df

def create_country(input_df, output_path):
    """
        Collect country data, create dataframe and write data into parquet files.
        
        :param input_df: dataframe of input data.
        :param output_path: path of output parquet file.
        :return: country dimension( in this case it is the input df)
    """
    
    util.output_to_parquet_file(input_df, output_path, "country")
    
    return input_df

def create_immigration(immigration_spark, output_path, spark):
    """
        Collect all immigration data, join dimension tables, create data frame and write to parquet file.        
        :param input_df: dataframe of input data.
        :param output_path: path of output parquet file 
        :return: final immigration fact data frame
    """
    
    airport = spark.read.parquet(output_path+"airport")
    country = spark.read.parquet(output_path+"country")
    temperature = spark.read.parquet(output_path+"temperature")
    country_temperature = spark.read.parquet(output_path+"country_temperature_mapping")
    state = spark.read.parquet(output_path+"state")
    status = spark.read.parquet(output_path+"status")
    time = spark.read.parquet(output_path+"time")
    visa = spark.read.parquet(output_path+"visa")

    # join all tables to immigration
    output_df = immigration_spark.select(["*"])\
                .join(airport, (immigration_spark.i94port == airport.ident), how='full')\
                .join(country_temperature, (immigration_spark.i94res == country_temperature.code), how='full')\
                .join(status, (immigration_spark.entdepa == status.arrival_flag) & (immigration_spark.entdepd == status.departure_flag)\
                    & (immigration_spark.matflag == status.match_flag), how='full')\
                .join(visa, (immigration_spark.i94visa == visa.i94visa) & (immigration_spark.visatype == visa.visatype)\
                    & (immigration_spark.visapost == visa.visapost), how='full')\
                .join(state, (immigration_spark.i94addr == state.state_code), how='full')\
                .join(time, (immigration_spark.arrdate == time.arrdate), how='full')\
                .where(col('cicid').isNotNull())\
                .select(["cicid", "i94res", "depdate", "i94mode", "i94port", "i94cit", "i94addr", "airline", "fltno", "ident", "code",\
                         "temperature_id", "status_flag_id", "visa_id", "state_code", country_temperature.country,time.arrdate.alias("arrdate")])
    
    #util.output_to_parquet_file(output_df, output_path, "immigration")
    
    return output_df
