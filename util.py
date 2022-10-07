# Import Libraries
import pandas as pd
import numpy as np
#pd.set_option('display.width',170, 'display.max_rows',200, 'display.max_columns',900)
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import types as T



    
def report_unique_info(df,df_name,column_list):
    """
        This is a generic utility api to check how many unique 
        values are there in a column
        
        :param df : input dataframe
        :param df_name : name of the dataframe (for printing)
        :param column_list: list of columns to be checked for unique values
    """
    
    print(f"Displaying number of unique values for given columns in  {df_name} dataframe")
    for column in column_list:
        unique_values = df[column].unique()
        print(f"There are {unique_values.size} unique values in column {column}")
        

def cleanup_missing_column_values(df,column_list):
    """
        Remove rows with null values for columns specified in column_list
        
        :param df : input dataframe
        :param column_list: list of columns to be checked for null values
    """
    print(f"removing rows with null values for {column_list}")
    print(f"total rows before clean up {df.shape[0]}")
    for column in column_list:
        bool_series = pd.notnull(df[column])
        df = df[bool_series]
        
    print(f"total rows after clean up {df.shape[0]}")
    
    return df

## write to parquet 
def output_to_parquet_file(df, output_path, table_name):
    """
        Writes the dataframe as parquet file.
        
        :param df: dataframe to write
        :param output_path: output path where to write
        :param table_name: name of the table
    """
    
    file_path = output_path + table_name
    
    print("Writing table {} to {}".format(table_name, file_path))
    
    df.write.mode("overwrite").parquet(file_path)
    
    print("Write complete!")

    
def run_record_count_check(df, table_name):
    """Check row count for data sanity. If no records found, report error 
        :param input_df: spark dataframe to check counts on.
        :param table_name: name of table
    """
    
    count = df.count()

    if (count == 0):
        print("Record count check failed for {} table with zero records!".format(table_name))
    else:
        print("Record count check passed for {} table with record count: {} records.".format(table_name, count))
        
    return 0    
    
    
          