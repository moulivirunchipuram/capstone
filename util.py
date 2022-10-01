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
    print("=============")
    print(f"Displaying number of unique values for given columns in  {df_name} dataframe")
    for column in column_list:
        unique_values = df[column].unique()
        print(f"There are {unique_values.size} unique values in column {column}")
        

def cleanup_missing_column_values(df,column_list):
    print(f"removing rows with null values for {column_list}")
    print(f"total rows before clean up {df.shape[0]}")
    for column in column_list:
        bool_series = pd.notnull(df[column])
        df = df[bool_series]
        
    print(f"total rows after clean up {df.shape[0]}")
    
    return df


# #The following api was suggested by mentor
# def convert_sas_to_date(sas_date):
#     print(f"sas_date {sas_date}")
#     if sas_date is None:
#         dt = None
#     else:
#         dt = datetime.fromordinal(datetime(1960, 1, 1).toordinal() + int(sas_date))
#     return dt
    




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
    
    
    
    
          