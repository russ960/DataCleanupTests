from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType
import pyspark.sql.functions as F
import pandas as pd
import os
import shutil

# Function to create DataFrame for a specific business unit
def create_business_unit_df(df, business_unit_col):
    bu_index = df.columns.index(business_unit_col)
    selected_cols = [df.columns[0], df.columns[bu_index], df.columns[bu_index+1], df.columns[bu_index+2], df.columns[bu_index+3]]
    
    df_bu = df.select(selected_cols)
    col_renames = {
        df_bu.columns[0]: 'OrderID',
        df_bu.columns[1]: 'FirstClass',
        df_bu.columns[2]: 'SameDay',
        df_bu.columns[3]: 'SecondClass',
        df_bu.columns[4]: 'StandardClass'
    }
    for old_name, new_name in col_renames.items():
        df_bu = df_bu.withColumnRenamed(old_name, new_name)
    
    return df_bu.withColumn("BusinessUnit", F.lit(business_unit_col))
							  

# Function to import and clean initial Excel file
def import_and_clean_excel(excel_path):
    pdf = pd.read_excel(excel_path, sheet_name='Dirty 1')
    df = spark.createDataFrame(pdf)
    drop_cols = ["Consumer Total", "Corporate Total", "Home Office Total"]
    df = df.drop(*drop_cols).withColumn('index', F.monotonically_increasing_id())
    return df.filter(~df.index.isin([0, 1]))

# Function to process cleaned DataFrame and build the final table
def process_and_build_table(clean_df):
    business_units = ["Consumer", "Corporate", "Home Office"]
    ship_modes = ["FirstClass", "SameDay", "SecondClass", "StandardClass"]
    
    schema = StructType([
        StructField("Segment", StringType(), True),
        StructField("ShipMode", StringType(), True),
        StructField("OrderID", StringType(), False),
        StructField("Sales", FloatType(), False)
    ])
    
    df_union = spark.createDataFrame([], schema)
    
    for bu in business_units:
        df_bu = create_business_unit_df(clean_df, bu)
        
        for ship_mode in ship_modes:
																   
            df_ship = df_bu.withColumn("ShipMode", F.lit(ship_mode))
            df_ship = df_ship.na.drop()
            df_ship = df_ship.select("BusinessUnit", "ShipMode", "OrderID", F.round(ship_mode, 2)).filter(F.col(ship_mode) > 0.00).filter(df_ship.OrderID != 'Grand Total').where(F.isnan(F.col(ship_mode)) == False)
            
            df_union = df_union.union(df_ship)
    
    return df_union

# Create Spark Session
spark = SparkSession.builder.appName('DataCleanup').getOrCreate()

# Clear output folder if it exists
output_path = "D:/DataCleanupTests/Badly-Structured-Sales-Data-1/output"

											
if os.path.exists(output_path):
    shutil.rmtree(output_path)

# Import file and initial cleanup
excel_file_path = 'Badly-Structured-Sales-Data-1/1.-Badly-Structured-Sales-Data-1.xlsx'
input_df = import_and_clean_excel(excel_file_path)

# Process and return final formatted DataFrame
output_df = process_and_build_table(input_df)

# Write the output DataFrame to CSV
output_df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").option("sep", ",").save("file:///D:/DataCleanupTests/Badly-Structured-Sales-Data-1/output")

# Stop Spark Session
spark.stop()
