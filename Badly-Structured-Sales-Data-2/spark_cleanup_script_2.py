from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType, DateType
import pyspark.sql.functions as F
import pandas as pd
import os
import shutil
import datetime
import re

# Define a UDF to parse the GregorianCalendar string and convert to formatted date string
def parse_gregorian_calendar(gc_str):
    year = int(re.search(r"YEAR=(\d+)", gc_str).group(1))
    month = int(re.search(r"MONTH=(\d+)", gc_str).group(1)) + 1
    day = int(re.search(r"DAY_OF_MONTH=(\d+)", gc_str).group(1))
    date = datetime.date(year, month, day)
    return date.strftime('%d-%b-%y')

# Function to create DataFrame for a specific business unit
def create_shipmode_df(df, ship_mode_col):
    bu_index = df.columns.index(ship_mode_col)
    selected_cols = [df.columns[0], df.columns[bu_index], df.columns[bu_index+1], df.columns[bu_index+2]]
    
    df_bu = df.select(selected_cols)
    col_renames = {
        df_bu.columns[0]: 'OrderDate',
        df_bu.columns[1]: 'Consumer',
        df_bu.columns[2]: 'Corporate',
        df_bu.columns[3]: 'HomeOffice'
    }
    for old_name, new_name in col_renames.items():
        df_bu = df_bu.withColumnRenamed(old_name, new_name)
    
    return df_bu.withColumn("ShipMode", F.lit(ship_mode_col))
							  

# Function to import and clean initial Excel file
def import_and_clean_excel(excel_path):
    pdf = pd.read_excel(excel_path, sheet_name='Dirty 2')
    df = spark.createDataFrame(pdf)
    df = df.withColumn('index', F.monotonically_increasing_id())
    return df.filter(~df.index.isin([0, 1]))

# Function to process cleaned DataFrame and build the final table
def process_and_build_table(clean_df):
    business_units = ["Consumer", "Corporate", "HomeOffice"]
    ship_modes = ["First Class", "Same Day", "Second Class", "Standard Class"]
    
    schema = StructType([
        StructField("ShipMode", StringType(), True),
        StructField("Segment", StringType(), True),
        StructField("OrderDate", DateType(), False),
        StructField("Sales", FloatType(), False)
    ])
    
    df_union = spark.createDataFrame([], schema)
    
    for sm in ship_modes:
        df_ship = create_shipmode_df(clean_df, sm)
        
        for bu in business_units:													   
            df_bu = df_ship.withColumn("BusinessUnit", F.lit(bu))
            df_bu= df_bu.na.drop()
            df_bu = df_bu.select("ShipMode", "BusinessUnit", "OrderDate", F.round(bu, 2)).filter(F.col(bu) > 0.00).where(F.isnan(F.col(bu)) == False)
            df_union = df_union.union(df_bu)

    # Register UDF
    parse_gregorian_calendar_udf = F.udf(parse_gregorian_calendar, StringType())

    # Apply UDF to DataFrame
    df_union = df_union.withColumn("OrderDate", parse_gregorian_calendar_udf(df_union["OrderDate"]))

    return df_union

# Create Spark Session
spark = SparkSession.builder.appName('DataCleanup').getOrCreate()

# Clear output folder if it exists
output_path = "D:/DataCleanupTests/Badly-Structured-Sales-Data-2/output"

											
if os.path.exists(output_path):
    shutil.rmtree(output_path)

# Import file and initial cleanup
excel_file_path = 'Badly-Structured-Sales-Data-2/2.-Badly-Structured-Sales-Data-2.xlsx'
input_df = import_and_clean_excel(excel_file_path)

# Process and return final formatted DataFrame
output_df = process_and_build_table(input_df)

# Write the output DataFrame to CSV
output_df.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").option("sep", ",").save("file:///D:/DataCleanupTests/Badly-Structured-Sales-Data-2/output")

# Stop Spark Session
spark.stop()
