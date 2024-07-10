from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, FloatType, StringType
import pyspark.sql.functions as func
import pandas as pd
import os
import shutil

def create_business_unit_dataframe(source_dataframe,business_unit):
    first_col = source_dataframe.columns.index(business_unit)
    consurmer_cols = [source_dataframe[0],source_dataframe[first_col], source_dataframe[first_col+1], source_dataframe[first_col+2], source_dataframe[first_col+3]]
    dataframe_to_return = source_dataframe.select(consurmer_cols)
    dataframe_to_return = dataframe_to_return.withColumnRenamed(dataframe_to_return.columns[0], 'OrderID') \
        .withColumnRenamed(dataframe_to_return.columns[1], 'FirstClass') \
        .withColumnRenamed(dataframe_to_return.columns[2], 'SameDay') \
        .withColumnRenamed(dataframe_to_return.columns[3], 'SecondClass') \
        .withColumnRenamed(dataframe_to_return.columns[4], 'StandardClass')
    dataframe_to_return = dataframe_to_return.withColumn("BusinessUnit", func.lit(business_unit))
    return dataframe_to_return

def import_inital_clean_file (excel_filename_path):
    # Load Excel data, remove unused columns and filter unsued rows.
    dirty_excel_data = pd.read_excel(open(excel_filename_path, 'rb'),sheet_name='Dirty 1')  
    dirty_df = sparkSession.createDataFrame(dirty_excel_data)
    drop_columns = ["Consumer Total", "Corporate Total", "Home Office Total"]
    dirty_df = dirty_df.drop(*drop_columns).withColumn('index', func.monotonically_increasing_id())
    return dirty_df.filter(~dirty_df.index.isin([0,1])) 

def process_and_build_table (clean_dataframe):
    business_units = ["Consumer","Corporate","Home Office"]
    ship_modes = ["FirstClass", "SameDay", "SecondClass", "StandardClass"]

    schema = StructType([StructField("Segment", StringType(), True),
                        StructField("ShipMode", StringType(), True),
                        StructField("OrderID", StringType(), False),
                        StructField("Sales", FloatType(), False)]
                        )


    df_union = sparkSession.createDataFrame([], schema)
    for bu in business_units:
        for ship_mode in ship_modes:
            df = create_business_unit_dataframe(clean_dataframe,bu)
            df = df.withColumn("ShipMode", func.lit(ship_mode))
            df = df.na.drop()
            df = df.select("BusinessUnit","ShipMode","OrderID",func.round(ship_mode, 2)).filter(func.col(ship_mode)>0.00).where(func.isnan(func.col(ship_mode)) == False)
            df_union = df_union.union(df)
    return df_union

# Create Spark Session 
sparkSession = SparkSession.builder.appName('Cleanuptest1').getOrCreate()

# Clear output folder
output_path = "D:/DataCleanupTests/Badly-Structured-Sales-Data-1/output.csv"

# Clean up the output directory if it exists
if os.path.exists(output_path):
    shutil.rmtree(output_path)

# Import file and intial cleanup
excel_file_path = 'Badly-Structured-Sales-Data-1\\1.-Badly-Structured-Sales-Data-1.xlsx'
input_dataframe = import_inital_clean_file(excel_file_path)

# Process and return final formatted dataframe
output_dataframe = process_and_build_table(input_dataframe)
#output_dataframe.show()
output_dataframe.coalesce(1).write.format("csv").mode("overwrite").options(header="true",sep=",").save("file:///D:/DataCleanupTests/Badly-Structured-Sales-Data-1/output.csv")

# Stop Spark Session
sparkSession.stop()
