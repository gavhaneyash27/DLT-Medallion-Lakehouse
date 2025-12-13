import dlt
from pyspark.sql.functions import *

# bronze_customers
@dlt.table(
    name = "bronze.bronze_customers"
)
def bronze_customers():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .load("/Volumes/databricksyash/raw_schema/bronze_volume/customers/")
    return df

# bronze_sales
@dlt.table(
    name = "bronze.bronze_sales"
)
def bronze_sales():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .load("/Volumes/databricksyash/raw_schema/bronze_volume/sales_data/")
    return df

# bronze_products
@dlt.table(
    name = "bronze.bronze_products"
)
def bronze_products():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .load("/Volumes/databricksyash/raw_schema/bronze_volume/products/")
    return df

# bronze_stores
@dlt.table(
    name = "bronze.bronze_stores"
)
def bronze_stores():
    df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .load("/Volumes/databricksyash/raw_schema/bronze_volume/stores/")
    return df









