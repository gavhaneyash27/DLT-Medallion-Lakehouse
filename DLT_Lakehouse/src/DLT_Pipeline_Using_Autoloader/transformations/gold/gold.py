import dlt
from pyspark.sql.functions import *

# customers gold table
@dlt.table(
    name = "gold.customers_gold"
)
def customers_gold():
    df = spark.read.table('silver.customers_silver')
    df = df.filter("__END_AT IS NULL").drop("_rescued_data", "__START_AT", "__END_AT")
    return df

# products gold table
@dlt.table(
    name = "gold.products_gold"
)
def products_gold():
    df = spark.read.table('silver.products_silver')
    df = df.filter("__END_AT IS NULL").drop("_rescued_data", "__START_AT", "__END_AT")
    return df

# fact sales gold table
@dlt.table(
    name = "gold.factsales_gold"
)
def factsales_gold():
    df = spark.read.table('silver.sales_silver')
    df = df.filter("__END_AT IS NULL").drop("_rescued_data", "__START_AT", "__END_AT")
    return df

# stores gold table
@dlt.table(
    name = "gold.stores_gold"
)
def stores_gold():
    df = spark.read.table('silver.stores_silver')
    df = df.filter("__END_AT IS NULL").drop("_rescued_data", "__START_AT", "__END_AT")
    return df