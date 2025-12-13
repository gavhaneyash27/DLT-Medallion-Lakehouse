import dlt
from pyspark.sql.functions import *

# creating expectations
expectations = {
    "rule1":"product_id is not null"
}

# streaming view
@dlt.view(
    name = "products_silver_view"
)
@dlt.expect_all_or_fail(expectations)
def products_silver_view():
    df = spark.readStream.table('bronze.bronze_products')
    df = df.withColumn('processDate',current_timestamp())
    return df

# streaming scd 2 table
dlt.create_streaming_table(
    name = 'silver.products_silver'
)

dlt.create_auto_cdc_flow(
    target = 'silver.products_silver',
    source = 'products_silver_view',
    keys = ['product_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type = 2,
    except_column_list = ['processDate']
)