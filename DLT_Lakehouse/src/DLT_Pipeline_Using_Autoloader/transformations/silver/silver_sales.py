import dlt
from pyspark.sql.functions import *

# creating expectations
expectations = {
    "rule1":"sales_id is not null"
}

# streaming view
@dlt.view(
    name = "sales_silver_view"
)
@dlt.expect_all_or_fail(expectations)
def sales_silver_view():
    df = spark.readStream.table('bronze.bronze_sales')
    df = df.withColumn('price_per_sale',round(col('total_amount')/col('quantity'),2))
    df = df.withColumn('processDate',current_timestamp())
    return df

# streaming scd 2 table
dlt.create_streaming_table(
    name = 'silver.sales_silver'
)

dlt.create_auto_cdc_flow(
    target = 'silver.sales_silver',
    source = 'sales_silver_view',
    keys = ['sales_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type = 2,
    except_column_list = ['processDate']
)