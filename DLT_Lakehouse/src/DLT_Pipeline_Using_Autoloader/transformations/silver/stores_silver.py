import dlt
from pyspark.sql.functions import *

# creating expectations
expectations = {
    "rule1":"store_id is not null"
}

# streaming view
@dlt.view(
    name = "stores_silver_view"
)
@dlt.expect_all_or_fail(expectations)
def stores_silver_view():
    df = spark.readStream.table('bronze.bronze_stores')
    df = df.withColumn('store_name',regexp_replace(col('store_name'),"_",""))
    df = df.withColumn('processDate',current_timestamp())
    return df

# streaming scd 2 table
dlt.create_streaming_table(
    name = 'silver.stores_silver'
)

dlt.create_auto_cdc_flow(
    target = 'silver.stores_silver',
    source = 'stores_silver_view',
    keys = ['store_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type = 2,
    except_column_list = ['processDate']
)