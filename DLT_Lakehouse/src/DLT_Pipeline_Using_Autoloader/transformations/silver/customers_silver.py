import dlt
from pyspark.sql.functions import *

# creating expectations
expectations = {
    "rule1":"customer_id is not null"
}

# streaming view
@dlt.view(
    name = "customers_silver_view"
)
@dlt.expect_all_or_fail(expectations)
def customers_silver_view():
    df = spark.readStream.table('bronze.bronze_customers')
    df = df.withColumn('name',upper(col('name')))
    df = df.withColumn('domain',split(col('email'),'@')[1])
    df = df.withColumn('processDate',current_timestamp())
    return df

# streaming scd 2 table
dlt.create_streaming_table(
    name = 'silver.customers_silver'
)

dlt.create_auto_cdc_flow(
    target = 'silver.customers_silver',
    source = 'customers_silver_view',
    keys = ['customer_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type = 2,
    except_column_list = ['processDate']
)