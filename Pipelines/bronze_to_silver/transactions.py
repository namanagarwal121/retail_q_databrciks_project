from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.table(
    name="retail_q.silver.transactions",
    comment="Standardized transactions data with type conversions and data quality rules applied"
)
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dp.expect_or_drop("valid_quantity", "quantity > 0")
@dp.expect_or_drop("valid_selling_price", "selling_price >= 0")
@dp.expect_or_drop("valid_discount_amount", "discount_amount >= 0")
@dp.expect("valid_discount", "discount_amount <= selling_price * quantity")
def transactions():
    return (
        spark.read.table("retail_q.blob_bronze.transactions")
        .select(
            upper(trim(col("transaction_id"))).alias("transaction_id"),
            upper(trim(col("product_id"))).alias("product_id"),
            upper(trim(col("store_id"))).alias("store_id"),
            initcap(trim(col("opportunity_name"))).alias("opportunity_name"),
            initcap(trim(col("payment_mode"))).alias("payment_mode"),
            initcap(trim(col("sales_channel"))).alias("sales_channel"),
            col("quantity").cast("int").alias("quantity"),
            col("selling_price").cast("decimal(10,2)").alias("selling_price"),
            col("discount_amount").cast("decimal(10,2)").alias("discount_amount"),
            col("transaction_timestamp").cast("timestamp").alias("transaction_timestamp"),
            col("_rescued_data"),
            current_timestamp().alias("silver_processed_at")
        )
    )
