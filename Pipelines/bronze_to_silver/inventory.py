from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.table(
    name="retail_q.silver.inventory",
    comment="Standardized inventory data with data quality rules applied - active records only"
)
@dp.expect_or_fail("valid_inventory_id", "inventory_id IS NOT NULL")
@dp.expect_or_fail("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_fail("valid_store_id", "store_id IS NOT NULL")
@dp.expect_or_drop("valid_stock_quantity", "stock_quantity >= 0")
@dp.expect_or_drop("valid_reorder_level", "reorder_level >= 0")
@dp.expect("low_stock_warning", "stock_quantity > reorder_level OR reorder_level IS NULL")
def inventory():
    return (
        spark.read.table("retail_q.postgres_bronze.inventory")
        .filter(col("__END_AT").isNull())
        .select(
            upper(trim(col("inventory_id"))).alias("inventory_id"),
            upper(trim(col("product_id"))).alias("product_id"),
            upper(trim(col("store_id"))).alias("store_id"),
            col("stock_quantity"),
            col("reorder_level"),
            initcap(trim(col("warehouse_location"))).alias("warehouse_location"),
            col("last_stock_update"),
            current_timestamp().alias("silver_processed_at")
        )
    )
