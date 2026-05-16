from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.table(
    name="retail_q.silver.product_catalog",
    comment="Standardized product catalog with data quality rules applied - active records only"
)
@dp.expect_or_fail("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_fail("valid_product_name", "product_name IS NOT NULL")
@dp.expect_or_drop("valid_unit_price", "unit_price >= 0")
@dp.expect_or_drop("valid_category", "category IS NOT NULL")
@dp.expect("active_product_has_price", "NOT is_active OR (is_active AND unit_price > 0)")
def product_catalog():
    return (
        spark.read.table("retail_q.postgres_bronze.product_catalog")
        .filter(col("__END_AT").isNull())
        .select(
            upper(trim(col("product_id"))).alias("product_id"),
            initcap(trim(col("product_name"))).alias("product_name"),
            initcap(trim(col("category"))).alias("category"),
            initcap(trim(col("subcategory"))).alias("subcategory"),
            initcap(trim(col("brand"))).alias("brand"),
            initcap(trim(col("supplier_name"))).alias("supplier_name"),
            col("unit_price"),
            col("launch_date"),
            col("is_active"),
            col("updated_at"),
            current_timestamp().alias("silver_processed_at")
        )
    )
