from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.table(
    name="retail_q.silver.account",
    comment="Standardized Salesforce account data with data quality rules applied"
)
@dp.expect_or_fail("valid_id", "Id IS NOT NULL")
@dp.expect_or_fail("valid_name", "Name IS NOT NULL")
@dp.expect_or_drop("valid_annual_revenue", "AnnualRevenue IS NULL OR AnnualRevenue >= 0")
@dp.expect_or_drop("valid_employee_count", "NumberOfEmployees IS NULL OR NumberOfEmployees >= 0")
@dp.expect("active_account", "IsDeleted = false")
def account():
    return (
        spark.read.table("retail_q.salesforce_bronze.account")
        .select(
            upper(trim(col("Id"))).alias("Id"),
            upper(trim(col("MasterRecordId"))).alias("MasterRecordId"),
            upper(trim(col("ParentId"))).alias("ParentId"),
            col("IsDeleted"),
            initcap(trim(col("Name"))).alias("Name"),
            initcap(trim(col("Type"))).alias("Type"),
            initcap(trim(col("Industry"))).alias("Industry"),
            trim(col("BillingStreet")).alias("BillingStreet"),
            initcap(trim(col("BillingCity"))).alias("BillingCity"),
            initcap(trim(col("BillingState"))).alias("BillingState"),
            trim(col("BillingPostalCode")).alias("BillingPostalCode"),
            initcap(trim(col("BillingCountry"))).alias("BillingCountry"),
            upper(trim(col("BillingStateCode"))).alias("BillingStateCode"),
            upper(trim(col("BillingCountryCode"))).alias("BillingCountryCode"),
            col("BillingLatitude"),
            col("BillingLongitude"),
            col("BillingGeocodeAccuracy"),
            trim(col("ShippingStreet")).alias("ShippingStreet"),
            initcap(trim(col("ShippingCity"))).alias("ShippingCity"),
            initcap(trim(col("ShippingState"))).alias("ShippingState"),
            trim(col("ShippingPostalCode")).alias("ShippingPostalCode"),
            initcap(trim(col("ShippingCountry"))).alias("ShippingCountry"),
            upper(trim(col("ShippingStateCode"))).alias("ShippingStateCode"),
            upper(trim(col("ShippingCountryCode"))).alias("ShippingCountryCode"),
            col("ShippingLatitude"),
            col("ShippingLongitude"),
            col("ShippingGeocodeAccuracy"),
            trim(col("Phone")).alias("Phone"),
            trim(col("Fax")).alias("Fax"),
            lower(trim(col("Website"))).alias("Website"),
            trim(col("PhotoUrl")).alias("PhotoUrl"),
            col("AnnualRevenue"),
            col("NumberOfEmployees"),
            current_timestamp().alias("silver_processed_at")
        )
    )
