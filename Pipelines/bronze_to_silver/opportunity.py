from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.table(
    name="retail_q.silver.opportunity",
    comment="Standardized Salesforce opportunity data with data quality rules applied"
)
@dp.expect_or_fail("valid_id", "Id IS NOT NULL")
@dp.expect_or_fail("valid_name", "Name IS NOT NULL")
@dp.expect_or_fail("valid_account_id", "AccountId IS NOT NULL")
@dp.expect_or_fail("valid_close_date", "CloseDate IS NOT NULL")
@dp.expect_or_drop("valid_amount", "Amount IS NULL OR Amount >= 0")
@dp.expect_or_drop("valid_probability", "Probability IS NULL OR (Probability >= 0 AND Probability <= 100)")
@dp.expect("won_opportunities_closed", "NOT IsWon OR (IsWon AND IsClosed)")
def opportunity():
    return (
        spark.read.table("retail_q.salesforce_bronze.opportunity")
        .select(
            upper(trim(col("Id"))).alias("Id"),
            upper(trim(col("AccountId"))).alias("AccountId"),
            upper(trim(col("Pricebook2Id"))).alias("Pricebook2Id"),
            upper(trim(col("OwnerId"))).alias("OwnerId"),
            upper(trim(col("CreatedById"))).alias("CreatedById"),
            upper(trim(col("LastModifiedById"))).alias("LastModifiedById"),
            upper(trim(col("ContactId"))).alias("ContactId"),
            col("IsDeleted"),
            col("IsClosed"),
            col("IsWon"),
            col("HasOpportunityLineItem"),
            initcap(trim(col("Name"))).alias("Name"),
            trim(col("Description")).alias("Description"),
            initcap(trim(col("StageName"))).alias("StageName"),
            initcap(trim(col("Type"))).alias("Type"),
            initcap(trim(col("NextStep"))).alias("NextStep"),
            initcap(trim(col("LeadSource"))).alias("LeadSource"),
            initcap(trim(col("ForecastCategory"))).alias("ForecastCategory"),
            initcap(trim(col("ForecastCategoryName"))).alias("ForecastCategoryName"),
            initcap(trim(col("Fiscal"))).alias("Fiscal"),
            col("Amount"),
            col("Probability"),
            col("PushCount"),
            col("FiscalQuarter"),
            col("FiscalYear"),
            col("CloseDate"),
            col("CreatedDate"),
            col("LastModifiedDate"),
            col("SystemModstamp"),
            col("LastActivityDate"),
            col("LastStageChangeDate"),
            col("LastViewedDate"),
            current_timestamp().alias("silver_processed_at")
        )
    )
