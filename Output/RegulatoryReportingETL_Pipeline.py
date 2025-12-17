# ====================================================================
# Author: Ascendion AAVA
# Date: 
# Description: Enhanced PySpark ETL pipeline integrating BRANCH_OPERATIONAL_DETAILS for improved compliance reporting
# ====================================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, lit, when
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "RegulatoryReportingETL") -> SparkSession:
    """
    Initializes and returns a Spark session.
    # [MODIFIED] - Updated to use getActiveSession() for Spark Connect compatibility
    # [MODIFIED] - Removed sparkContext calls for Spark Connect compatibility
    """
    try:
        # [MODIFIED] - Try to get active session first for Spark Connect compatibility
        spark = SparkSession.getActiveSession()
        if spark is None:
            spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .enableHiveSupport() \
                .getOrCreate()
        
        # [MODIFIED] - Conditional sparkContext access for compatibility
        if hasattr(spark, 'sparkContext') and spark.sparkContext:
            spark.sparkContext.setLogLevel("WARN")
        
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

# [ADDED] - New Sample Data Generation Function
def create_sample_data(spark: SparkSession) -> tuple:
    """
    Creates self-contained sample data for all source tables including new BRANCH_OPERATIONAL_DETAILS
    """
    logger.info("Creating sample data for all source tables")
    
    # Customer data
    customer_data = [
        (101, "John Doe", "john.doe@email.com"),
        (102, "Jane Smith", "jane.smith@email.com"),
        (103, "Bob Johnson", "bob.johnson@email.com")
    ]
    customer_schema = StructType([
        StructField("CUSTOMER_ID", LongType(), True),
        StructField("NAME", StringType(), True),
        StructField("EMAIL", StringType(), True)
    ])
    customer_df = spark.createDataFrame(customer_data, customer_schema)
    
    # Account data
    account_data = [
        (1001, 101, 201, "CHECKING", 5000.0),
        (1002, 102, 202, "SAVINGS", 10000.0),
        (1003, 103, 201, "CHECKING", 3000.0)
    ]
    account_schema = StructType([
        StructField("ACCOUNT_ID", LongType(), True),
        StructField("CUSTOMER_ID", LongType(), True),
        StructField("BRANCH_ID", LongType(), True),
        StructField("ACCOUNT_TYPE", StringType(), True),
        StructField("BALANCE", DoubleType(), True)
    ])
    account_df = spark.createDataFrame(account_data, account_schema)
    
    # Transaction data
    transaction_data = [
        (10001, 1001, 500.0, "DEPOSIT", "2024-01-15"),
        (10002, 1001, 200.0, "WITHDRAWAL", "2024-01-16"),
        (10003, 1002, 1000.0, "DEPOSIT", "2024-01-17"),
        (10004, 1003, 300.0, "DEPOSIT", "2024-01-18")
    ]
    transaction_schema = StructType([
        StructField("TRANSACTION_ID", LongType(), True),
        StructField("ACCOUNT_ID", LongType(), True),
        StructField("AMOUNT", DoubleType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("TRANSACTION_DATE", StringType(), True)
    ])
    transaction_df = spark.createDataFrame(transaction_data, transaction_schema)
    
    # Branch data
    branch_data = [
        (201, "Downtown Branch", "123 Main St"),
        (202, "Uptown Branch", "456 Oak Ave")
    ]
    branch_schema = StructType([
        StructField("BRANCH_ID", LongType(), True),
        StructField("BRANCH_NAME", StringType(), True),
        StructField("ADDRESS", StringType(), True)
    ])
    branch_df = spark.createDataFrame(branch_data, branch_schema)
    
    # [ADDED] - BRANCH_OPERATIONAL_DETAILS data
    branch_operational_data = [
        (201, "NORTH", "2024-01-01", "ACTIVE"),
        (202, "SOUTH", "2023-12-15", "ACTIVE")
    ]
    branch_operational_schema = StructType([
        StructField("BRANCH_ID", LongType(), True),
        StructField("REGION", StringType(), True),
        StructField("LAST_AUDIT_DATE", StringType(), True),
        StructField("STATUS", StringType(), True)
    ])
    branch_operational_df = spark.createDataFrame(branch_operational_data, branch_operational_schema)
    
    return customer_df, account_df, transaction_df, branch_df, branch_operational_df

# [DEPRECATED] - Original JDBC Read Function
# def read_table(spark: SparkSession, jdbc_url: str, table_name: str, connection_properties: dict) -> DataFrame:
#     """
#     Reads a table from a JDBC source into a DataFrame.
#     # Preserved for future reference and production deployment
#     """
#     logger.info(f"Reading table: {table_name}")
#     try:
#         df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
#         return df
#     except Exception as e:
#         logger.error(f"Failed to read table {table_name}: {e}")
#         raise

def create_aml_customer_transactions(customer_df: DataFrame, account_df: DataFrame, transaction_df: DataFrame) -> DataFrame:
    """
    Creates the AML_CUSTOMER_TRANSACTIONS DataFrame by joining customer, account, and transaction data.

    :param customer_df: DataFrame with customer data.
    :param account_df: DataFrame with account data.
    :param transaction_df: DataFrame with transaction data.
    :return: A DataFrame ready for the AML customer transactions report.
    """
    logger.info("Creating AML Customer Transactions DataFrame.")
    return customer_df.join(account_df, "CUSTOMER_ID") \
                      .join(transaction_df, "ACCOUNT_ID") \
                      .select(
                          col("CUSTOMER_ID"),
                          col("NAME"),
                          col("ACCOUNT_ID"),
                          col("TRANSACTION_ID"),
                          col("AMOUNT"),
                          col("TRANSACTION_TYPE"),
                          col("TRANSACTION_DATE")
                      )

# [MODIFIED] - Enhanced Branch Summary Report Function
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                               branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level.
    # [MODIFIED] - Added branch_operational_df parameter for new source integration
    # [ADDED] - LEFT JOIN with BRANCH_OPERATIONAL_DETAILS to enrich summary data
    # [ADDED] - New fields: REGION and LAST_AUDIT_DATE in target schema

    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: A DataFrame containing the branch summary report.
    """
    logger.info("Creating Branch Summary Report DataFrame with operational details.")
    
    # Create base aggregation
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                 .join(branch_df, "BRANCH_ID") \
                                 .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                 .agg(
                                     count("*").alias("TOTAL_TRANSACTIONS"),
                                     sum("AMOUNT").alias("TOTAL_AMOUNT")
                                 )
    
    # [ADDED] - LEFT JOIN with BRANCH_OPERATIONAL_DETAILS to enrich summary data
    enhanced_summary = base_summary.join(
        branch_operational_df.select("BRANCH_ID", "REGION", "LAST_AUDIT_DATE"),
        "BRANCH_ID",
        "left"
    ).select(
        col("BRANCH_ID"),
        col("BRANCH_NAME"),
        col("TOTAL_TRANSACTIONS"),
        col("TOTAL_AMOUNT"),
        col("REGION"),  # [ADDED] - New field from operational details
        col("LAST_AUDIT_DATE")  # [ADDED] - New field from operational details
    )
    
    return enhanced_summary

# [ADDED] - Data Quality Validation Function
def validate_data_quality(df: DataFrame, table_name: str) -> bool:
    """
    # [ADDED] - Comprehensive data quality checks for null values and record counts
    """
    logger.info(f"Validating data quality for {table_name}")
    
    try:
        record_count = df.count()
        if record_count == 0:
            logger.warning(f"No records found in {table_name}")
            return False
            
        # Check for null values in key columns
        null_checks = []
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            if null_count > 0:
                null_checks.append(f"{column}: {null_count} nulls")
        
        if null_checks:
            logger.warning(f"Null values found in {table_name}: {', '.join(null_checks)}")
        
        logger.info(f"Data quality validation completed for {table_name}: {record_count} records")
        return True
        
    except Exception as e:
        logger.error(f"Data quality validation failed for {table_name}: {e}")
        return False

def write_to_delta_table(df: DataFrame, table_name: str):
    """
    Writes a DataFrame to a Delta table.

    :param df: The DataFrame to write.
    :param table_name: The name of the target Delta table.
    """
    logger.info(f"Writing DataFrame to Delta table: {table_name}")
    try:
        df.write.format("delta") \
          .mode("overwrite") \
          .option("mergeSchema", "true") \
          .saveAsTable(table_name)
        logger.info(f"Successfully written data to {table_name}")
    except Exception as e:
        logger.error(f"Failed to write to Delta table {table_name}: {e}")
        raise

# [MODIFIED] - Enhanced Main Function
def main():
    """
    Main ETL execution function.
    # [MODIFIED] - Replaced JDBC reads with sample data creation for self-contained execution
    # [ADDED] - Integration of branch_operational_df in processing pipeline
    # [ADDED] - Enhanced error handling and validation workflows
    """
    spark = None
    try:
        spark = get_spark_session()

        # [MODIFIED] - Replaced JDBC reads with sample data creation
        customer_df, account_df, transaction_df, branch_df, branch_operational_df = create_sample_data(spark)
        
        # [ADDED] - Data quality validation
        validate_data_quality(customer_df, "CUSTOMER")
        validate_data_quality(account_df, "ACCOUNT")
        validate_data_quality(transaction_df, "TRANSACTION")
        validate_data_quality(branch_df, "BRANCH")
        validate_data_quality(branch_operational_df, "BRANCH_OPERATIONAL_DETAILS")

        # Create and write AML_CUSTOMER_TRANSACTIONS
        aml_transactions_df = create_aml_customer_transactions(customer_df, account_df, transaction_df)
        validate_data_quality(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")
        write_to_delta_table(aml_transactions_df, "AML_CUSTOMER_TRANSACTIONS")

        # [MODIFIED] - Create and write BRANCH_SUMMARY_REPORT with operational details
        branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
        validate_data_quality(branch_summary_df, "BRANCH_SUMMARY_REPORT")
        write_to_delta_table(branch_summary_df, "BRANCH_SUMMARY_REPORT")

        logger.info("ETL job completed successfully with enhanced operational details integration.")

    except Exception as e:
        logger.error(f"ETL job failed with exception: {e}")
        # In a production environment, you might want to exit with a non-zero status code
        # import sys
        # sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()
