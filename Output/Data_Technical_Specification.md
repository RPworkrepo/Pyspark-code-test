# Technical Specification for BRANCH_SUMMARY_REPORT Enhancement

=============================================
Author: Ascendion AAVA
Date: 
Description: Integration of BRANCH_OPERATIONAL_DETAILS source table into existing BRANCH_SUMMARY_REPORT ETL pipeline
=============================================

## Introduction

This technical specification outlines the required changes to integrate the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the existing ETL pipeline for the `BRANCH_SUMMARY_REPORT`. The enhancement aims to improve compliance and audit readiness by incorporating branch-level operational metadata including region, manager name, audit date, and active status.

### Business Context
- **JIRA Story**: KAN-9 - Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- **Objective**: Enhance the existing BRANCH_SUMMARY_REPORT with operational metadata from the new BRANCH_OPERATIONAL_DETAILS table
- **Impact**: Improved compliance reporting and audit readiness

## Code Changes

### 1. ETL Pipeline Modifications (RegulatoryReportingETL.py)

#### 1.1 New Function Addition
Add a new function to read the BRANCH_OPERATIONAL_DETAILS table:

```python
def read_branch_operational_details(spark: SparkSession, jdbc_url: str, connection_properties: dict) -> DataFrame:
    """
    Reads the BRANCH_OPERATIONAL_DETAILS table from Oracle source.
    
    :param spark: The SparkSession object.
    :param jdbc_url: The JDBC URL for the database connection.
    :param connection_properties: A dictionary of connection properties.
    :return: A Spark DataFrame containing branch operational details.
    """
    logger.info("Reading BRANCH_OPERATIONAL_DETAILS table")
    try:
        df = spark.read.jdbc(url=jdbc_url, table="BRANCH_OPERATIONAL_DETAILS", properties=connection_properties)
        return df
    except Exception as e:
        logger.error(f"Failed to read BRANCH_OPERATIONAL_DETAILS table: {e}")
        raise
```

#### 1.2 Enhanced Branch Summary Report Function
Modify the existing `create_branch_summary_report` function:

```python
def create_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                                branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data at the branch level
    and enriching with operational details.
    
    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: A DataFrame containing the enhanced branch summary report.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame.")
    
    # Base aggregation
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \
                                 .join(branch_df, "BRANCH_ID") \
                                 .groupBy("BRANCH_ID", "BRANCH_NAME") \
                                 .agg(
                                     count("*").alias("TOTAL_TRANSACTIONS"),
                                     sum("AMOUNT").alias("TOTAL_AMOUNT")
                                 )
    
    # Enrich with operational details
    enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left") \
                                  .select(
                                      col("BRANCH_ID"),
                                      col("BRANCH_NAME"),
                                      col("TOTAL_TRANSACTIONS"),
                                      col("TOTAL_AMOUNT"),
                                      col("REGION"),
                                      col("LAST_AUDIT_DATE").cast("string").alias("LAST_AUDIT_DATE")
                                  )
    
    return enhanced_summary
```

#### 1.3 Main Function Updates
Update the main function to include the new data source:

```python
def main():
    # ... existing code ...
    
    # Read source tables (add new table)
    customer_df = read_table(spark, jdbc_url, "CUSTOMER", connection_properties)
    account_df = read_table(spark, jdbc_url, "ACCOUNT", connection_properties)
    transaction_df = read_table(spark, jdbc_url, "TRANSACTION", connection_properties)
    branch_df = read_table(spark, jdbc_url, "BRANCH", connection_properties)
    branch_operational_df = read_branch_operational_details(spark, jdbc_url, connection_properties)  # NEW
    
    # ... existing AML transactions code ...
    
    # Create and write enhanced BRANCH_SUMMARY_REPORT
    branch_summary_df = create_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
    write_to_delta_table(branch_summary_df, "BRANCH_SUMMARY_REPORT")
    
    # ... rest of existing code ...
```

## Data Model Updates

### 2.1 Source Data Model Changes

#### New Source Table: BRANCH_OPERATIONAL_DETAILS
- **Table Name**: BRANCH_OPERATIONAL_DETAILS
- **Source System**: Oracle Database
- **Primary Key**: BRANCH_ID
- **Relationship**: One-to-One with BRANCH table

**Schema Structure:**
```sql
CREATE TABLE BRANCH_OPERATIONAL_DETAILS (
    BRANCH_ID INT PRIMARY KEY,
    REGION VARCHAR2(50),
    MANAGER_NAME VARCHAR2(100),
    LAST_AUDIT_DATE DATE,
    IS_ACTIVE CHAR(1)
);
```

### 2.2 Target Data Model Updates

#### Enhanced BRANCH_SUMMARY_REPORT Table
The target table schema has been updated to include new fields:

**Updated Schema:**
```sql
CREATE TABLE workspace.default.branch_summary_report (
    BRANCH_ID BIGINT,
    BRANCH_NAME STRING,
    TOTAL_TRANSACTIONS BIGINT,
    TOTAL_AMOUNT DOUBLE,
    REGION STRING,           -- NEW FIELD
    LAST_AUDIT_DATE STRING   -- NEW FIELD
)
USING delta
```

### 2.3 Data Lineage Updates

**Enhanced Data Flow:**
```
TRANSACTION → 
             ↘
ACCOUNT    →  → BRANCH_SUMMARY_REPORT (Enhanced)
             ↗                    ↑
BRANCH     → ↗                    ↑
BRANCH_OPERATIONAL_DETAILS → → → ↗
```

## Source-to-Target Mapping

### 3.1 Field Mapping Table

| Source Table | Source Field | Target Table | Target Field | Transformation Rule | Data Type Conversion |
|--------------|--------------|--------------|--------------|--------------------|-----------------------|
| TRANSACTION | COUNT(*) | BRANCH_SUMMARY_REPORT | TOTAL_TRANSACTIONS | Aggregation by BRANCH_ID | INT → BIGINT |
| TRANSACTION | SUM(AMOUNT) | BRANCH_SUMMARY_REPORT | TOTAL_AMOUNT | Aggregation by BRANCH_ID | DECIMAL(15,2) → DOUBLE |
| BRANCH | BRANCH_ID | BRANCH_SUMMARY_REPORT | BRANCH_ID | Direct mapping | INT → BIGINT |
| BRANCH | BRANCH_NAME | BRANCH_SUMMARY_REPORT | BRANCH_NAME | Direct mapping | STRING → STRING |
| BRANCH_OPERATIONAL_DETAILS | REGION | BRANCH_SUMMARY_REPORT | REGION | Direct mapping via LEFT JOIN | VARCHAR2(50) → STRING |
| BRANCH_OPERATIONAL_DETAILS | LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT | LAST_AUDIT_DATE | Cast to STRING | DATE → STRING |

### 3.2 Join Logic

**Primary Joins:**
1. TRANSACTION ⟕ ACCOUNT (ON ACCOUNT_ID)
2. Result ⟕ BRANCH (ON BRANCH_ID)
3. Result ⟕ BRANCH_OPERATIONAL_DETAILS (LEFT JOIN ON BRANCH_ID)

**Join Rationale:**
- LEFT JOIN with BRANCH_OPERATIONAL_DETAILS ensures all branches are included even if operational details are missing
- Maintains data completeness for existing branch summary reports

### 3.3 Transformation Rules

#### 3.3.1 Data Type Conversions
- **LAST_AUDIT_DATE**: Convert Oracle DATE to STRING format for Delta table compatibility
- **REGION**: Convert VARCHAR2(50) to STRING
- **Numeric Fields**: Maintain precision for financial calculations

#### 3.3.2 Business Logic Rules
- **Null Handling**: If BRANCH_OPERATIONAL_DETAILS data is missing, REGION and LAST_AUDIT_DATE will be null
- **Data Quality**: Filter active branches only if IS_ACTIVE = 'Y' (optional enhancement)
- **Aggregation**: Maintain existing aggregation logic for TOTAL_TRANSACTIONS and TOTAL_AMOUNT

## Assumptions and Constraints

### 4.1 Assumptions
- BRANCH_OPERATIONAL_DETAILS table is available in the same Oracle database as other source tables
- BRANCH_ID exists in both BRANCH and BRANCH_OPERATIONAL_DETAILS tables
- Data refresh frequency for BRANCH_OPERATIONAL_DETAILS aligns with existing ETL schedule
- Oracle JDBC driver supports the new table structure

### 4.2 Constraints
- **Performance**: Additional JOIN operation may impact ETL performance
- **Data Availability**: New fields will be null for branches without operational details
- **Backward Compatibility**: Existing downstream consumers must handle new fields
- **Security**: Same access credentials apply to the new source table

### 4.3 Dependencies
- Oracle database connectivity
- Spark cluster resources
- Delta Lake write permissions
- JDBC driver compatibility

## Implementation Considerations

### 5.1 Testing Strategy
- **Unit Tests**: Test new function `read_branch_operational_details`
- **Integration Tests**: Validate enhanced `create_branch_summary_report` function
- **Data Quality Tests**: Verify join accuracy and null handling
- **Performance Tests**: Monitor ETL execution time impact

### 5.2 Deployment Steps
1. Deploy updated ETL code to development environment
2. Execute data validation tests
3. Perform performance benchmarking
4. Deploy to staging environment
5. Conduct user acceptance testing
6. Deploy to production environment

### 5.3 Rollback Plan
- Maintain previous version of ETL code
- Ability to exclude new fields from target table if needed
- Revert to original `create_branch_summary_report` function

## References

- **JIRA Story**: KAN-9 - Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- **Source Code**: RegulatoryReportingETL.py
- **Source DDL**: Source_DDL.txt, branch_operational_details.sql
- **Target DDL**: Target_DDL.txt
- **Documentation**: Confluence context (if available)

---

**Document Version**: 1.0  
**Last Updated**: Generated via Ascendion AAVA  
**Review Status**: Pending Technical Review