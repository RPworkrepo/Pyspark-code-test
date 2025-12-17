# Technical Specification for BRANCH_SUMMARY_REPORT Enhancement

## Metadata
=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for integrating BRANCH_OPERATIONAL_DETAILS source table into existing BRANCH_SUMMARY_REPORT ETL pipeline
=============================================

## Introduction

This technical specification outlines the required changes to integrate the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the existing ETL pipeline for the `BRANCH_SUMMARY_REPORT`. The enhancement aims to improve compliance and audit readiness by incorporating branch-level operational metadata including region, manager name, audit date, and active status.

### Business Requirements
- **JIRA Story**: KAN-9 - Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
- **Objective**: Integrate BRANCH_OPERATIONAL_DETAILS table to enhance branch reporting with operational metadata
- **Impact**: Improved compliance and audit readiness through enriched branch summary reports

## Code Changes

### 1. Main ETL Script Updates (RegulatoryReportingETL.py)

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
Modify the `create_branch_summary_report` function to include operational details:

```python
def create_enhanced_branch_summary_report(transaction_df: DataFrame, account_df: DataFrame, 
                                         branch_df: DataFrame, branch_operational_df: DataFrame) -> DataFrame:
    """
    Creates the enhanced BRANCH_SUMMARY_REPORT DataFrame by aggregating transaction data 
    and joining with branch operational details.
    
    :param transaction_df: DataFrame with transaction data.
    :param account_df: DataFrame with account data.
    :param branch_df: DataFrame with branch data.
    :param branch_operational_df: DataFrame with branch operational details.
    :return: A DataFrame containing the enhanced branch summary report.
    """
    logger.info("Creating Enhanced Branch Summary Report DataFrame.")
    
    # Create base branch summary
    base_summary = transaction_df.join(account_df, "ACCOUNT_ID") \n                                .join(branch_df, "BRANCH_ID") \n                                .groupBy("BRANCH_ID", "BRANCH_NAME") \n                                .agg(
                                    count("*").alias("TOTAL_TRANSACTIONS"),
                                    sum("AMOUNT").alias("TOTAL_AMOUNT")
                                )
    
    # Join with operational details
    enhanced_summary = base_summary.join(branch_operational_df, "BRANCH_ID", "left") \n                                  .select(
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
Modify the main() function to incorporate the new data source:

```python
# Add after existing table reads
branch_operational_df = read_branch_operational_details(spark, jdbc_url, connection_properties)

# Replace the existing branch summary creation
branch_summary_df = create_enhanced_branch_summary_report(transaction_df, account_df, branch_df, branch_operational_df)
write_to_delta_table(branch_summary_df, "BRANCH_SUMMARY_REPORT")
```

### 2. Data Validation and Error Handling

#### 2.1 Add Data Quality Checks
```python
def validate_branch_operational_data(df: DataFrame) -> DataFrame:
    """
    Validates the BRANCH_OPERATIONAL_DETAILS data quality.
    """
    logger.info("Validating BRANCH_OPERATIONAL_DETAILS data quality")
    
    # Filter active branches only
    validated_df = df.filter(col("IS_ACTIVE") == "Y")
    
    # Log validation metrics
    total_records = df.count()
    active_records = validated_df.count()
    logger.info(f"Total branch operational records: {total_records}")
    logger.info(f"Active branch records: {active_records}")
    
    return validated_df
```

## Data Model Updates

### 1. Source Data Model Changes

#### New Source Table: BRANCH_OPERATIONAL_DETAILS
- **Table Type**: Oracle Source Table
- **Purpose**: Store branch-level operational metadata
- **Key Fields**:
  - `BRANCH_ID` (INT) - Primary Key, Foreign Key to BRANCH table
  - `REGION` (VARCHAR2(50)) - Branch region information
  - `MANAGER_NAME` (VARCHAR2(100)) - Branch manager name
  - `LAST_AUDIT_DATE` (DATE) - Last audit date for compliance tracking
  - `IS_ACTIVE` (CHAR(1)) - Active status flag ('Y'/'N')

### 2. Target Data Model Updates

#### Enhanced BRANCH_SUMMARY_REPORT Table
The target table schema has been updated to include:
- **New Fields Added**:
  - `REGION` (STRING) - Branch region from operational details
  - `LAST_AUDIT_DATE` (STRING) - Last audit date for compliance reporting

#### Updated Target Schema
```sql
CREATE TABLE workspace.default.branch_summary_report (
    BRANCH_ID BIGINT,
    BRANCH_NAME STRING,
    TOTAL_TRANSACTIONS BIGINT,
    TOTAL_AMOUNT DOUBLE,
    REGION STRING,              -- New field
    LAST_AUDIT_DATE STRING      -- New field
)
USING delta
```

### 3. Data Lineage Updates

#### Enhanced Data Flow
```
Source Tables:
├── CUSTOMER
├── ACCOUNT
├── TRANSACTION
├── BRANCH
└── BRANCH_OPERATIONAL_DETAILS (New)

Target Tables:
├── AML_CUSTOMER_TRANSACTIONS (Unchanged)
└── BRANCH_SUMMARY_REPORT (Enhanced)
```

## Source-to-Target Mapping

### 1. BRANCH_OPERATIONAL_DETAILS to BRANCH_SUMMARY_REPORT Mapping

| Source Table | Source Field | Target Table | Target Field | Transformation Rule | Data Type | Nullable |
|--------------|--------------|--------------|--------------|-------------------|-----------|----------|
| BRANCH_OPERATIONAL_DETAILS | BRANCH_ID | BRANCH_SUMMARY_REPORT | BRANCH_ID | Direct mapping via JOIN | INT → BIGINT | No |
| BRANCH_OPERATIONAL_DETAILS | REGION | BRANCH_SUMMARY_REPORT | REGION | Direct mapping | VARCHAR2(50) → STRING | Yes |
| BRANCH_OPERATIONAL_DETAILS | LAST_AUDIT_DATE | BRANCH_SUMMARY_REPORT | LAST_AUDIT_DATE | Cast to STRING format | DATE → STRING | Yes |
| BRANCH_OPERATIONAL_DETAILS | IS_ACTIVE | N/A | N/A | Filter condition (='Y') | Used for filtering only | N/A |
| BRANCH_OPERATIONAL_DETAILS | MANAGER_NAME | N/A | N/A | Not mapped to target | Not included in target | N/A |

### 2. Existing Field Mappings (Unchanged)

| Source Tables | Source Field | Target Table | Target Field | Transformation Rule | Data Type |
|---------------|--------------|--------------|--------------|-------------------|----------|
| BRANCH | BRANCH_ID | BRANCH_SUMMARY_REPORT | BRANCH_ID | Direct mapping | INT → BIGINT |
| BRANCH | BRANCH_NAME | BRANCH_SUMMARY_REPORT | BRANCH_NAME | Direct mapping | STRING → STRING |
| TRANSACTION | * | BRANCH_SUMMARY_REPORT | TOTAL_TRANSACTIONS | COUNT aggregation | * → BIGINT |
| TRANSACTION | AMOUNT | BRANCH_SUMMARY_REPORT | TOTAL_AMOUNT | SUM aggregation | DECIMAL(15,2) → DOUBLE |

### 3. Join Strategy

#### Primary Join Path
```sql
TRANSACTION 
  JOIN ACCOUNT ON TRANSACTION.ACCOUNT_ID = ACCOUNT.ACCOUNT_ID
  JOIN BRANCH ON ACCOUNT.BRANCH_ID = BRANCH.BRANCH_ID
  LEFT JOIN BRANCH_OPERATIONAL_DETAILS ON BRANCH.BRANCH_ID = BRANCH_OPERATIONAL_DETAILS.BRANCH_ID
```

#### Join Type Rationale
- **LEFT JOIN** with BRANCH_OPERATIONAL_DETAILS to ensure all branches are included even if operational details are missing
- This prevents data loss for branches that may not have operational details recorded yet

### 4. Transformation Rules

#### 4.1 Data Type Conversions
- `LAST_AUDIT_DATE`: Convert from Oracle DATE to STRING format for Delta table compatibility
- `BRANCH_ID`: Implicit conversion from INT to BIGINT

#### 4.2 Data Quality Rules
- Filter `BRANCH_OPERATIONAL_DETAILS` where `IS_ACTIVE = 'Y'`
- Handle NULL values in `REGION` and `LAST_AUDIT_DATE` gracefully
- Maintain referential integrity through proper join conditions

#### 4.3 Business Logic Rules
- Only include active branches in operational details
- Preserve all transaction aggregations even for branches without operational details
- Format audit dates consistently as strings

## Assumptions and Constraints

### Assumptions
1. **Data Availability**: BRANCH_OPERATIONAL_DETAILS table is available in the Oracle source system
2. **Data Quality**: BRANCH_ID in BRANCH_OPERATIONAL_DETAILS corresponds to existing BRANCH_ID values
3. **Performance**: The new join operation will not significantly impact ETL performance
4. **Schema Stability**: Target table schema can be modified to accommodate new fields
5. **Backward Compatibility**: Existing downstream consumers can handle additional fields

### Constraints
1. **Data Freshness**: BRANCH_OPERATIONAL_DETAILS data freshness depends on source system updates
2. **Join Performance**: LEFT JOIN may impact performance for large datasets
3. **Schema Evolution**: Target table schema changes require coordination with downstream systems
4. **Error Handling**: Missing operational details should not fail the entire ETL process
5. **Data Governance**: New fields must comply with existing data governance policies

### Technical Constraints
1. **Oracle JDBC Driver**: Ensure Oracle JDBC driver supports the source table structure
2. **Delta Table**: Target Delta table must support schema evolution
3. **Spark Resources**: Additional memory may be required for the enhanced join operations
4. **Data Types**: Oracle DATE to Spark STRING conversion must be handled properly

### Business Constraints
1. **Compliance Requirements**: New fields must meet regulatory reporting standards
2. **Audit Trail**: Changes must be logged for audit purposes
3. **Data Retention**: Operational details must follow the same retention policies as other branch data
4. **Access Control**: New fields must follow existing security and access control policies

## Implementation Plan

### Phase 1: Development
1. Update ETL code with new functions and enhanced logic
2. Implement data validation and error handling
3. Update target table schema
4. Create unit tests for new functionality

### Phase 2: Testing
1. Validate source-to-target mapping
2. Test data quality and transformation rules
3. Performance testing with enhanced joins
4. End-to-end integration testing

### Phase 3: Deployment
1. Deploy code changes to development environment
2. Execute data migration and validation
3. Deploy to production with monitoring
4. Validate downstream system compatibility

## References

1. **JIRA Story**: KAN-9 - Extend BRANCH_SUMMARY_REPORT Logic to Integrate New Source Table
2. **Source Files**:
   - `Input/RegulatoryReportingETL.py` - Existing ETL implementation
   - `Input/Source_DDL.txt` - Source table definitions
   - `Input/Target_DDL.txt` - Target table schema
   - `Input/branch_operational_details.sql` - New source table DDL
3. **Technical Standards**:
   - Delta Lake documentation for schema evolution
   - PySpark SQL functions reference
   - Oracle JDBC driver documentation
4. **Data Governance**:
   - Company data governance policies
   - Regulatory reporting requirements
   - Data quality standards

---

**Document Version**: 1.0  
**Last Updated**: Generated by Ascendion AAVA  
**Review Status**: Pending Review