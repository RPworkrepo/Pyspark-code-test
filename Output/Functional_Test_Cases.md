=============================================
Author: Ascendion AAVA
Date: 
Description: Functional test cases for integrating BRANCH_OPERATIONAL_DETAILS into BRANCH_SUMMARY_REPORT ETL pipeline
=============================================

# Functional Test Cases for BRANCH_SUMMARY_REPORT Enhancement

## Overview
This document contains comprehensive functional test cases for the integration of the new Oracle source table `BRANCH_OPERATIONAL_DETAILS` into the existing ETL pipeline for the `BRANCH_SUMMARY_REPORT`. These test cases ensure that the enhanced functionality meets all requirements and handles various scenarios including edge cases.

---

## Test Cases

### Test Case ID: TC_KAN9_01
**Title:** Validate successful reading of BRANCH_OPERATIONAL_DETAILS table  
**Description:** Ensure that the ETL pipeline can successfully read data from the new BRANCH_OPERATIONAL_DETAILS Oracle source table.  
**Preconditions:**  
- Oracle database is accessible
- BRANCH_OPERATIONAL_DETAILS table exists with valid data
- JDBC connection is properly configured
- Spark session is active

**Steps to Execute:**  
1. Initialize Spark session with Oracle JDBC driver
2. Configure JDBC connection properties for Oracle database
3. Execute read_branch_operational_details() function
4. Verify DataFrame is created successfully
5. Check DataFrame schema matches expected structure

**Expected Result:**  
- DataFrame is created without errors
- Schema contains: BRANCH_ID (INT), REGION (VARCHAR2), MANAGER_NAME (VARCHAR2), LAST_AUDIT_DATE (DATE), IS_ACTIVE (CHAR)
- Data is loaded successfully from Oracle source

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_02
**Title:** Validate enhanced BRANCH_SUMMARY_REPORT with operational details  
**Description:** Ensure that the enhanced create_branch_summary_report function successfully integrates operational details with existing branch summary data.  
**Preconditions:**  
- All source tables (TRANSACTION, ACCOUNT, BRANCH, BRANCH_OPERATIONAL_DETAILS) are available
- Sample data exists in all source tables
- All tables have matching BRANCH_ID values

**Steps to Execute:**  
1. Load sample data into TRANSACTION, ACCOUNT, BRANCH tables
2. Load sample data into BRANCH_OPERATIONAL_DETAILS table
3. Execute enhanced create_branch_summary_report() function
4. Verify the output DataFrame contains all expected columns
5. Validate data aggregation is correct
6. Check that operational details are properly joined

**Expected Result:**  
- Output DataFrame contains: BRANCH_ID, BRANCH_NAME, TOTAL_TRANSACTIONS, TOTAL_AMOUNT, REGION, LAST_AUDIT_DATE
- Transaction counts and amounts are correctly aggregated by branch
- REGION and LAST_AUDIT_DATE fields are populated from BRANCH_OPERATIONAL_DETAILS
- All branches from the base summary are preserved

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_03
**Title:** Validate LEFT JOIN behavior with missing operational details  
**Description:** Ensure that branches without corresponding operational details are still included in the report with NULL values for new fields.  
**Preconditions:**  
- BRANCH table contains branches with IDs: 1, 2, 3
- BRANCH_OPERATIONAL_DETAILS table contains data only for BRANCH_ID: 1, 2
- TRANSACTION and ACCOUNT tables have data for all three branches

**Steps to Execute:**  
1. Set up test data with missing operational details for BRANCH_ID = 3
2. Execute create_branch_summary_report() function
3. Verify all three branches appear in the output
4. Check that BRANCH_ID = 3 has NULL values for REGION and LAST_AUDIT_DATE
5. Validate that BRANCH_ID = 1 and 2 have populated operational details

**Expected Result:**  
- All three branches (1, 2, 3) appear in the output DataFrame
- BRANCH_ID = 3 has NULL values for REGION and LAST_AUDIT_DATE
- BRANCH_ID = 1 and 2 have non-null values for REGION and LAST_AUDIT_DATE
- TOTAL_TRANSACTIONS and TOTAL_AMOUNT are calculated correctly for all branches

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_04
**Title:** Validate data type conversion for LAST_AUDIT_DATE  
**Description:** Ensure that LAST_AUDIT_DATE is correctly converted from Oracle DATE to STRING format for Delta table compatibility.  
**Preconditions:**  
- BRANCH_OPERATIONAL_DETAILS table contains DATE values in LAST_AUDIT_DATE column
- Sample data includes various date formats

**Steps to Execute:**  
1. Insert test data with LAST_AUDIT_DATE values: '2024-01-15', '2024-12-31', '2023-06-30'
2. Execute create_branch_summary_report() function
3. Verify LAST_AUDIT_DATE column in output is STRING type
4. Check that date values are properly formatted
5. Validate no data loss during conversion

**Expected Result:**  
- LAST_AUDIT_DATE column in output DataFrame is of STRING type
- Date values are preserved and properly formatted
- No conversion errors occur
- All date values are readable and consistent

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_05
**Title:** Validate aggregation accuracy with new join logic  
**Description:** Ensure that transaction aggregation (count and sum) remains accurate after adding the new join with BRANCH_OPERATIONAL_DETAILS.  
**Preconditions:**  
- Known test dataset with specific transaction counts and amounts per branch
- BRANCH_OPERATIONAL_DETAILS table with corresponding branch data

**Steps to Execute:**  
1. Load test data: Branch 1 (5 transactions, $1000 total), Branch 2 (3 transactions, $750 total)
2. Execute enhanced create_branch_summary_report() function
3. Compare TOTAL_TRANSACTIONS and TOTAL_AMOUNT with expected values
4. Verify aggregation logic is not affected by the additional join
5. Cross-validate with manual calculation

**Expected Result:**  
- TOTAL_TRANSACTIONS for Branch 1 = 5, Branch 2 = 3
- TOTAL_AMOUNT for Branch 1 = $1000.00, Branch 2 = $750.00
- Aggregation results match manual calculations
- No duplicate counting due to additional join

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_06
**Title:** Validate handling of NULL values in REGION field  
**Description:** Ensure that NULL values in the REGION field from BRANCH_OPERATIONAL_DETAILS are properly handled.  
**Preconditions:**  
- BRANCH_OPERATIONAL_DETAILS table contains some records with NULL REGION values
- Other fields in the same records have valid data

**Steps to Execute:**  
1. Insert test data with REGION = NULL for specific branches
2. Execute create_branch_summary_report() function
3. Verify NULL REGION values are preserved in output
4. Check that other fields for the same branches are populated correctly
5. Validate no errors occur due to NULL handling

**Expected Result:**  
- Records with NULL REGION values appear in output with REGION = NULL
- Other fields (LAST_AUDIT_DATE, BRANCH_NAME, etc.) are populated correctly
- No errors or exceptions occur during processing
- Data integrity is maintained

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_07
**Title:** Validate enhanced target table schema compatibility  
**Description:** Ensure that the enhanced BRANCH_SUMMARY_REPORT data can be successfully written to the Delta table with the new schema.  
**Preconditions:**  
- Target Delta table exists with enhanced schema (6 columns)
- Enhanced DataFrame is generated successfully
- Write permissions are available

**Steps to Execute:**  
1. Generate enhanced BRANCH_SUMMARY_REPORT DataFrame
2. Execute write_to_delta_table() function for BRANCH_SUMMARY_REPORT
3. Verify data is written successfully to Delta table
4. Query the Delta table to confirm all 6 columns are populated
5. Validate data types match target schema

**Expected Result:**  
- Data is successfully written to Delta table without errors
- All 6 columns (BRANCH_ID, BRANCH_NAME, TOTAL_TRANSACTIONS, TOTAL_AMOUNT, REGION, LAST_AUDIT_DATE) are populated
- Data types match target schema: BIGINT, STRING, BIGINT, DOUBLE, STRING, STRING
- Delta table properties are preserved

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_08
**Title:** Validate error handling for missing BRANCH_OPERATIONAL_DETAILS table  
**Description:** Ensure appropriate error handling when BRANCH_OPERATIONAL_DETAILS table is not accessible.  
**Preconditions:**  
- BRANCH_OPERATIONAL_DETAILS table is not available or accessible
- Other source tables are available
- ETL pipeline is configured to read from the table

**Steps to Execute:**  
1. Make BRANCH_OPERATIONAL_DETAILS table inaccessible (rename or drop)
2. Execute read_branch_operational_details() function
3. Verify appropriate error is raised
4. Check that error message is descriptive and logged
5. Ensure ETL pipeline fails gracefully

**Expected Result:**  
- Function raises appropriate exception (e.g., table not found)
- Error message clearly indicates the missing table
- Error is properly logged with sufficient detail
- ETL pipeline stops execution and does not proceed with incomplete data

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_09
**Title:** Validate performance impact of additional join operation  
**Description:** Ensure that the additional LEFT JOIN with BRANCH_OPERATIONAL_DETAILS does not significantly impact ETL performance.  
**Preconditions:**  
- Large dataset available for performance testing
- Baseline performance metrics from original ETL pipeline
- Performance monitoring tools are available

**Steps to Execute:**  
1. Execute original create_branch_summary_report() function and record execution time
2. Execute enhanced create_branch_summary_report() function with same dataset
3. Compare execution times and resource utilization
4. Verify performance degradation is within acceptable limits (< 20%)
5. Monitor memory and CPU usage during execution

**Expected Result:**  
- Performance impact is minimal (< 20% increase in execution time)
- Memory usage remains within acceptable limits
- No significant CPU spikes or resource contention
- ETL completes successfully within SLA timeframes

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_10
**Title:** Validate data consistency across multiple ETL runs  
**Description:** Ensure that multiple executions of the enhanced ETL pipeline produce consistent results.  
**Preconditions:**  
- Static test dataset is available
- Enhanced ETL pipeline is deployed
- Target Delta table is accessible

**Steps to Execute:**  
1. Execute enhanced ETL pipeline with static test dataset (Run 1)
2. Record output results from BRANCH_SUMMARY_REPORT
3. Execute the same ETL pipeline again with identical dataset (Run 2)
4. Compare results from both runs
5. Verify data consistency and deterministic behavior

**Expected Result:**  
- Results from Run 1 and Run 2 are identical
- All field values match exactly between runs
- No random variations in aggregation or join results
- Delta table maintains consistent state

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_11
**Title:** Validate handling of special characters in REGION and MANAGER_NAME fields  
**Description:** Ensure that special characters, Unicode, and various text formats in operational details are properly handled.  
**Preconditions:**  
- BRANCH_OPERATIONAL_DETAILS table contains records with special characters
- Test data includes Unicode characters, apostrophes, hyphens, and spaces

**Steps to Execute:**  
1. Insert test data with REGION values: "North-East", "São Paulo", "O'Connor Region"
2. Insert MANAGER_NAME values: "José María", "O'Brien, John", "Smith-Johnson"
3. Execute create_branch_summary_report() function
4. Verify special characters are preserved in output
5. Check that no encoding issues occur

**Expected Result:**  
- All special characters are preserved correctly in output
- Unicode characters display properly
- No encoding errors or character corruption
- String fields maintain original formatting

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_12
**Title:** Validate backward compatibility with existing downstream systems  
**Description:** Ensure that the enhanced BRANCH_SUMMARY_REPORT maintains compatibility with existing downstream consumers.  
**Preconditions:**  
- Enhanced BRANCH_SUMMARY_REPORT is generated successfully
- Existing downstream systems are available for testing
- Original 4-column structure is preserved in first 4 columns

**Steps to Execute:**  
1. Generate enhanced BRANCH_SUMMARY_REPORT with 6 columns
2. Verify first 4 columns (BRANCH_ID, BRANCH_NAME, TOTAL_TRANSACTIONS, TOTAL_AMOUNT) maintain original structure
3. Test downstream system access to the enhanced table
4. Verify existing queries continue to work
5. Check that new columns can be optionally accessed

**Expected Result:**  
- First 4 columns maintain exact same structure and data types as before
- Existing downstream queries execute without modification
- New columns (REGION, LAST_AUDIT_DATE) are accessible when needed
- No breaking changes for existing consumers

**Linked Jira Ticket:** KAN-9

---

## Edge Cases and Boundary Conditions

### Test Case ID: TC_KAN9_13
**Title:** Validate handling of empty BRANCH_OPERATIONAL_DETAILS table  
**Description:** Ensure proper handling when BRANCH_OPERATIONAL_DETAILS table exists but contains no data.  
**Preconditions:**  
- BRANCH_OPERATIONAL_DETAILS table exists with correct schema
- Table contains zero records
- Other source tables contain valid data

**Steps to Execute:**  
1. Ensure BRANCH_OPERATIONAL_DETAILS table is empty
2. Execute create_branch_summary_report() function
3. Verify LEFT JOIN behavior with empty table
4. Check that all branches appear with NULL operational details
5. Validate aggregation still works correctly

**Expected Result:**  
- All branches appear in output with NULL values for REGION and LAST_AUDIT_DATE
- TOTAL_TRANSACTIONS and TOTAL_AMOUNT are calculated correctly
- No errors occur due to empty operational details table
- ETL completes successfully

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_14
**Title:** Validate handling of duplicate BRANCH_ID in BRANCH_OPERATIONAL_DETAILS  
**Description:** Ensure proper error handling or data resolution when BRANCH_OPERATIONAL_DETAILS contains duplicate BRANCH_ID values.  
**Preconditions:**  
- BRANCH_OPERATIONAL_DETAILS table contains duplicate BRANCH_ID entries
- Duplicate entries have different REGION or LAST_AUDIT_DATE values

**Steps to Execute:**  
1. Insert duplicate BRANCH_ID records with different operational details
2. Execute create_branch_summary_report() function
3. Observe behavior with duplicate join keys
4. Verify data integrity in output
5. Check for any data multiplication or incorrect aggregation

**Expected Result:**  
- System handles duplicates according to defined business rules
- Either error is raised for data quality issue, or deterministic selection occurs
- No incorrect aggregation due to duplicate joins
- Data integrity is maintained

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_15
**Title:** Validate handling of extremely long text values in operational fields  
**Description:** Ensure that very long text values in REGION and MANAGER_NAME fields are properly handled within system limits.  
**Preconditions:**  
- Test data with REGION and MANAGER_NAME values approaching field length limits
- VARCHAR2(50) for REGION, VARCHAR2(100) for MANAGER_NAME

**Steps to Execute:**  
1. Insert test data with REGION = 50-character string, MANAGER_NAME = 100-character string
2. Execute create_branch_summary_report() function
3. Verify long text values are preserved in output
4. Check for any truncation or overflow issues
5. Validate string conversion to Delta table STRING type

**Expected Result:**  
- Long text values are preserved without truncation
- No overflow errors or data corruption
- Successful conversion from VARCHAR2 to STRING type
- All characters are readable and properly formatted

**Linked Jira Ticket:** KAN-9

---

## Cost Estimation and Justification

### Token Usage Analysis
- **Input Tokens**: Approximately 4,200 tokens (including prompt, JIRA story content, source files, DDL schemas, and context)
- **Output Tokens**: Approximately 4,800 tokens (comprehensive functional test cases document)
- **Model Used**: GPT-4 (detected automatically)

### Cost Calculation
Based on current GPT-4 pricing:
- **Input Cost**: 4,200 tokens × $0.03/1K tokens = $0.126
- **Output Cost**: 4,800 tokens × $0.06/1K tokens = $0.288
- **Total Cost**: $0.126 + $0.288 = **$0.414**

### Cost Breakdown Formula
```
Total Cost = (Input Tokens × Input Rate) + (Output Tokens × Output Rate)
Total Cost = (4,200 × $0.00003) + (4,800 × $0.00006) = $0.414
```

This cost represents the computational expense for generating comprehensive functional test cases that cover all requirements, edge cases, and boundary conditions for integrating the BRANCH_OPERATIONAL_DETAILS table into the existing ETL pipeline, ensuring thorough validation of the enhanced BRANCH_SUMMARY_REPORT functionality.