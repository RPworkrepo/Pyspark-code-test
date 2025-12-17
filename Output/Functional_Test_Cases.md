=============================================
Author: Ascendion AAVA
Date: 
Description: Comprehensive functional test cases for BRANCH_SUMMARY_REPORT enhancement with BRANCH_OPERATIONAL_DETAILS integration
=============================================

# Functional Test Cases for BRANCH_SUMMARY_REPORT Enhancement

## Overview
This document contains detailed functional test cases for the integration of BRANCH_OPERATIONAL_DETAILS source table into the existing BRANCH_SUMMARY_REPORT ETL pipeline. The test cases ensure comprehensive coverage of all requirements, edge cases, and data validation scenarios.

## Test Case Categories
1. **Data Integration Test Cases** - Validating successful integration of new source table
2. **Data Transformation Test Cases** - Verifying correct data transformations and mappings
3. **Data Quality Test Cases** - Ensuring data quality and validation rules
4. **Edge Case Test Cases** - Testing boundary conditions and exceptional scenarios
5. **Error Handling Test Cases** - Validating error scenarios and recovery mechanisms

---

## 1. Data Integration Test Cases

### Test Case ID: TC_KAN9_01
**Title:** Validate successful reading of BRANCH_OPERATIONAL_DETAILS table
**Description:** Ensure that the ETL pipeline can successfully read data from the new BRANCH_OPERATIONAL_DETAILS Oracle source table.
**Preconditions:**
- Oracle database is accessible
- BRANCH_OPERATIONAL_DETAILS table exists with sample data
- JDBC connection is properly configured
- ETL pipeline is deployed

**Steps to Execute:**
1. Execute the read_branch_operational_details() function
2. Verify the DataFrame is created successfully
3. Check the DataFrame schema matches expected structure
4. Validate that data is loaded without errors

**Expected Result:**
- DataFrame is created with correct schema (BRANCH_ID, REGION, MANAGER_NAME, LAST_AUDIT_DATE, IS_ACTIVE)
- No connection errors or exceptions
- Data count matches source table record count
- All expected columns are present with correct data types

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_02
**Title:** Validate LEFT JOIN between BRANCH and BRANCH_OPERATIONAL_DETAILS tables
**Description:** Ensure that the LEFT JOIN operation correctly combines branch data with operational details while preserving all branch records.
**Preconditions:**
- Both BRANCH and BRANCH_OPERATIONAL_DETAILS tables contain test data
- Some branches exist without operational details
- ETL pipeline is configured correctly

**Steps to Execute:**
1. Load BRANCH table data
2. Load BRANCH_OPERATIONAL_DETAILS table data
3. Execute the enhanced branch summary report creation
4. Verify JOIN results

**Expected Result:**
- All branches from BRANCH table are included in the result
- Branches with operational details show populated REGION and LAST_AUDIT_DATE
- Branches without operational details show NULL values for REGION and LAST_AUDIT_DATE
- No data loss occurs during JOIN operation

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_03
**Title:** Validate enhanced BRANCH_SUMMARY_REPORT target table creation
**Description:** Ensure that the target Delta table is created with the new schema including REGION and LAST_AUDIT_DATE fields.
**Preconditions:**
- Target workspace is accessible
- Delta table creation permissions are available
- Enhanced ETL pipeline is deployed

**Steps to Execute:**
1. Execute the enhanced ETL pipeline
2. Verify target table creation
3. Check table schema and properties
4. Validate data insertion

**Expected Result:**
- Delta table 'branch_summary_report' is created successfully
- Schema includes all required fields: BRANCH_ID, BRANCH_NAME, TOTAL_TRANSACTIONS, TOTAL_AMOUNT, REGION, LAST_AUDIT_DATE
- Table properties are set correctly (delta features enabled)
- Data is inserted without schema conflicts

**Linked Jira Ticket:** KAN-9

---

## 2. Data Transformation Test Cases

### Test Case ID: TC_KAN9_04
**Title:** Validate DATE to STRING conversion for LAST_AUDIT_DATE field
**Description:** Ensure that LAST_AUDIT_DATE from Oracle DATE format is correctly converted to STRING format in the target table.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS contains records with valid LAST_AUDIT_DATE values
- ETL pipeline includes date conversion logic

**Steps to Execute:**
1. Insert test data with various date formats in source
2. Execute ETL pipeline
3. Query target table for LAST_AUDIT_DATE values
4. Verify date format conversion

**Expected Result:**
- Oracle DATE values are converted to STRING format
- Date format is consistent (e.g., 'YYYY-MM-DD')
- No data loss during conversion
- NULL dates remain NULL in target

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_05
**Title:** Validate BRANCH_ID data type conversion from INT to BIGINT
**Description:** Ensure that BRANCH_ID values are correctly converted from INT to BIGINT during the ETL process.
**Preconditions:**
- Source tables contain BRANCH_ID as INT
- Target table expects BRANCH_ID as BIGINT

**Steps to Execute:**
1. Load source data with various BRANCH_ID values
2. Execute ETL pipeline
3. Verify data type conversion in target table
4. Check for any data truncation or loss

**Expected Result:**
- All BRANCH_ID values are correctly converted to BIGINT
- No data truncation occurs
- JOIN operations work correctly with converted data types
- Target table accepts the converted values

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_06
**Title:** Validate aggregation calculations with new data source
**Description:** Ensure that TOTAL_TRANSACTIONS and TOTAL_AMOUNT calculations remain accurate after integrating the new source table.
**Preconditions:**
- Transaction data exists for multiple branches
- BRANCH_OPERATIONAL_DETAILS is populated
- Baseline aggregation results are known

**Steps to Execute:**
1. Execute enhanced ETL pipeline
2. Compare aggregation results with baseline
3. Verify calculations for branches with and without operational details
4. Check aggregation accuracy

**Expected Result:**
- TOTAL_TRANSACTIONS count remains accurate
- TOTAL_AMOUNT sum calculations are correct
- Aggregations are not affected by the additional JOIN
- Results match expected baseline calculations

**Linked Jira Ticket:** KAN-9

---

## 3. Data Quality Test Cases

### Test Case ID: TC_KAN9_07
**Title:** Validate IS_ACTIVE filter functionality
**Description:** Ensure that only branches with IS_ACTIVE = 'Y' from BRANCH_OPERATIONAL_DETAILS are included in the final report.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS contains records with both 'Y' and 'N' IS_ACTIVE values
- Data validation function is implemented

**Steps to Execute:**
1. Insert test data with IS_ACTIVE = 'Y' and IS_ACTIVE = 'N'
2. Execute ETL pipeline with validation
3. Query target table results
4. Verify filtering logic

**Expected Result:**
- Only branches with IS_ACTIVE = 'Y' contribute operational details
- Branches with IS_ACTIVE = 'N' are excluded from operational details
- Filtering does not affect base branch summary calculations
- Data quality metrics are logged correctly

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_08
**Title:** Validate handling of NULL values in REGION field
**Description:** Ensure that NULL values in the REGION field are handled gracefully without causing pipeline failures.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS contains records with NULL REGION values
- ETL pipeline includes NULL handling logic

**Steps to Execute:**
1. Insert test data with NULL REGION values
2. Execute ETL pipeline
3. Verify NULL handling in target table
4. Check for any pipeline errors

**Expected Result:**
- NULL REGION values are preserved in target table
- No pipeline failures due to NULL values
- JOIN operations handle NULLs correctly
- Target table accepts NULL values in REGION field

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_09
**Title:** Validate referential integrity between BRANCH and BRANCH_OPERATIONAL_DETAILS
**Description:** Ensure that BRANCH_ID values in BRANCH_OPERATIONAL_DETAILS correspond to existing branches.
**Preconditions:**
- BRANCH table contains master branch data
- BRANCH_OPERATIONAL_DETAILS references valid BRANCH_IDs

**Steps to Execute:**
1. Load test data with valid and invalid BRANCH_ID references
2. Execute ETL pipeline
3. Verify referential integrity handling
4. Check for orphaned operational details

**Expected Result:**
- Valid BRANCH_ID references are processed correctly
- Invalid BRANCH_ID references are handled gracefully
- No referential integrity violations cause pipeline failures
- Orphaned operational details are logged but don't break the process

**Linked Jira Ticket:** KAN-9

---

## 4. Edge Case Test Cases

### Test Case ID: TC_KAN9_10
**Title:** Validate processing when BRANCH_OPERATIONAL_DETAILS table is empty
**Description:** Ensure that the ETL pipeline continues to function correctly when the BRANCH_OPERATIONAL_DETAILS table contains no data.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS table exists but is empty
- Other source tables contain valid data

**Steps to Execute:**
1. Ensure BRANCH_OPERATIONAL_DETAILS table is empty
2. Execute ETL pipeline
3. Verify target table creation and data population
4. Check for graceful handling of empty source

**Expected Result:**
- ETL pipeline completes successfully
- Target table is created with base branch summary data
- REGION and LAST_AUDIT_DATE fields contain NULL values
- No errors or exceptions are thrown

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_11
**Title:** Validate processing with maximum field length values
**Description:** Ensure that the pipeline handles maximum length values for VARCHAR fields correctly.
**Preconditions:**
- Test data includes maximum length values for REGION (50 chars) and MANAGER_NAME (100 chars)
- ETL pipeline is configured for field length handling

**Steps to Execute:**
1. Insert test data with maximum field lengths
2. Execute ETL pipeline
3. Verify data truncation or handling
4. Check target table data integrity

**Expected Result:**
- Maximum length values are processed without truncation
- No data corruption occurs
- Target table accepts the full field values
- Field length constraints are respected

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_12
**Title:** Validate processing with duplicate BRANCH_ID in operational details
**Description:** Ensure that duplicate BRANCH_ID entries in BRANCH_OPERATIONAL_DETAILS are handled appropriately.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS contains duplicate BRANCH_ID entries
- ETL pipeline includes duplicate handling logic

**Steps to Execute:**
1. Insert test data with duplicate BRANCH_ID values
2. Execute ETL pipeline
3. Verify duplicate handling strategy
4. Check final target table results

**Expected Result:**
- Duplicate handling strategy is applied consistently (e.g., take latest, first, or error)
- No data corruption in target table
- Duplicate handling is logged appropriately
- Business rules for duplicates are enforced

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_13
**Title:** Validate processing with future dates in LAST_AUDIT_DATE
**Description:** Ensure that future dates in LAST_AUDIT_DATE field are handled according to business rules.
**Preconditions:**
- Test data includes future dates in LAST_AUDIT_DATE
- Business rules for date validation are defined

**Steps to Execute:**
1. Insert test data with future LAST_AUDIT_DATE values
2. Execute ETL pipeline
3. Verify date validation logic
4. Check target table results

**Expected Result:**
- Future dates are handled according to business rules (accept, reject, or flag)
- Data validation warnings are logged if applicable
- Target table reflects the validation decisions
- No pipeline failures due to future dates

**Linked Jira Ticket:** KAN-9

---

## 5. Error Handling Test Cases

### Test Case ID: TC_KAN9_14
**Title:** Validate error handling when BRANCH_OPERATIONAL_DETAILS table is inaccessible
**Description:** Ensure that appropriate error handling occurs when the BRANCH_OPERATIONAL_DETAILS table cannot be accessed.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS table is temporarily inaccessible (permissions, network, etc.)
- Error handling mechanisms are implemented

**Steps to Execute:**
1. Make BRANCH_OPERATIONAL_DETAILS table inaccessible
2. Execute ETL pipeline
3. Verify error handling and logging
4. Check fallback behavior

**Expected Result:**
- Appropriate error messages are logged
- Pipeline fails gracefully with clear error indication
- No partial data corruption occurs
- Error recovery mechanisms are triggered if implemented

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_15
**Title:** Validate error handling for schema mismatch in source table
**Description:** Ensure that schema changes in BRANCH_OPERATIONAL_DETAILS are detected and handled appropriately.
**Preconditions:**
- BRANCH_OPERATIONAL_DETAILS table schema is modified (column added/removed/renamed)
- Schema validation is implemented in ETL pipeline

**Steps to Execute:**
1. Modify BRANCH_OPERATIONAL_DETAILS table schema
2. Execute ETL pipeline
3. Verify schema validation and error handling
4. Check error logging and notifications

**Expected Result:**
- Schema mismatch is detected and reported
- Clear error messages indicate the schema issue
- Pipeline stops execution to prevent data corruption
- Schema validation errors are logged with details

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_16
**Title:** Validate error handling for target table write failures
**Description:** Ensure that write failures to the target Delta table are handled gracefully with appropriate error reporting.
**Preconditions:**
- Target Delta table has write restrictions or storage issues
- Error handling for write operations is implemented

**Steps to Execute:**
1. Simulate target table write failure (permissions, storage full, etc.)
2. Execute ETL pipeline
3. Verify error detection and handling
4. Check transaction rollback behavior

**Expected Result:**
- Write failures are detected immediately
- Appropriate error messages are logged
- Transaction rollback occurs if applicable
- No partial data writes corrupt the target table

**Linked Jira Ticket:** KAN-9

---

## 6. Performance and Integration Test Cases

### Test Case ID: TC_KAN9_17
**Title:** Validate ETL performance with additional JOIN operation
**Description:** Ensure that the additional LEFT JOIN with BRANCH_OPERATIONAL_DETAILS does not significantly degrade ETL performance.
**Preconditions:**
- Baseline performance metrics are available
- Large dataset is available for testing
- Performance monitoring is configured

**Steps to Execute:**
1. Execute ETL pipeline with large dataset
2. Monitor execution time and resource usage
3. Compare with baseline performance metrics
4. Analyze JOIN operation impact

**Expected Result:**
- Performance degradation is within acceptable limits (e.g., <20% increase)
- Memory usage remains within allocated resources
- JOIN operation completes within expected timeframes
- No performance bottlenecks are introduced

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_18
**Title:** Validate end-to-end data flow with all source tables
**Description:** Ensure that the complete ETL pipeline works correctly with all five source tables including the new BRANCH_OPERATIONAL_DETAILS.
**Preconditions:**
- All source tables (CUSTOMER, ACCOUNT, TRANSACTION, BRANCH, BRANCH_OPERATIONAL_DETAILS) contain test data
- Complete ETL pipeline is deployed

**Steps to Execute:**
1. Load comprehensive test data in all source tables
2. Execute complete ETL pipeline
3. Verify data flow through all transformations
4. Validate final target table results

**Expected Result:**
- All source tables are read successfully
- Data transformations and aggregations are correct
- Target table contains expected results with all fields populated
- End-to-end data lineage is maintained

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_19
**Title:** Validate backward compatibility with existing downstream systems
**Description:** Ensure that existing downstream systems can handle the enhanced BRANCH_SUMMARY_REPORT with additional fields.
**Preconditions:**
- Downstream systems are configured to consume BRANCH_SUMMARY_REPORT
- Enhanced target table schema is deployed

**Steps to Execute:**
1. Execute enhanced ETL pipeline
2. Verify downstream system data consumption
3. Check for any compatibility issues
4. Validate that existing functionality is preserved

**Expected Result:**
- Downstream systems continue to function correctly
- Existing fields are accessible and unchanged
- New fields are available for systems that can utilize them
- No breaking changes affect existing integrations

**Linked Jira Ticket:** KAN-9

---

### Test Case ID: TC_KAN9_20
**Title:** Validate data consistency across multiple ETL runs
**Description:** Ensure that multiple executions of the enhanced ETL pipeline produce consistent results.
**Preconditions:**
- Source data remains static between runs
- ETL pipeline supports multiple executions

**Steps to Execute:**
1. Execute ETL pipeline first time
2. Record target table results
3. Execute ETL pipeline second time with same source data
4. Compare results for consistency

**Expected Result:**
- Results are identical across multiple runs
- No data duplication occurs
- Aggregation calculations are consistent
- Delta table handles multiple writes correctly

**Linked Jira Ticket:** KAN-9

---

## Test Execution Summary

### Test Coverage Matrix

| Requirement Category | Test Cases | Coverage |
|---------------------|------------|----------|
| Data Integration | TC_KAN9_01, TC_KAN9_02, TC_KAN9_03 | 100% |
| Data Transformation | TC_KAN9_04, TC_KAN9_05, TC_KAN9_06 | 100% |
| Data Quality | TC_KAN9_07, TC_KAN9_08, TC_KAN9_09 | 100% |
| Edge Cases | TC_KAN9_10, TC_KAN9_11, TC_KAN9_12, TC_KAN9_13 | 100% |
| Error Handling | TC_KAN9_14, TC_KAN9_15, TC_KAN9_16 | 100% |
| Performance & Integration | TC_KAN9_17, TC_KAN9_18, TC_KAN9_19, TC_KAN9_20 | 100% |

### Risk Coverage
- **High Risk Scenarios**: Covered by TC_KAN9_14, TC_KAN9_15, TC_KAN9_16
- **Medium Risk Scenarios**: Covered by TC_KAN9_10, TC_KAN9_12, TC_KAN9_17
- **Low Risk Scenarios**: Covered by remaining test cases

### Acceptance Criteria Validation
✅ **Integration of BRANCH_OPERATIONAL_DETAILS**: Covered by TC_KAN9_01, TC_KAN9_02  
✅ **Enhanced target table schema**: Covered by TC_KAN9_03  
✅ **Data transformation accuracy**: Covered by TC_KAN9_04, TC_KAN9_05, TC_KAN9_06  
✅ **Data quality and validation**: Covered by TC_KAN9_07, TC_KAN9_08, TC_KAN9_09  
✅ **Error handling and resilience**: Covered by TC_KAN9_14, TC_KAN9_15, TC_KAN9_16  
✅ **Performance and compatibility**: Covered by TC_KAN9_17, TC_KAN9_18, TC_KAN9_19  

---

## Cost Estimation and Justification

### Token Usage Analysis
- **Input Tokens**: Approximately 4,800 tokens (including prompt, Jira story, DDL files, and context)
- **Output Tokens**: Approximately 4,200 tokens (comprehensive test cases document)
- **Model Used**: GPT-4 (detected automatically)

### Cost Breakdown
- **Input Cost**: 4,800 tokens × $0.03/1K tokens = $0.144
- **Output Cost**: 4,200 tokens × $0.06/1K tokens = $0.252
- **Total Cost**: $0.144 + $0.252 = $0.396

### Cost Formula
```
Total Cost = (Input Tokens × Input Cost per 1K) + (Output Tokens × Output Cost per 1K)
Total Cost = (4,800 × $0.03/1K) + (4,200 × $0.06/1K) = $0.396
```

---

**Document Version**: 1.0  
**Generated By**: Ascendion AAVA Data Validation Specialist  
**Total Test Cases**: 20  
**Coverage Level**: Comprehensive (100% requirement coverage)  
**Review Status**: Ready for Review