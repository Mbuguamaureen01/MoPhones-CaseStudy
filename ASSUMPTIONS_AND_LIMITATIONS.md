## Assumptions and Data Limitations

This document outlines the key assumptions and data limitations encountered during the MoPhones credit portfolio analysis.

---

## Question 4: Assumptions Required Due to Point-in-Time Data

### Snapshot-Based Data Assumptions

1. **Payment Behavior Between Snapshots**
   - **Assumption**: Payments occur relatively evenly between snapshot dates
   - **Reality**: Cannot track daily/weekly payment patterns
   - **Impact**: Trends are directional, not precise

2. **Status Change Timing**
   - **Assumption**: Status changes happened sometime within the quarter
   - **Reality**: Cannot pinpoint when accounts moved from Active → FPD → Default
   - **Impact**: Cannot analyze trigger events or speed of deterioration

3. **Account Age Interpretation**
   - **Assumption**: "CUSTOMER_AGE" field represents account age in days (not customer's age)
   - **Rationale**: Values like 102, 211, 22 align with days since sale_date
   - **Impact**: Using this for tenure analysis, not for customer demographic age

4. **NPS Survey Timing**
   - **Assumption**: NPS responses reflect customer experience around survey date
   - **Reality**: Survey submission may lag the experience by days/weeks
   - **Impact**: Credit outcomes and NPS may not be perfectly synchronized

5. **Cohort Age Calculation**
   - **Assumption**: Quarterly cohorts (grouping by sale_date quarter)
   - **Rationale**: Aligns with snapshot frequency
   - **Impact**: Cannot do monthly or weekly cohort analysis

### Data Completeness Assumptions

6. **NPS Response Representativeness**
   - **Assumption**: 4,129 NPS responses represent the broader customer base
   - **Reality**: Only ~20% response rate, potential self-selection bias
   - **Impact**: Satisfaction analysis may not reflect non-respondents

---

## Question 5: Key Data Limitations

#### 1. Transaction-Level Data Missing

**What's Missing:**
- Payment transaction history (dates, amounts, methods)
- Status change event logs with timestamps
- Collection activity records
- Phone lock/unlock events

**Impact:**
- Cannot track payment frequency or regularity
- Cannot identify payment patterns (e.g., pays only when contacted)
- Cannot measure collection effectiveness
- Cannot correlate operational actions with outcomes

#### 2. Incomplete NPS Coverage

**Issue:**
- Only 4,129 NPS responses
- 20,742 total loans in latest snapshot
- ~20% response rate
- 144 responses have null NPS scores

**Impact:**
- NPS analysis only represents responding customers
- Potential self-selection bias (satisfied or very unsatisfied more likely to respond)
- Cannot analyze NPS trends over time (no survey date distribution)
- Limited statistical power for segment analysis

---

## How These Limitations Affect Analysis Confidence

### High Confidence

These analyses are reliable given available data:
- Portfolio size and growth trends
- Account status distributions over time
- Days Past Due (DPD) distribution and trends
- Product mix and loan term analysis
- Arrears rate and FPD rate calculations

### Medium Confidence

These analyses are directional but limited:
- Credit-NPS correlation (limited by small NPS sample)
- Payment performance trends (snapshot-based, not transaction-level)
- Cohort analysis (quarterly resolution only)
- Operational issue impact (based on self-reported NPS data)

### Low/No Confidence 

These analyses cannot be performed as requested:
- Customer demographic risk profiling
- Payment timing and frequency patterns (no transaction data)
- Exact collections effectiveness (no action tracking)

---

## Implications for Business Decisions

### What We CAN Conclude:

1. Portfolio is growing but with increasing credit risk (arrears/FPD trends)
2. Operational issues (phone locks, payment delays) significantly impact NPS
3. Certain products/loan terms perform differently
4. DPD is increasing over time, suggesting portfolio aging
5. Strong correlation between operational quality and satisfaction

### What We CANNOT Conclude:

1. Age demographics of customers (some customer data not available)
2. Income appropriateness of loans (some customers data not available)
3. Exact payment behavior (only snapshot states)
4. Collection strategy effectiveness (no activity tracking)
5. Customer lifetime value (incomplete customer data)

---

## Recommendations to Address Limitations

1. Add Customer Demographics to be mandatory
   - Collect Date of Birth at onboarding
   - Capture monthly income or income range
   - Store employment information
   - Add location/region data

2. Implement Transaction Tracking
   - Log every payment with timestamp and amount
   - Track status changes as events
   - Record collection actions
   - Timestamp all operational events (locks, unlocks)


3. Improve NPS Process
   - Increase survey response rate (incentivize?)
   - Add survey date/time to responses
   - Link to recent credit events
   - Make key questions mandatory

4. Data Quality Framework
   - Automated validation on data entry
   - Completeness monitoring
   - Consistency checks across systems
   - Regular data quality reports

5. Build Data Warehouse
   - Centralize all data sources
   - Event-based architecture for tracking changes
   - Historical snapshots + transaction logs
   - Support for real-time analytics

6. Predictive Analytics
   - Early warning models for default (requires transaction data)
   - NPS prediction based on behavior
   - Customer lifetime value models (requires complete data)

---

## Conclusion

This analysis provides valuable directional insights into portfolio health, credit risk trends, and the relationship between operations and customer satisfaction. However,critical fields required by the case study (DOB and income full data - some are missing) are missing, preventing the requested demographic segmentation analysis.

To fully answer the case study questions and make data-driven business decisions, MoPhones must:
1. Capture customer demographic data (age, income)- Mandatory for every customer
2. Implement transaction-level tracking
3. Fix data export truncation issues
4. Improve NPS survey coverage

The dbt models and analytical framework built in this project are production-ready and repeatable, but the insights are fundamentally limited by the available data.

----
