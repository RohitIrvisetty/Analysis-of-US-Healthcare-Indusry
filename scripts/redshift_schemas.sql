# Redshift Schemas

--Redshift Imports
--child rate
CREATE EXTERNAL TABLE s3ext.child_rate (
BusinessYear TEXT,
AVG_Child_Sub DOUBLE PRECISION,
MAX_Child_Sub REAL,
MIN_Child_Sub REAL,
Median_Child_Sub REAL
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/child-rate/';

--converted age data
CREATE EXTERNAL TABLE s3ext.converted_age_data (
BusinessYear TEXT,
StateCode TEXT,
IssuerId TEXT,
SourceName TEXT,
ImportDate TEXT,
FederalTIN TEXT,
RateEffectiveDate TEXT,
RateExpirationDate TEXT,
PlanId TEXT,
RatingAreaId TEXT,
Tobacco TEXT,
Age TEXT,
IndividualRate REAL,
IndividualTobaccoRate REAL,
Couple REAL,
PrimarySubscriberAndOneDependent REAL,
PrimarySubscriberAndTwoDependents REAL,
PrimarySubscriberAndThreeOrMoreDependents REAL,
CoupleAndOneDependent REAL,
CoupleAndTwoDependents REAL,
CoupleAndThreeOrMoreDependents REAL,
AgeNum INTEGER
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/converted-age-data/';

--couple rate
CREATE EXTERNAL TABLE s3ext.couple_rate (
BusinessYear TEXT,
AgeNum INTEGER,
AVG_IN_Sub DOUBLE PRECISION,
MAX_IN_Sub DOUBLE PRECISION,
MIN_IN_Sub DOUBLE PRECISION,
Median_IN_Sub DOUBLE PRECISION,
AVG_Couple DOUBLE PRECISION,
MAX_Couple DOUBLE PRECISION,
MIN_Couple DOUBLE PRECISION,
Median_Couple  DOUBLE PRECISION
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/couple-rate/';

--crosswalk rate
CREATE EXTERNAL TABLE s3ext.crosswalk_rate (
BusinessYear TEXT,
previousYear TEXT,
State TEXT,
Old_PlanID TEXT,
Old_IssuerID TEXT,
Old_MetalLevel TEXT,
New_PlanID TEXT,
New_IssuerID TEXT,
New_MetalLevel TEXT,
rateIncreased_avg DOUBLE PRECISION,
rateIncreased_max DOUBLE PRECISION,
rateIncreased_min DOUBLE PRECISION,
rateIncreased_Median  DOUBLE PRECISION,
issuerChanged BOOLEAN,
metalChanged BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/crosswalk-rate/';

--crosswalk rate updated
CREATE EXTERNAL TABLE s3ext.crosswalk_rate_updated (
BusinessYear TEXT,
Plan_Count BIGINT,
Issuer_Change BIGINT,
Metal_Change BIGINT,
Rate_Increase BIGINT,
Rate_Decrease BIGINT
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/crosswalk-rate-updated/';

--crosswalk stat 1
CREATE EXTERNAL TABLE s3ext.crosswalk_stat1 (
BusinessYear TEXT,
State TEXT,
Plan_Count BIGINT
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/crosswalk-stat1/';

--crosswalk stat 2
CREATE EXTERNAL TABLE s3ext.crosswalk_stat2 (
BusinessYear TEXT,
Total_Plan_Count BIGINT
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/crosswalk-stat2/';

--family_case_1child_rate
CREATE EXTERNAL TABLE s3ext.family_case_one_child_rate (
BusinessYear TEXT,
AVG_IN_Sub DOUBLE PRECISION,
MAX_IN_Sub DOUBLE PRECISION,
MIN_IN_Sub DOUBLE PRECISION,
Median_IN_Sub DOUBLE PRECISION,
AVG_Family1 DOUBLE PRECISION,
MAX_Family1 DOUBLE PRECISION,
MIN_Family1 DOUBLE PRECISION,
Median_Family1 DOUBLE PRECISION
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/family_case_1child-rate/';

--family_case_2child_rate
CREATE EXTERNAL TABLE s3ext.family_case_two_child_rate (
BusinessYear TEXT,
AVG_IN_Sub DOUBLE PRECISION,
MAX_IN_Sub DOUBLE PRECISION,
MIN_IN_Sub DOUBLE PRECISION,
Median_IN_Sub DOUBLE PRECISION,
AVG_Family2 DOUBLE PRECISION,
MAX_Family2 DOUBLE PRECISION,
MIN_Family2 DOUBLE PRECISION,
Median_Family2 DOUBLE PRECISION
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/family_case_2child-rate/';

--family_case_3child_rate
CREATE EXTERNAL TABLE s3ext.family_case_three_child_rate (
BusinessYear TEXT,
AVG_IN_Sub DOUBLE PRECISION,
MAX_IN_Sub DOUBLE PRECISION,
MIN_IN_Sub DOUBLE PRECISION,
Median_IN_Sub DOUBLE PRECISION,
AVG_Family3 DOUBLE PRECISION,
MAX_Family3 DOUBLE PRECISION,
MIN_Family3 DOUBLE PRECISION,
Median_Family3 DOUBLE PRECISION
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/family_case_3child-rate/';

--family rate
CREATE EXTERNAL TABLE s3ext.family_rate(
BusinessYear TEXT,
AVG_Family1 DOUBLE PRECISION,
MAX_Family1 DOUBLE PRECISION,
MIN_Family1 DOUBLE PRECISION,
Median_Family1 DOUBLE PRECISION,
AVG_Family2 DOUBLE PRECISION,
MAX_Family2 DOUBLE PRECISION,
MIN_Family2 DOUBLE PRECISION,
Median_Family2 DOUBLE PRECISION,
AVG_Family3 DOUBLE PRECISION,
MAX_Family3 DOUBLE PRECISION,
MIN_Family3 DOUBLE PRECISION,
Median_Family3 DOUBLE PRECISION
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/family-rate/';

--plan count
CREATE EXTERNAL TABLE s3ext.plan_count(
BusinessYear TEXT,
Individual_Plan_Count BIGINT,
Family_Plan_Count BIGINT,
Total_Plan_Count BIGINT
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/plan-count/';

--plan scope
CREATE EXTERNAL TABLE s3ext.plan_scope(
BusinessYear TEXT,
PlanId TEXT,
State_Count BIGINT,
Area_Count BIGINT
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/plan-scope/';

--rate analysis
CREATE EXTERNAL TABLE s3ext.rate_analysis(
StateCode TEXT,
BusinessYear TEXT,
Plan_Count BIGINT,
AVG_IndividualRate DOUBLE PRECISION,
AVG_IndividualTobaccoRate DOUBLE PRECISION,
AVG_Couple DOUBLE PRECISION,
AVG_PrimarySubscriberAndOneDependent DOUBLE PRECISION,
AVG_PrimarySubscriberAndThreeOrMoreDependents DOUBLE PRECISION,
AVG_CoupleAndOneDependent DOUBLE PRECISION,
AVG_CoupleAndTwoDependents DOUBLE PRECISION,
AVG_CoupleAndThreeOrMoreDependents DOUBLE PRECISION
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/rate-analysis/';

--rate individual year
CREATE EXTERNAL TABLE s3ext.rate_individual_year(
BusinessYear TEXT,
AVG_IndividualRate DOUBLE PRECISION,
AVG_IndividualTobaccoRate DOUBLE PRECISION
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/rate-individual-year/';

--rate individual year
CREATE EXTERNAL TABLE s3ext.rate_individual_year(
BusinessYear TEXT,
AVG_IndividualRate DOUBLE PRECISION,
AVG_IndividualTobaccoRate DOUBLE PRECISION
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/rate-individual-year/';

--rate family year
CREATE EXTERNAL TABLE s3ext.rate_family_year(
BusinessYear TEXT,
AVG_Couple DOUBLE PRECISION,
AVG_PrimarySubscriberAndOneDependent DOUBLE PRECISION,
AVG_PrimarySubscriberAndThreeOrMoreDependents DOUBLE PRECISION,
AVG_CoupleAndOneDependent DOUBLE PRECISION,
AVG_CoupleAndTwoDependents DOUBLE PRECISION,
AVG_CoupleAndThreeOrMoreDependents DOUBLE PRECISION,
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/rate-individual-year/';


CREATE EXTERNAL TABLE s3ext.plan_popularity_name (
PlanVariantMarketingName TEXT,
Popularity BIGINT
)
STORED AS PARQUET
LOCATION 's3://732-rri-a5/s3/plan-attributes-puf/';



CREATE EXTERNAL TABLE s3ext.disease_analysis (
disease TEXT,
value FLOAT,
coverage_percent FLOAT
)
STORED AS PARQUET
LOCATION 's3://732-rri-a5/s3/us_analysis/';


CREATE EXTERNAL TABLE s3ext.bene_top (
BenefitName TEXT,
Benefit_Popularity BIGINT
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/bene_top/';


CREATE EXTERNAL TABLE s3ext.bene_bot (
BenefitName TEXT,
Benefit_Popularity BIGINT
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/bene_bot/';


CREATE EXTERNAL TABLE s3ext.source_popularity_name (
SourceName TEXT,
Popularity BIGINT
)
STORED AS PARQUET
LOCATION 's3://c732-rri-a5/s3/plan-attributes-puf_source/';

