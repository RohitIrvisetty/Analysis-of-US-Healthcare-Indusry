##################################
#Author: Karthik Srinatha (ksa166)
#Modified by: Rovenna Chu (ytc17)
##################################

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

import os
import pandas as pd
from pyspark.sql.functions import avg,asc
from pyspark.sql import SparkSession, functions, types
from schemas import benefits_schema, rates_schema, new_rates_schema, plan_schema, crosswalk_schema,new_crosswalk_schema

#Removing unwanted coloums
benefits_dropping_columns = ['IsEHB', 
                             'IsCovered', 
                             'QuantLimitOnSvc', 
                             'LimitQty', 
                             'LimitUnit', 
                             'Exclusions', 
                             'Explanation', 
                             'EHBVarReason', 
                             'IsExclFromInnMOOP', 
                             'IsExclFromOonMOOP']

#Removing unwanted coloums
plan_dropping_columns = ['DesignType',
                        'UniquePlanDesign',
                        'QHPNonQHPTypeId',
                        'IsNoticeRequiredForPregnancy',
                        'IsReferralRequiredForSpecialist',
                        'SpecialistRequiringReferral',
                        'PlanLevelExclusions',
                        'IndianPlanVariationEstimatedAdvancedPaymentAmountPerEnrollee',
                        'CompositeRatingOffered',
                        'ChildOnlyOffering',
                        'ChildOnlyPlanId',
                        'WellnessProgramOffered',
                        'EHBPercentTotalPremium',
                        'EHBPediatricDentalApportionmentQuantity',
                        'IsGuaranteedRate',
                        'PlanEffectiveDate',
                        'PlanExpirationDate',
                        'OutOfCountryCoverage',
                        'OutOfCountryCoverageDescription',
                        'OutOfServiceAreaCoverage',
                        'OutOfServiceAreaCoverageDescription',
                        'NationalNetwork',
                        'URLForEnrollmentPayment',
                        'FormularyURL',
                        'CSRVariationType',
                        'IssuerActuarialValue',
                        'AVCalculatorOutputNumber',
                        'MedicalDrugDeductiblesIntegrated',
                        'MedicalDrugMaximumOutofPocketIntegrated',
                        'MultipleInNetworkTiers',
                        'FirstTierUtilization',
                        'SecondTierUtilization',
                        'SBCHavingaBabyDeductible',
                        'SBCHavingaBabyCopayment',
                        'SBCHavingaBabyCoinsurance',
                        'SBCHavingaBabyLimit',
                        'SBCHavingDiabetesDeductible',
                        'SBCHavingDiabetesCopayment',
                        'SBCHavingDiabetesCoinsurance',
                        'SBCHavingDiabetesLimit',
                        'SBCHavingSimplefractureDeductible',
                        'SBCHavingSimplefractureCopayment',
                        'SBCHavingSimplefractureCoinsurance',
                        'SBCHavingSimplefractureLimit',
                        'SpecialtyDrugMaximumCoinsurance',
                        'InpatientCopaymentMaximumDays',
                        'BeginPrimaryCareCostSharingAfterNumberOfVisits',
                        'BeginPrimaryCareDeductibleCoinsuranceAfterNumberOfCopays',
                        'MEHBInnTier1IndividualMOOP',
                        'MEHBInnTier1FamilyPerPersonMOOP',
                        'MEHBInnTier1FamilyPerGroupMOOP',
                        'MEHBInnTier2IndividualMOOP',
                        'MEHBInnTier2FamilyPerPersonMOOP',
                        'MEHBInnTier2FamilyPerGroupMOOP',
                        'MEHBOutOfNetIndividualMOOP',
                        'MEHBOutOfNetFamilyPerPersonMOOP',
                        'MEHBOutOfNetFamilyPerGroupMOOP',
                        'MEHBCombInnOonIndividualMOOP',
                        'MEHBCombInnOonFamilyPerPersonMOOP',
                        'MEHBCombInnOonFamilyPerGroupMOOP',
                        'DEHBInnTier1IndividualMOOP',
                        'DEHBInnTier1FamilyPerPersonMOOP',
                        'DEHBInnTier1FamilyPerGroupMOOP',
                        'DEHBInnTier2IndividualMOOP',
                        'DEHBInnTier2FamilyPerPersonMOOP',
                        'DEHBInnTier2FamilyPerGroupMOOP',
                        'DEHBOutOfNetIndividualMOOP',
                        'DEHBOutOfNetFamilyPerPersonMOOP',
                        'DEHBOutOfNetFamilyPerGroupMOOP',
                        'DEHBCombInnOonIndividualMOOP',
                        'DEHBCombInnOonFamilyPerPersonMOOP',
                        'DEHBCombInnOonFamilyPerGroupMOOP',
                        'TEHBInnTier1IndividualMOOP',
                        'TEHBInnTier1FamilyPerPersonMOOP',
                        'TEHBInnTier1FamilyPerGroupMOOP',
                        'TEHBInnTier2IndividualMOOP',
                        'TEHBInnTier2FamilyPerPersonMOOP',
                        'TEHBInnTier2FamilyPerGroupMOOP',
                        'TEHBOutOfNetIndividualMOOP',
                        'TEHBOutOfNetFamilyPerPersonMOOP',
                        'TEHBOutOfNetFamilyPerGroupMOOP',
                        'TEHBCombInnOonIndividualMOOP',
                        'TEHBCombInnOonFamilyPerPersonMOOP',
                        'TEHBCombInnOonFamilyPerGroupMOOP',
                        'MEHBDedInnTier1Individual',
                        'MEHBDedInnTier1FamilyPerPerson',
                        'MEHBDedInnTier1FamilyPerGroup',
                        'MEHBDedInnTier1Coinsurance',
                        'MEHBDedInnTier2Individual',
                        'MEHBDedInnTier2FamilyPerPerson',
                        'MEHBDedInnTier2FamilyPerGroup',
                        'MEHBDedInnTier2Coinsurance',
                        'MEHBDedOutOfNetIndividual',
                        'MEHBDedOutOfNetFamilyPerPerson',
                        'MEHBDedOutOfNetFamilyPerGroup',
                        'MEHBDedCombInnOonIndividual',
                        'MEHBDedCombInnOonFamilyPerPerson',
                        'MEHBDedCombInnOonFamilyPerGroup',
                        'DEHBDedInnTier1Individual',
                        'DEHBDedInnTier1FamilyPerPerson',
                        'DEHBDedInnTier1FamilyPerGroup',
                        'DEHBDedInnTier1Coinsurance',
                        'DEHBDedInnTier2Individual',
                        'DEHBDedInnTier2FamilyPerPerson',
                        'DEHBDedInnTier2FamilyPerGroup',
                        'DEHBDedInnTier2Coinsurance',
                        'DEHBDedOutOfNetIndividual',
                        'DEHBDedOutOfNetFamilyPerPerson',
                        'DEHBDedOutOfNetFamilyPerGroup',
                        'DEHBDedCombInnOonIndividual',
                        'DEHBDedCombInnOonFamilyPerPerson',
                        'DEHBDedCombInnOonFamilyPerGroup',
                        'TEHBDedInnTier1Individual',
                        'TEHBDedInnTier1FamilyPerPerson',
                        'TEHBDedInnTier1FamilyPerGroup',
                        'TEHBDedInnTier1Coinsurance',
                        'TEHBDedInnTier2Individual',
                        'TEHBDedInnTier2FamilyPerPerson',
                        'TEHBDedInnTier2FamilyPerGroup',
                        'TEHBDedInnTier2Coinsurance',
                        'TEHBDedOutOfNetIndividual',
                        'TEHBDedOutOfNetFamilyPerPerson',
                        'TEHBDedOutOfNetFamilyPerGroup',
                        'TEHBDedCombInnOonIndividual',
                        'TEHBDedCombInnOonFamilyPerPerson',
                        'TEHBDedCombInnOonFamilyPerGroup',
                        'IsHSAEligible',
                        'HSAOrHRAEmployerContribution',
                        'HSAOrHRAEmployerContributionAmount',
                        'URLForSummaryofBenefitsCoverage',
                        'PlanBrochure'
                        ]



def cleaning_data(running_in_cluster):

    #This function cleans the data and converts the csv to parquet format

    #This parameter is set because in cluster the default filesystem is hdfs where as in local it is default filesystem
    if running_in_cluster != 1:

        initial_path = './prepared_dataset/'

        parquet_path = './parquet_dataset/'

        prepared = os.listdir(initial_path)

    else:

        initial_path = 'america_insurance/prepared_dataset/'

        parquet_path = 'america_insurance/parquet_dataset/'

        prepared = ['benefits-and-cost-sharing-puf', 'rate-puf', 'plan-attributes-puf', 'plan-id-crosswalk-puf']


    for folder in prepared:

        prepared_folder_path = initial_path + folder + "/"

        parquet_folder_path = parquet_path + folder + "/"

        if running_in_cluster == 0:
            if not os.path.exists(parquet_folder_path):
                os.makedirs(parquet_folder_path)

        if folder == 'benefits-and-cost-sharing-puf':
            df = spark.read.csv(prepared_folder_path, schema = benefits_schema,  header = True)
            df = df.drop(*benefits_dropping_columns)

        elif(folder == 'rate-puf'):
            df = spark.read.csv(prepared_folder_path, schema = new_rates_schema, header = True)
            df = df.filter(df['BusinessYear']!='2016')
            
            tmp = spark.read.csv(prepared_folder_path+'Rate_PUF2016.csv', schema = rates_schema, header = True)
            df = df.union(tmp.filter(tmp['BusinessYear']=='2016').drop('VersionNum','IssuerId2','RowNumber'))

        elif(folder == 'plan-id-crosswalk-puf'):
            years = ['2020', '2019', '2018', '2017', '2016']
            df = spark.createDataFrame([], new_crosswalk_schema)
            for year in years:
                tmp = spark.read.csv(prepared_folder_path + 'Plan_ID_Crosswalk_PUF' + year + '.csv', schema = crosswalk_schema,  header = True)
                tmp = tmp.withColumn('BusinessYear', functions.lit(year))
                df = df.union(tmp)
        else:
            df = spark.read.csv(prepared_folder_path, schema = plan_schema,  header = True)
            df = df.drop(*plan_dropping_columns)

        df.repartition(1).write.mode('overwrite').parquet(parquet_folder_path)

    print("Cleaning done proceed with running codes ...")


if __name__ == '__main__':
        
    spark = SparkSession.builder.appName("etl").getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    
    #Function call to clean the data and convert to parquet
    running_in_cluster = 1
    cleaning_data(running_in_cluster)
