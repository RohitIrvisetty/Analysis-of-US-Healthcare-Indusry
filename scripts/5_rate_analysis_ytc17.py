
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from schemas import benefits_schema, rates_schema, plan_schema, crosswalk_schema



def rate_analysis(inputs, output1, output2,output3):
    #This function performs rate analysis - Average plan rate across 5 years
    
    # Reading the dataframe
    rate = spark.read.parquet(inputs, header = True, schema = rates_schema)

    rate.createOrReplaceTempView("rate")

    #Average plan rate according to age by state, smoker / non-smoker
    rate_age_and_statecode_df = rate.groupby('PlanId', 'StateCode', 'BusinessYear').agg(\
        functions.avg('IndividualRate').alias('AVG_IndividualRate'), \
        functions.avg('IndividualTobaccoRate').alias('AVG_IndividualTobaccoRate'), \
        functions.avg('Couple').alias('AVG_Couple'), \
        functions.avg('PrimarySubscriberAndOneDependent').alias('AVG_PrimarySubscriberAndOneDependent'), \
        functions.avg('PrimarySubscriberAndThreeOrMoreDependents').alias('AVG_PrimarySubscriberAndThreeOrMoreDependents'), \
        functions.avg('CoupleAndOneDependent').alias('AVG_CoupleAndOneDependent'), \
        functions.avg('CoupleAndTwoDependents').alias('AVG_CoupleAndTwoDependents'), \
        functions.avg('CoupleAndThreeOrMoreDependents').alias('AVG_CoupleAndThreeOrMoreDependents'), \
        ).groupby('StateCode', 'BusinessYear').agg( \
            functions.countDistinct('PlanId').alias('Plan_Count'), \
            functions.avg('AVG_IndividualRate').alias('AVG_IndividualRate'), \
            functions.avg('AVG_IndividualTobaccoRate').alias('AVG_IndividualTobaccoRate'), \
            functions.avg('AVG_Couple').alias('AVG_Couple'), \
            functions.avg('AVG_PrimarySubscriberAndOneDependent').alias('AVG_PrimarySubscriberAndOneDependent'), \
            functions.avg('AVG_PrimarySubscriberAndThreeOrMoreDependents').alias('AVG_PrimarySubscriberAndThreeOrMoreDependents'), \
            functions.avg('AVG_CoupleAndOneDependent').alias('AVG_CoupleAndOneDependent'), \
            functions.avg('AVG_CoupleAndTwoDependents').alias('AVG_CoupleAndTwoDependents'), \
            functions.avg('AVG_CoupleAndThreeOrMoreDependents').alias('AVG_CoupleAndThreeOrMoreDependents'), \
        ).cache()

    #1. change of average rate across the years for individual
    #2. change of average rate across the years for individual smokers
    rate_individual_years_df = rate_age_and_statecode_df.groupBy('BusinessYear').agg(\
        functions.avg('AVG_IndividualRate').alias('AVG_IndividualRate'), \
        functions.avg('AVG_IndividualTobaccoRate').alias('AVG_IndividualTobaccoRate'), \
    )
    
    rate_family_years_df = rate_age_and_statecode_df.groupBy('BusinessYear').agg( \
        functions.avg('AVG_Couple').alias('AVG_Couple'), \
        functions.avg('AVG_PrimarySubscriberAndOneDependent').alias('AVG_PrimarySubscriberAndOneDependent'), \
        functions.avg('AVG_PrimarySubscriberAndThreeOrMoreDependents').alias('AVG_PrimarySubscriberAndThreeOrMoreDependents'), \
        functions.avg('AVG_CoupleAndOneDependent').alias('AVG_CoupleAndOneDependent'), \
        functions.avg('AVG_CoupleAndTwoDependents').alias('AVG_CoupleAndTwoDependents'), \
        functions.avg('AVG_CoupleAndThreeOrMoreDependents').alias('AVG_CoupleAndThreeOrMoreDependents'), \
    )
    
    
    
    #Writing the analysis base in parquet
    rate_age_and_statecode_df.orderBy(rate_age_and_statecode_df['AVG_Couple'].desc()).show(10)
    rate_age_and_statecode_df.repartition(1).write.mode('overwrite').parquet(output3)
    
    #Writing the results in parquet
    rate_individual_years_df.show(10)
    rate_individual_years_df.repartition(1).write.mode('overwrite').parquet(output1)

    #Writing the results in parquet
    rate_family_years_df.show(10)
    rate_family_years_df.repartition(1).write.mode('overwrite').parquet(output2)

if __name__ == '__main__':
    input_folder = sys.argv[1]

    spark = SparkSession.builder.appName('queries').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    #This parameter is set because in cluster the default filesystem is hdfs where as in local it is default filesystem

    running_in_cluster = 1

    if running_in_cluster == 0:
        inputs = '/home/rovenna/Desktop/Project/parquet_dataset/rate-puf'
        output1 = '/home/rovenna/Desktop/Project/s3/rate-individual-year/'
        output2 = '/home/rovenna/Desktop/Project/s3/rate-family-year/'
        output3 = '/home/rovenna/Desktop/Project/s3/rate-analysis/'
    else:
        inputs = '{}/parquet_dataset/rate-puf'.format(input_folder)
        output1 = '{}/s3/rate-individual-year/'.format(input_folder)
        output2 = '{}/s3/rate-family-year/'.format(input_folder)
        output3 = '{}/s3/rate-analysis/'.format(input_folder)
    
    rate_analysis(inputs,output1, output2, output3)