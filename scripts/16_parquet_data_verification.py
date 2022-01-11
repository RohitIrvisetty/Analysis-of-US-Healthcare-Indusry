
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from schemas import benefits_schema, rates_schema, plan_schema, crosswalk_schema, new_crosswalk_schema

def parquet_data_verification(inputs):
    #This is a simple function to verify the column names of the parquet to make use they matches with the create table scripts to be run in Redshift
    
    # Reading the parquet
    #0
    child_rate = spark.read.parquet(inputs[0], header = True)
    
    print("child_rate")
    print(child_rate.schema)
    
    child_rate.show(5)
    
    
    
    #1
    converted_age_data = spark.read.parquet(inputs[1], header = True)
    
    print("converted_age_data")
    print(converted_age_data.schema)
    
    converted_age_data.show(1)
    
    #2
    couple_rate = spark.read.parquet(inputs[2], header = True)
    
    print("couple_rate")
    print(couple_rate.schema)
    
    couple_rate.show(1)
    
    #3
    crosswalk_rate = spark.read.parquet(inputs[3], header = True)
    
    print("crosswalk_rate")
    print(crosswalk_rate.schema)
    
    crosswalk_rate.show(1)
    
    #4
    crosswalk_rate_updated = spark.read.parquet(inputs[4], header = True)
    
    print("crosswalk_rate_updated")
    print(crosswalk_rate_updated.schema)
    
    crosswalk_rate_updated.show(1)
    
    
    #5
    crosswalk_stat1 = spark.read.parquet(inputs[5], header = True)
    
    print("crosswalk_stat1")
    print(crosswalk_stat1.schema)
    
    crosswalk_stat1.show(1)
    
    #6
    crosswalk_stat2 = spark.read.parquet(inputs[6], header = True)
    
    print("crosswalk_stat2")
    print(crosswalk_stat2.schema)
    
    crosswalk_stat2.show(1)
    
    #7
    family_case_1child_rate = spark.read.parquet(inputs[7], header = True)
    
    print("family_case_1child_rate")
    print(family_case_1child_rate.schema)
    
    family_case_1child_rate.show(1)
    
    #8
    family_case_2child_rate = spark.read.parquet(inputs[8], header = True)
    
    print("family_case_2child_rate")
    print(family_case_2child_rate.schema)
    
    family_case_2child_rate.show(1)
    
    #9
    family_case_3child_rate = spark.read.parquet(inputs[9], header = True)
    
    print("family_case_3child_rate")
    print(family_case_3child_rate.schema)
    
    family_case_3child_rate.show(1)
    
    #10
    family_rate = spark.read.parquet(inputs[10], header = True)
    
    print("family_rate")
    print(family_rate.schema)
    
    family_rate.show(1)
    
    #11
    plan_count = spark.read.parquet(inputs[11], header = True)
    
    print("plan_count")
    print(plan_count.schema)
    
    plan_count.show(1)
    
    #12
    plan_scope = spark.read.parquet(inputs[12], header = True)
    
    print("plan_scope")
    print(plan_scope.schema)
    
    plan_scope.show(1)
    
    #13
    rate_analysis = spark.read.parquet(inputs[13], header = True)
    
    print("rate_analysis")
    print(rate_analysis.schema)
    
    rate_analysis.show(1)
    
    #14
    rate_family_year = spark.read.parquet(inputs[14], header = True)
    
    print("rate_family_year")
    print(rate_family_year.schema)
    
    rate_family_year.show(1)
    
    #15
    rate_individual_year = spark.read.parquet(inputs[15], header = True)
    
    print("rate_individual_year")
    print(rate_individual_year.schema)
    
    rate_individual_year.show(1)
    
    print("****************************")
    
    print("All Done!")
   

if __name__ == '__main__':
    input_folder = sys.argv[1]

    spark = SparkSession.builder.appName('queries').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    #This parameter is set because in cluster the default filesystem is hdfs where as in local it is default filesystem

    running_in_cluster = 1
    inputs = [None] * 16

    if running_in_cluster == 0:
    # this will never be run locally so I will skip this part
        pass
    else:
        inputs[0] = '{}/s3/child-rate'.format(input_folder)
        inputs[1] = '{}/s3/converted-age-data'.format(input_folder)
        inputs[2] = '{}/s3/couple-rate/'.format(input_folder)
        inputs[3] = '{}/s3/crosswalk-rate/'.format(input_folder)
        inputs[4] = '{}/s3/crosswalk-rate-updated/'.format(input_folder)
        inputs[5] = '{}/s3/crosswalk-stat1/'.format(input_folder)
        inputs[6] = '{}/s3/crosswalk-stat2/'.format(input_folder)
        inputs[7] = '{}/s3/family_case_1child-rate/'.format(input_folder)
        inputs[8] = '{}/s3/family_case_2child-rate/'.format(input_folder)
        inputs[9] = '{}/s3/family_case_3child-rate/'.format(input_folder)
        inputs[10] = '{}/s3/family-rate/'.format(input_folder)
        inputs[11] = '{}/s3/plan-count/'.format(input_folder)
        inputs[12] = '{}/s3/plan-scope/'.format(input_folder)
        inputs[13] = '{}/s3/rate-analysis/'.format(input_folder)
        inputs[14] = '{}/s3/rate-family-year/'.format(input_folder)
        inputs[15] = '{}/s3/rate-individual-year/'.format(input_folder)
      
    parquet_data_verification(inputs)
