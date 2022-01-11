##################################
#Author: Yeuk Ting Chu (Rovenna) (ytc17)
##################################

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from schemas import benefits_schema, rates_schema, plan_schema, crosswalk_schema

def plan_count_over_years(inputs, output1, output2):
    #This function performs analysis on plan scope across the states and identifys the number of plans available for family / individuals
    
    # Reading the dataframe
    rate = spark.read.parquet(inputs, header = True, schema = rates_schema)

    rate.createOrReplaceTempView("rate")

    #Plan scope by state and RatingAreaId
    plan_scope_df = rate.groupby('BusinessYear','PlanId').agg( \
        functions.countDistinct('StateCode').alias('State_Count'), \
        functions.countDistinct('RatingAreaId').alias('Area_Count'), \
    ).orderBy(functions.col('BusinessYear').desc())
    
    plan_scope_df.show()
    
    plan_scope_df = plan_scope_df.orderBy(plan_scope_df['BusinessYear'].desc(), plan_scope_df['State_Count'].desc(), plan_scope_df['Area_Count'].desc())
    
    #Average plan rate if I subscribe my whole family vs individual subscriptions
    #Case 0: Plan count with family option - choices of family plan
    plan_count_family_df = rate.filter(rate['Age'] == 'Family Option').groupby('BusinessYear').agg(functions.countDistinct('PlanId').alias('Family_Plan_Count'))
    
    plan_count_family_df.show()
    
    plan_count_individual_df = rate.filter(rate['Age'] != 'Family Option').groupby('BusinessYear').agg(functions.countDistinct('PlanId').alias('Individual_Plan_Count'))
    
    plan_count_individual_df.show()
    
    plan_count_df = rate.filter((rate['PlanId'].isNotNull()) | (rate['PlanId']!='')).groupby('BusinessYear').agg(functions.countDistinct('PlanId').alias('Total_Plan_Count'))
    
    plan_count_df.show()
    
    consolidated_plan_count_df = plan_count_family_df.join(plan_count_individual_df, ['BusinessYear']).join(plan_count_df, ['BusinessYear']).cache()
    
    consolidated_plan_count_df = consolidated_plan_count_df.select(consolidated_plan_count_df['BusinessYear'],consolidated_plan_count_df['Individual_Plan_Count'],consolidated_plan_count_df['Family_Plan_Count'], consolidated_plan_count_df['Total_Plan_Count'])
    
    consolidated_plan_count_df.show()
    
    #Writing the analysis base in parquet
    plan_scope_df.repartition(1).write.mode('overwrite').parquet(output1)
    consolidated_plan_count_df.repartition(1).write.mode('overwrite').parquet(output2)

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
        output1 = '/home/rovenna/Desktop/Project/s3/plan-scope/'
        output2 = '/home/rovenna/Desktop/Project/s3/plan-count/'
    else:
        inputs = '{}/parquet_dataset/rate-puf'.format(input_folder)
        output1 = '{}/s3/plan-scope/'.format(input_folder)
        output2 = '{}/s3/plan-count/'.format(input_folder)
    
    plan_count_over_years(inputs,output1, output2)