
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from schemas import benefits_schema, rates_schema, plan_schema, crosswalk_schema, new_crosswalk_schema

def crosswalk_analysis(input1, input2, outputs):
    #This function analyze the impact of crosswalk to insurance plan rate for individuals
    
    # Reading the dataframe
    crosswalk = spark.read.parquet(input1, header = True, schema = new_crosswalk_schema)
    age_converted = spark.read.parquet(input2)

    crosswalk.createOrReplaceTempView("crosswalk")
    age_converted.createOrReplaceTempView("rate")

    #Does crosswalk affect the insurance plan rate of individual / family in general
    #Increase rate if there is no crosswalk
    #Increate rate if there is crosswalk
    
    crosswalk_case_df = crosswalk.select('BusinessYear','State','Old_PlanID', 'Old_IssuerID', 'Old_MetalLevel','New_PlanID', 'New_IssuerID', 'New_MetalLevel').withColumn('previousYear', functions.lit(crosswalk['BusinessYear'].cast('int')-1).cast('string'))
    
    expr = 'percentile_approx({}, 0.5)'
    rate_selected = age_converted.select('BusinessYear','StateCode','PlanId','Age', 'AgeNum','IndividualRate','IndividualTobaccoRate',	'Couple', 'PrimarySubscriberAndOneDependent','PrimarySubscriberAndTwoDependents',	'PrimarySubscriberAndThreeOrMoreDependents',	'CoupleAndOneDependent',	'CoupleAndTwoDependents',	'CoupleAndThreeOrMoreDependents').groupby('BusinessYear','PlanId').agg( \
        functions.avg(age_converted['IndividualRate']).alias('AVG_IndividualRate'), \
        functions.max(age_converted['IndividualRate']).alias('MAX_IndividualRate'), \
        functions.min(age_converted['IndividualRate']).alias('MIN_IndividualRate'), \
        functions.expr(expr.format('IndividualRate')).alias('Median_IndividualRate'), \
    )
    
    #crosswalk.filter(crosswalk['Old_PlanID'] == '10046HI0020005').show()
    #crosswalk_case_df.filter(crosswalk_case_df['Old_PlanID'] == '10046HI0020005').show()
    #rate_selected.filter(rate_selected['PlanID'] == '10046HI0020005').show()
    
    #find rate and identify whether the rate has been increased / decreased
    rate_table_df = crosswalk_case_df.join(rate_selected.alias('old_rate'), (crosswalk_case_df['Old_PlanID'] == functions.col('old_rate.PlanId')) & (crosswalk_case_df['previousYear'] == functions.col('old_rate.BusinessYear'))).join(rate_selected.alias('new_rate'),(crosswalk_case_df['New_PlanID'] == functions.col('new_rate.PlanId')) & (crosswalk_case_df['BusinessYear'] == functions.col('new_rate.BusinessYear')))
    #rate_table_df = crosswalk_case_df.join(rate_selected.alias('old_rate'), (crosswalk_case_df['Old_PlanID'] == functions.col('old_rate.PlanId'))).join(rate_selected.alias('new_rate'),(crosswalk_case_df['New_PlanID'] == functions.col('new_rate.PlanId')))
    
    
    rate_table_df.show()
    
    rate_table_df = rate_table_df.withColumn('rateIncreased_avg', rate_table_df['new_rate.AVG_IndividualRate'] - rate_table_df['old_rate.AVG_IndividualRate'])
    rate_table_df = rate_table_df.withColumn('rateIncreased_min', rate_table_df['new_rate.MIN_IndividualRate'] - rate_table_df['old_rate.MIN_IndividualRate'])
    rate_table_df = rate_table_df.withColumn('rateIncreased_max', rate_table_df['new_rate.MAX_IndividualRate'] - rate_table_df['old_rate.MAX_IndividualRate'])
    rate_table_df = rate_table_df.withColumn('rateIncreased_Median', rate_table_df['new_rate.Median_IndividualRate'] - rate_table_df['old_rate.Median_IndividualRate'])
    rate_cw_analyzed_df = rate_table_df.withColumn('issuerChanged', rate_table_df['Old_IssuerID'] != rate_table_df['New_IssuerID']).withColumn('metalChanged', rate_table_df['Old_MetalLevel'] != rate_table_df['New_MetalLevel'])
    
    rate_cw_analyzed_df.show()
    
    #crosswalk distribution
    crosswalk_stat1_df = crosswalk_case_df.groupBy('BusinessYear','State').agg( \
        functions.countDistinct('Old_PlanID').alias('Plan_Count'), \
    )
    
    crosswalk_stat2_df = crosswalk_stat1_df.groupBy('BusinessYear').agg( \
        functions.sum('Plan_Count').alias('Total_Plan_Count'), \
    )
    
    rate_cw_analyzed_df= rate_cw_analyzed_df.select('previousYear','new_rate.BusinessYear','State','Old_PlanID', 'Old_IssuerID', 'Old_MetalLevel','New_PlanID', 'New_IssuerID', 'New_MetalLevel','rateIncreased_avg','rateIncreased_max','rateIncreased_min', 'rateIncreased_Median', 'issuerChanged','metalChanged')
    
    cnt_cond = lambda cond: functions.sum(functions.when(cond, 1).otherwise(0))
    
    
    rate_cw_analyzed_updated_df= rate_cw_analyzed_df.groupBy('new_rate.BusinessYear').agg( \
        functions.countDistinct('Old_PlanID').alias('Plan_Count'), \
        cnt_cond(rate_cw_analyzed_df['issuerChanged']=='true').alias('Issuer_Change'), \
        cnt_cond(rate_cw_analyzed_df['metalChanged']=='true').alias('Metal_Change'), \
        cnt_cond(rate_cw_analyzed_df['rateIncreased_Median'] > 0).alias('Rate_Increase'), \
        cnt_cond(rate_cw_analyzed_df['rateIncreased_avg'] > 0).alias('Rate_Increase_Avg'), \
        cnt_cond(rate_cw_analyzed_df['rateIncreased_Median'] < 0).alias('Rate_Decrease'), \
    )
    
    rate_cw_analyzed_updated_df.show()
    
    #Writing the analysis base in parquet
    rate_cw_analyzed_df.repartition(1).write.mode('overwrite').parquet(outputs[0])
    crosswalk_stat1_df.repartition(1).write.mode('overwrite').parquet(outputs[1])
    crosswalk_stat2_df.repartition(1).write.mode('overwrite').parquet(outputs[2])
    rate_cw_analyzed_updated_df.repartition(1).write.mode('overwrite').parquet(outputs[3])
    #consolidated_plan_count_df.repartition(1).write.mode('overwrite').parquet(output2)
    
    #        .withColumn('withoutCrosswalk', rate_table_df['Old_IssuerID'] != rate_table_df['New_IssuerID'])\
    
    

if __name__ == '__main__':
    input_folder = sys.argv[1]

    spark = SparkSession.builder.appName('queries').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    #This parameter is set because in cluster the default filesystem is hdfs where as in local it is default filesystem

    running_in_cluster = 1
    outputs = [None] * 4

    if running_in_cluster == 0:
        input1 = '/home/rovenna/Desktop/Project/parquet_dataset/plan-id-crosswalk-puf'
        input2 = '/home/rovenna/Desktop/Project/s3/converted-age-data'
        outputs[0] = '/home/rovenna/Desktop/Project/s3/crosswalk-rate/'
        outputs[1] = '/home/rovenna/Desktop/Project/s3/crosswalk-stat1/'
        outputs[2] = '/home/rovenna/Desktop/Project/s3/crosswalk-stat2/'
        outputs[3] = '/home/rovenna/Desktop/Project/s3/crosswalk-rate-updated/'
    else:
        input1 = '{}/parquet_dataset/plan-id-crosswalk-puf'.format(input_folder)
        input2 = '{}/s3/converted-age-data'.format(input_folder)
        outputs[0] = '{}/s3/crosswalk-rate/'.format(input_folder)
        outputs[1] = '{}/s3/crosswalk-stat1/'.format(input_folder)
        outputs[2] = '{}/s3/crosswalk-stat2/'.format(input_folder)
        outputs[3] = '{}/s3/crosswalk-rate-updated/'.format(input_folder)
      
    crosswalk_analysis(input1,input2,outputs)
