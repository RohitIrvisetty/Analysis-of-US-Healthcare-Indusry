
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from schemas import benefits_schema, rates_schema, plan_schema, crosswalk_schema

def family_rate_analysis(inputs, output):
    #This function compares the plan rate if families of different sizes subscribe to insurance plans individually / as a single family unit
    
    # Reading the dataframe
    rate = spark.read.parquet(inputs, header = True, schema = rates_schema)

    rate.createOrReplaceTempView("rate")

    #data manipulation
    condition1 = (rate['Age'] == '0-20') | (rate['Age'] == '65 and over') | (rate['Age'] == 'Family Option')
    condition2 = (rate['Age'] != '0-20') & (rate['Age'] != '65 and over')
    condition3 = (rate['Age'] == '0-14') | (rate['Age'] == '64 and over') | (rate['Age'] == 'Family Option')
    condition4 = (rate['Age'] != '0-14') & (rate['Age'] != '64 and over')
    age_convert1 = rate.filter(condition1).withColumn('AgeNum', functions.lit(-1))
    age_convert2 = rate.filter(condition2 & condition4 & (rate['Age'] != 'Family Option')).withColumn('AgeNum',functions.trim(rate['Age']).cast('int'))
    age_convert3 = rate.filter(condition3).withColumn('AgeNum', functions.lit(-1))
    age_convert4 = rate.filter(rate['Age'] == 'Family Option').withColumn('AgeNum', functions.lit(-1))
    
    age_converted = age_convert1.union(age_convert2).union(age_convert3).union(age_convert4)
    
    age_converted.show(10)
    
    
    #Case 1: Couple, assume both X years old
    #note that family options and individual options are separate options in all plans
    
    expr = 'percentile_approx({}, 0.5)'
    #quantiles = functions.expr('percentile_approx({}, array(0.25, 0.5, 0.75))')
    
    rate_couple_df = age_converted.filter(age_converted['Age'] == 'Family Option').groupby('BusinessYear', 'PlanId').agg( \
        functions.avg('Couple').alias('AVG_Couple'), \
        functions.max('Couple').alias('MAX_Couple'), \
        functions.min('Couple').alias('MIN_Couple'), \
        functions.expr(expr.format('Couple')).alias('Median_Couple'), \
    ).groupby('BusinessYear').agg( \
        functions.avg('AVG_Couple').alias('AVG_Couple'), \
        functions.max('MAX_Couple').alias('MAX_Couple'), \
        functions.min('MIN_Couple').alias('MIN_Couple'), \
        functions.expr(expr.format('Median_Couple')).alias('Median_Couple'), \
    ).cache()
    
    rate_couple_df.orderBy(rate_couple_df['BusinessYear'].asc()).show(30)
    
    
    rate_in_df = age_converted.filter(age_converted['AgeNum']!=-1).groupby('BusinessYear','PlanId','AgeNum').agg( \
        functions.avg(age_converted['IndividualRate']).alias('AVG_IN_Sub'), \
        functions.max(age_converted['IndividualRate']).alias('MAX_IN_Sub'), \
        functions.min(age_converted['IndividualRate']).alias('MIN_IN_Sub'), \
        functions.expr(expr.format('IndividualRate')).alias('Median_IN_Sub'), \
    )
    
    rate_in_df.orderBy(rate_in_df['AVG_IN_Sub'].asc()).show(30)
    
    rate_in_df = rate_in_df.groupby('BusinessYear','AgeNum').agg( \
        functions.avg(rate_in_df['AVG_IN_Sub']*2).alias('AVG_IN_Sub'), \
        functions.max(rate_in_df['MAX_IN_Sub']*2).alias('MAX_IN_Sub'), \
        functions.min(rate_in_df['MIN_IN_Sub']*2).alias('MIN_IN_Sub'), \
        functions.expr(expr.format('Median_IN_Sub')).alias('Median_IN_Sub'), \
    ).cache()
    
    rate_in_df.orderBy(rate_in_df['BusinessYear'].asc()).show(30)

    couple_consolidated = rate_in_df.join(rate_couple_df,['BusinessYear'])
    
    print('couple_consolidated')
    couple_consolidated.orderBy(couple_consolidated['AVG_Couple'].desc()).show(30)
    
    #Case 2: Family with couple and 1 child, assume couples are both X years old and the child is 0-20
    #already got rate for couple and individual adult in rate_couple_df and rate_in_df resp
    rate_family_df = age_converted.filter(age_converted['CoupleAndOneDependent'].isNotNull()).groupby('BusinessYear','PlanId').agg( \
        functions.avg(age_converted['CoupleAndOneDependent']).alias('AVG_Family1'), \
        functions.max(age_converted['CoupleAndOneDependent']).alias('MAX_Family1'), \
        functions.min(age_converted['CoupleAndOneDependent']).alias('MIN_Family1'), \
        functions.expr(expr.format('CoupleAndOneDependent')).alias('Median_Family1'), \
        functions.avg(age_converted['CoupleAndTwoDependents']).alias('AVG_Family2'), \
        functions.max(age_converted['CoupleAndTwoDependents']).alias('MAX_Family2'), \
        functions.min(age_converted['CoupleAndTwoDependents']).alias('MIN_Family2'), \
        functions.expr(expr.format('CoupleAndTwoDependents')).alias('Median_Family2'), \
        functions.avg(age_converted['CoupleAndThreeOrMoreDependents']).alias('AVG_Family3'), \
        functions.max(age_converted['CoupleAndThreeOrMoreDependents']).alias('MAX_Family3'), \
        functions.min(age_converted['CoupleAndThreeOrMoreDependents']).alias('MIN_Family3'), \
        functions.expr(expr.format('CoupleAndThreeOrMoreDependents')).alias('Median_Family3'), \
    )

    print('rate_family_df1')
    rate_family_df.show()
    
    rate_family_df = rate_family_df.groupby('BusinessYear').agg( \
        functions.avg(rate_family_df['AVG_Family1']).alias('AVG_Family1'), \
        functions.max(rate_family_df['MAX_Family1']).alias('MAX_Family1'), \
        functions.min(rate_family_df['MIN_Family1']).alias('MIN_Family1'), \
        functions.expr(expr.format('Median_Family1')).alias('Median_Family1'), \
        functions.avg(rate_family_df['AVG_Family2']).alias('AVG_Family2'), \
        functions.max(rate_family_df['MAX_Family2']).alias('MAX_Family2'), \
        functions.min(rate_family_df['MIN_Family2']).alias('MIN_Family2'), \
        functions.expr(expr.format('Median_Family2')).alias('Median_Family2'), \
        functions.avg(rate_family_df['AVG_Family3']).alias('AVG_Family3'), \
        functions.max(rate_family_df['MAX_Family3']).alias('MAX_Family3'), \
        functions.min(rate_family_df['MIN_Family3']).alias('MIN_Family3'), \
        functions.expr(expr.format('Median_Family3')).alias('Median_Family3'), \
    )
    
    print('rate_family_df2')
    rate_family_df.show()
    
    rate_child_df = age_converted.filter((age_converted['Age'] == '0-20') | (age_converted['Age'] == '0-14')).groupby('BusinessYear','PlanId').agg( \
        functions.avg(age_converted['IndividualRate']).alias('AVG_Child_Sub'), \
        functions.max(age_converted['IndividualRate']).alias('MAX_Child_Sub'), \
        functions.min(age_converted['IndividualRate']).alias('MIN_Child_Sub'), \
        functions.expr(expr.format('IndividualRate')).alias('Median_Child_Sub'), \
    )
    rate_child_df = rate_child_df.groupby('BusinessYear').agg( \
        functions.avg(rate_child_df['AVG_Child_Sub']).alias('AVG_Child_Sub'), \
        functions.max(rate_child_df['MAX_Child_Sub']).alias('MAX_Child_Sub'), \
        functions.min(rate_child_df['MIN_Child_Sub']).alias('MIN_Child_Sub'), \
        functions.expr(expr.format('Median_Child_Sub')).alias('Median_Child_Sub'), \
    )
    
    print('rate_child_df')
    rate_child_df.show()
    
    family_case = rate_in_df.join(rate_family_df,['BusinessYear']).join(rate_child_df,['BusinessYear']).cache()
    family_case = family_case.withColumn('family1', family_case['Median_Child_Sub']+family_case['Median_IN_Sub']).withColumn('family2', family_case['Median_Child_Sub']*2+family_case['Median_IN_Sub']).withColumn('family3', family_case['Median_Child_Sub']*3+family_case['Median_IN_Sub'])
    
    family_case_1child = family_case.groupby('BusinessYear').agg( \
        functions.avg(family_case['AVG_Child_Sub']+family_case['AVG_IN_Sub']).alias('AVG_IN_Sub'), \
        functions.max(family_case['MAX_Child_Sub']+family_case['MAX_IN_Sub']).alias('MAX_IN_Sub'), \
        functions.min(family_case['MIN_Child_Sub']+family_case['MIN_IN_Sub']).alias('MIN_IN_Sub'), \
        functions.expr(expr.format('family1')).alias('Median_IN_Sub'), \
        functions.avg(family_case['AVG_Family1']).alias('AVG_Family1'), \
        functions.max(family_case['MAX_Family1']).alias('MAX_Family1'), \
        functions.min(family_case['MIN_Family1']).alias('MIN_Family1'), \
        functions.expr(expr.format('Median_Family1')).alias('Median_Family1'), \
    )

    print('family_case_1child')
    family_case_1child.show()
    
    #Case 3: Family with couple and 2 children
    family_case_2child = family_case.groupby('BusinessYear').agg( \
        functions.avg(family_case['AVG_Child_Sub']*2+family_case['AVG_IN_Sub']).alias('AVG_IN_Sub'), \
        functions.max(family_case['MAX_Child_Sub']*2+family_case['MAX_IN_Sub']).alias('MAX_IN_Sub'), \
        functions.min(family_case['MIN_Child_Sub']*2+family_case['MIN_IN_Sub']).alias('MIN_IN_Sub'), \
        functions.expr(expr.format('family2')).alias('Median_IN_Sub'), \
        functions.avg(family_case['AVG_Family2']).alias('AVG_Family2'), \
        functions.max(family_case['MAX_Family2']).alias('MAX_Family2'), \
        functions.min(family_case['MIN_Family2']).alias('MIN_Family2'), \
        functions.expr(expr.format('Median_Family2')).alias('Median_Family2'), \
    )
    
    
    print('family_case_2child')
    family_case_2child.show()
    
    
    #Case 4: Family with couple and 3 children and more
    family_case_3child = family_case.groupby('BusinessYear').agg( \
        functions.avg(family_case['AVG_Child_Sub']*3+family_case['AVG_IN_Sub']).alias('AVG_IN_Sub'), \
        functions.max(family_case['MAX_Child_Sub']*3+family_case['MAX_IN_Sub']).alias('MAX_IN_Sub'), \
        functions.min(family_case['MIN_Child_Sub']*3+family_case['MIN_IN_Sub']).alias('MIN_IN_Sub'), \
        functions.expr(expr.format('family3')).alias('Median_IN_Sub'), \
        functions.avg(family_case['AVG_Family3']).alias('AVG_Family3'), \
        functions.max(family_case['MAX_Family3']).alias('MAX_Family3'), \
        functions.min(family_case['MIN_Family3']).alias('MIN_Family3'), \
        functions.expr(expr.format('Median_Family3')).alias('Median_Family3'), \
    )
    
    print('family_case_3child')
    family_case_3child.show()

    #Writing the analysis base in parquet
    age_converted.repartition(1).write.mode('overwrite').parquet(output[0])
    rate_family_df.repartition(1).write.mode('overwrite').parquet(output[1])
    rate_child_df.repartition(1).write.mode('overwrite').parquet(output[2])
    
    couple_consolidated.repartition(1).write.mode('overwrite').parquet(output[3])
    family_case_1child.repartition(1).write.mode('overwrite').parquet(output[4])
    family_case_2child.repartition(1).write.mode('overwrite').parquet(output[5])
    family_case_3child.repartition(1).write.mode('overwrite').parquet(output[6])

if __name__ == '__main__':
    input_folder = sys.argv[1]

    spark = SparkSession.builder.appName('queries').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    #This parameter is set because in cluster the default filesystem is hdfs where as in local it is default filesystem

    running_in_cluster = 1
    output = [None] * 7

    if running_in_cluster == 0:
        inputs = '/home/rovenna/Desktop/Project/parquet_dataset/rate-puf'
        output[0] = '/home/rovenna/Desktop/Project/s3/converted-age-data/'
        output[1] = '/home/rovenna/Desktop/Project/s3/family-rate/'
        output[2] = '/home/rovenna/Desktop/Project/s3/child-rate/'
        output[3] = '/home/rovenna/Desktop/Project/s3/couple-rate/'
        output[4] = '/home/rovenna/Desktop/Project/s3/family_case_1child-rate/'
        output[5] = '/home/rovenna/Desktop/Project/s3/family_case_2child-rate/'
        output[6] = '/home/rovenna/Desktop/Project/s3/family_case_3child-rate/'
    else:
        inputs = '{}/parquet_dataset/rate-puf'.format(input_folder)
        output[0] = '{}/s3/converted-age-data/'.format(input_folder)
        output[1] = '{}/s3/family-rate/'.format(input_folder)
        output[2] = '{}/s3/child-rate/'.format(input_folder)
        output[3] = '{}/s3/couple-rate/'.format(input_folder)
        output[4] = '{}/s3/family_case_1child-rate/'.format(input_folder)
        output[5] = '{}/s3/family_case_2child-rate/'.format(input_folder)
        output[6] = '{}/s3/family_case_3child-rate/'.format(input_folder)
    
    family_rate_analysis(inputs,output)
