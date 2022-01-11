import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession,types
from pyspark.sql.functions import *
from schemas import benefits_schema, rates_schema, plan_schema

def main(inputs,output):
    # main logic starts here
    rate=spark.read.parquet(inputs+'/rate-puf', header=True,schema=rates_schema)
    plan_attributes=spark.read.parquet(inputs+'/plan-attributes-puf', header=True,schema=plan_schema)
    rate.show()
    rate_select=rate.select('BusinessYear','StateCode','IndividualRate','PlanId','Age')
    plan_attributes_select=plan_attributes.select('PlanId','DentalOnlyPlan')
    # Number of US states included in the dataset
    #print(rate_select.select('StateCode').distinct().count())
    # How many of years of information is extracted
    #rate_select.select('BusinessYear').distinct().show()

    # Joining rates and PlanAttributes table to distinguish medical and dental rates
    # rates table has all the price details, PlanAttributes has information of plan coverage
    rate_join=rate_select.join(plan_attributes_select,(rate_select.PlanId==substring(plan_attributes_select.PlanId,1,14)),"inner")
    
    # Average rate in relation to age in all the given years (includes both medical only and dental only)
    rate_age=rate_select.select('IndividualRate','Age').groupBy('Age').agg({"IndividualRate": "avg"})
    rate_age=rate_age.withColumnRenamed('avg(IndividualRate)','avg_rate_age').orderBy('avg_rate_age',ascending=False)
    #rate_age.show(100)
    # Average Rate is Positively Correlated with Age

    # Average Rate in relation to age across all states in all the given years
    # And also Extracting Data for Heat Map
    rate_age_states=rate_join.select('StateCode','Age','IndividualRate').groupBy('StateCode','Age').agg({"IndividualRate": "avg"})
    rate_age_states=rate_age_states.withColumnRenamed('avg(IndividualRate)','avg_rate_age').orderBy('StateCode',ascending=False)
    #rate_age_states.show(100)

    # Checking whether the above Correlation is true for both Mental Only and Dental Only

    # Average Medical rate in relation to age in all the given years
    # Filtered Medical only data with 'dentalonlyplan' Column
    medicalrate_age=rate_join.filter(rate_join.DentalOnlyPlan=='No').select('Age','IndividualRate').groupBy('Age').agg({"IndividualRate": "avg"})
    medicalrate_age=medicalrate_age.withColumnRenamed('avg(IndividualRate)','medicalrate_only_average').orderBy('medicalrate_only_average',ascending=False)
    #medicalrate_age.show(100)
    # Medical Rate is also positively correlated with AGe

    # Average Medical rate in relation to age across all states in all the given years
    # And also Extracting Data for Heat Map
    medicalrate_age_states=rate_join.filter(rate_join.DentalOnlyPlan=='No').select('StateCode','Age','IndividualRate').groupBy('StateCode','Age').agg({"IndividualRate": "avg"})
    medicalrate_age_states=medicalrate_age_states.withColumnRenamed('avg(IndividualRate)','medicalrate_only_average').orderBy('StateCode',ascending=False)
    #medicalrate_age_states.show(100)

    # Average dental rate in relation to age in all the given years
    # Filtered Dental only data with 'dentalonlyplan' Column
    dentalrate_age=rate_join.filter((rate_join.DentalOnlyPlan=='Yes')&(rate_join.IndividualRate<999999)).select('Age','IndividualRate').groupBy('Age').agg({"IndividualRate": "avg"})
    dentalrate_age=dentalrate_age.withColumnRenamed('avg(IndividualRate)','dentalrate_only_average').orderBy('dentalrate_only_average',ascending=False)
    #dentalrate_age.show(100)
    # But Dental Rate has no correlation with Age

    # Average Denal rate in relation to age across all states in all the given years
    # And also Extracting Data for Heat Map
    denalrate_age_states=rate_join.filter(rate_join.DentalOnlyPlan=='Yes').select('StateCode','Age','IndividualRate').groupBy('StateCode','Age').agg({"IndividualRate": "avg"})
    denalrate_age_states=denalrate_age_states.withColumnRenamed('avg(IndividualRate)','dentalrate_only_average').orderBy('StateCode',ascending=False)
    #denalrate_age_states.show(100)

    
    # rate_join_medicalonly.coalesce(1)
    # writing files into HDFS Storage
    # And also repartitioning to 1 file
    rate_age.repartition(1).write.parquet(output+'/AverageRate_Age/rate_age',mode='overwrite')
    rate_age_states.repartition(1).write.parquet(output+'/AverageRate_Age/rate_age_states',mode='overwrite')
    medicalrate_age.repartition(1).write.parquet(output+'/AverageRate_Age/medicalrate_age',mode='overwrite')
    medicalrate_age_states.repartition(1).write.parquet(output+'/AverageRate_Age/medicalrate_age_states',mode='overwrite')
    dentalrate_age.repartition(1).write.parquet(output+'/AverageRate_Age/dentalrate_age',mode='overwrite')
    denalrate_age_states.repartition(1).write.parquet(output+'/AverageRate_Age/denalrate_age_states',mode='overwrite')
    
if __name__ == '__main__':
    inputs='america_insurance/parquet_dataset'
    output='america_insurance/s3'
    spark = SparkSession.builder.appName('Health_Insuarnce').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs,output)
