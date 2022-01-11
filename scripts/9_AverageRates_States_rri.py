import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession,types
from pyspark.sql import functions
from schemas import benefits_schema, rates_schema, plan_schema

def main(inputs,output):
    # main logic starts here
    rate=spark.read.parquet(inputs+'/rate-puf', header=True,schema=rates_schema)
    plan_attributes=spark.read.parquet(inputs+'/plan-attributes-puf', header=True,schema=plan_schema)
    #rate.show()
    #plan_attributes.show()
    rate_select=rate.select('BusinessYear','StateCode','IndividualRate','PlanId')
    plan_attributes_select=plan_attributes.select('PlanId','DentalOnlyPlan')
    plan_attributes_select=plan_attributes_select.withColumnRenamed('PlanId','PlanId1')
    # Number of US states included in the dataset
    print(rate_select.select('StateCode').distinct().count())
    # How many of years of information is extracted
    rate_select.select('BusinessYear').distinct().show()    
    # joining rates and PlanAttributes table to distinguish medical and dental rates
    # rates table has all the price details, PlanAttributes has information of plan coverage
    rate_join=rate_select.join(plan_attributes_select,(rate_select.PlanId==functions.substring(plan_attributes_select.PlanId1,1,14)),"inner")
    #rate_join.show()

    # Average Insurance Rate across states in the given years( Irrespective of being medical or dental)
    rate_join_MedicalAndDental=rate_join.select('StateCode','BusinessYear','IndividualRate').groupBy('StateCode','BusinessYear').agg({"IndividualRate": "avg"}).orderBy('avg(IndividualRate)',descending=True)
    rate_join_MedicalAndDental=rate_join_MedicalAndDental.withColumnRenamed('avg(IndividualRate)','Average_Rate')
    #rate_join_MedicalAndDental.show(100)
    # Surprisingly Some States got high average in the years 2016 and then declined sharply in 2017
    # Therefore there are outliers in the data
    rate_join_MedicalAndDental=rate_join.filter((rate_join.StateCode=='MS')&(rate_join.BusinessYear=='2016')).select('IndividualRate').orderBy('IndividualRate',descending=True)
    rate_join_MedicalAndDental.show(100)
    # Looks like state of Missouri has Super Premium Dental Plans Pricing $1000000 per year and the next Premium Pricing starts from $1960 per year
    rate_join_MedicalAndDental=rate_join.filter((rate_join.StateCode=='MS')&(rate_join.BusinessYear=='2017')).select('IndividualRate').orderBy('IndividualRate',descending=True)
    rate_join_MedicalAndDental.show(100)
    # Super Premium Dental Plans Pricing 1000000 were discontinued after 2016
    # Dental plans have high standard deviation So Removing Outliers to calculate the true average
    # So Filtered all the plans >=999999
    rate_join_MedicalAndDental=rate_join.filter(rate_join.IndividualRate<999999).select('StateCode','BusinessYear','IndividualRate').groupBy('StateCode','BusinessYear').agg({"IndividualRate": "avg"})
    rate_join_MedicalAndDental=rate_join_MedicalAndDental.withColumnRenamed('avg(IndividualRate)','Average_Rate').orderBy('Average_Rate',descending=True)
    #rate_join_MedicalAndDental.show(100)
    
    # Average Medical Only Insurance Rate across states in the given years
    # Filtered Medical only data with 'dentalonlyplan' Column
    rate_join_medicalonly=rate_join.filter(rate_join.DentalOnlyPlan=='No').select('StateCode','BusinessYear','IndividualRate').groupBy('StateCode','BusinessYear').agg({"IndividualRate": "avg"})
    rate_join_medicalonly=rate_join_medicalonly.withColumnRenamed('avg(IndividualRate)','MedicalRate_average').orderBy('medicalrate_average',descending=True)
    #rate_join_medicalonly.show(100)

    # Average Dental Only Insurance Rate across states in the given years
    # Filtered Dental only data with 'dentalonlyplan' Column
    # Dental plans have high standard deviation and Super Premium Plans were discontinued after 2016
    # So Filtered all the plans >=999999
    rate_join_dentalonly=rate_join.filter((rate_join.DentalOnlyPlan=='Yes')&(rate_join.IndividualRate<999999)).select('StateCode','BusinessYear','IndividualRate').groupBy('StateCode','BusinessYear').agg({"IndividualRate": "avg"})
    rate_join_dentalonly=rate_join_dentalonly.withColumnRenamed('avg(IndividualRate)','DentalRate_average').orderBy('dentalrate_average',descending=True)
    #rate_join_dentalonly.show(100)
    # rate_join_medicalonly.coalesce(1)
    # writing files into HDFS Storage
    # And also repartitioning to 1 file
    rate_join_medicalonly.repartition(1).write.parquet(output+'/AverageAcrossStates/medicalrate_only_average',mode='overwrite')
    rate_join_dentalonly.repartition(1).write.parquet(output+'/AverageAcrossStates/dental_only_average',mode='overwrite')
    rate_join_MedicalAndDental.repartition(1).write.parquet(output+'/AverageAcrossStates/medical&dental_combined_average',mode='overwrite')
    
if __name__ == '__main__':
    inputs='america_insurance/parquet_dataset'
    output='america_insurance/s3'
    spark = SparkSession.builder.appName('Health_Insuarnce').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs,output)
