##################################
#Author: Rohit Irvisetty (rri)
##################################
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession,types
from pyspark.sql import functions
from schemas import benefits_schema, rates_schema, plan_schema

def main(inputs1,inputs2,output):
    # main logic starts here
   
    benefits=spark.read.parquet(inputs2+'/benefits.snappy.parquet', header=True,inferSchema=True)
    lat_long=spark.read.csv(inputs2+'/statelatlong.csv',header=True,inferSchema=True)
    plan_attributes=spark.read.parquet(inputs2+'/plan-attributes-puf', header=True,schema=plan_schema)
    rate=spark.read.parquet(inputs2+'/rate-puf', header=True,schema=rates_schema)
    rate_select=rate.filter(rate.BusinessYear=='2020')
    # In this Notebook analysis of how aca(affordable care act) has impacted us health insurance is being done
    # The aca(affordable care act) was passed by president obama in 2011, and came to full implementation by 2014 across all states and had a huge impact on us health industy by 2016
    # To see the difference clearly, going to focus more on cancer dataset as copay for cancer treatment is usually high
    
    # 1) Average CoInsurance rate for cancer treatment across states
    benefits_select=benefits.select('CoinsInnTier1','StateCode','BenefitName').filter(benefits.BenefitName=='Infusion Therapy')
    benefits_select=benefits_select.withColumn("CoinsInnTier1",functions.regexp_replace("CoinsInnTier1", "%",""))
    benefits_select=benefits_select.filter(functions.col('CoinsInnTier1').cast("int").isNotNull())
    benefits_select=benefits_select.filter(benefits_select.CoinsInnTier1>0)
    benefits_avg=benefits_select.select('CoinsInnTier1','StateCode').groupBy('StateCode').agg({"CoinsInnTier1": "avg"})
    benefits_avg=benefits_avg.withColumnRenamed('avg(CoinsInnTier1)','avg_CoinsInnTier1')
    benefits_avg.show(100)
    # Joining benefits_avg data with latitude and longitude to plot a map visual
    benefits_avg=benefits_avg.join(lat_long,benefits_avg.StateCode==lat_long.State,"inner")
    benefits_avg.show(100)

    # 2) Are cancer plans getting better or worse after aca?
    benefits_select=benefits.filter(benefits.BenefitName=='Infusion Therapy')
    benefits_select=benefits_select.withColumn("CoinsInnTier1",functions.regexp_replace("CoinsInnTier1", "%",""))
    benefits_select=benefits_select.filter(benefits_select.CoinsInnTier1>=50)
    copay_count=benefits_select.select('CoinsInnTier1','BusinessYear','StateCode').groupBy('BusinessYear','StateCode').count()
    
    copay_count.show(100)

    # 3)What is the range of prices for different coverege levels(Gold,Silver,Catostropic,Bronze)?
    Removing Outliers in Insurance rates
    rate_select=rate_select.filter(rate_select.Age!="Family Option")
    plan_attributes=plan_attributes.withColumn("TEHBInnTier1IndividualMOOP",functions.regexp_replace("TEHBInnTier1IndividualMOOP", ",",""))
    plan_attributes=plan_attributes.withColumn("TEHBInnTier1IndividualMOOP",functions.regexp_replace("TEHBInnTier1IndividualMOOP", "\\$",""))
    plan_attributes=plan_attributes.filter(functions.col('TEHBInnTier1IndividualMOOP').cast("int").isNotNull())
    plan_attributes=plan_attributes.withColumn("TEHBInnTier2IndividualMOOP",functions.regexp_replace("TEHBInnTier2IndividualMOOP", ",",""))
    plan_attributes=plan_attributes.withColumn("TEHBInnTier1IndividualMOOP",functions.regexp_replace("TEHBInnTier2IndividualMOOP", "\\$",""))
    plan_attributes=plan_attributes.filter(functions.col('TEHBInnTier2IndividualMOOP').cast("int").isNotNull())
    plan_attributes=plan_attributes.withColumn("TEHBOutOfNetIndividualMOOP",functions.regexp_replace("TEHBOutOfNetIndividualMOOP", ",",""))
    plan_attributes=plan_attributes.withColumn("TEHBOutOfNetIndividualMOOP",functions.regexp_replace("TEHBOutOfNetIndividualMOOP", "\\$",""))
    plan_attributes=plan_attributes.filter(functions.col('TEHBOutOfNetIndividualMOOP').cast("int").isNotNull())
    plan_attributes_select=plan_attributes.select('TEHBInnTier1IndividualMOOP','TEHBInnTier1IndividualMOOP','TEHBOutOfNetIndividualMOOP','benefit','PlanId','MetalLevel')
    plan_attributes_select=plan_attributes_select.join(rate_select,plan_attributes_select.PlanId==rate_select.PlanId)
    plan_attributes_select.filter(plan_attributes_select.MetalLevel='Bronze').select(IndividualRate).summary().show()
    plan_attributes_select.filter(plan_attributes_select.MetalLevel='Catastrophic').select(IndividualRate).summary().show()
    plan_attributes_select.filter(plan_attributes_select.MetalLevel='Gold').select(IndividualRate).summary().show()
    plan_attributes_select.filter(plan_attributes_select.MetalLevel='High').select(IndividualRate).summary().show()
    plan_attributes_select.filter(plan_attributes_select.MetalLevel='Low').select(IndividualRate).summary().show()
    plan_attributes_select.filter(plan_attributes_select.MetalLevel='Platinum').select(IndividualRate).summary().show()
    plan_attributes_select.filter(plan_attributes_select.MetalLevel='Silver').select(IndividualRate).summary().show()
    # And also repartitioning to 1 file
    benefits_avg.repartition(1).write.parquet(output+'/coinsurace_aca/coinsurace_avg',mode='overwrite')
    copay_count.repartition(1).write.parquet(output+'/coinsurace_aca/coinsurace_years',mode='overwrite')
    
if __name__ == '__main__':
    inputs1='america_insurance/supporting_csv'
    inputs2='america_insurance/parquet_dataset'
    output='america_insurance/s3'
    
    spark = SparkSession.builder.appName('Health_Insuarnce').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs1,inputs2,output)
