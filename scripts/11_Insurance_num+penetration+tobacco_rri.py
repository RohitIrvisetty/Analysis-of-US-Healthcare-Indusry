import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession,types
from pyspark.sql import functions
from schemas import benefits_schema, rates_schema, plan_schema


def main(inputs,output):
    # main logic starts here
    rate=spark.read.parquet(inputs+'/rate-puf', header=True,schema=rates_schema)
    plan_attributes=spark.read.parquet(inputs+'/plan-attributes-puf', header=True,schema=plan_schema)
    benefits=spark.read.parquet(inputs+'/benefits-and-cost-sharing-puf', header=True,schema=benefits_schema)
    lat_long=spark.read.csv('america_insurance/supporting_csv/statelatlong',header=True,inferSchema=True)
    #rate.show()
    rate_select=rate.select('BusinessYear','statecode','individualrate','planid','IndividualTobaccoRate','Tobacco')
    plan_attributes_select=plan_attributes.select('planid','dentalonlyplan')
    plan_attributes_select=plan_attributes_select.withColumnRenamed('planid','Planid1')
    Benefits_select=benefits.select('StateCode','BenefitName')
    # Number of US states included in the dataset
    #print(rate_select.select('statecode').distinct().count())
    # How many of years of information is extracted
    #rate_select.select('BusinessYear').distinct().show()
    # joining rates and PlanAttributes table to distinguish medical and dental rates
    # rates table has all the price details, PlanAttributes has information of plan coverage
    rate_join=rate_select.join(plan_attributes_select,(rate_select.planid==functions.substring(plan_attributes_select.Planid1,1,14)),"inner")
    #rate_join.show()

    # How many Insurance plans are available across states in all years
    # Calculating Number of Total Plans
    Num_Plans=rate_join.select('statecode','BusinessYear','planid').groupBy('statecode','BusinessYear').agg({"planid": "count"}).orderBy('count(PlanId)',ascending=False)
    Num_Plans=Num_Plans.withColumnRenamed('count(planid)','TotalPlans')
    #Num_Plans.show()

    # Calculating Number of Dental Only Plans
    Num_DentalPlans=rate_join.filter(rate_join.dentalonlyplan=='Yes').select('statecode','BusinessYear','planid').groupBy('statecode','BusinessYear').agg({"planid": "count"}).orderBy('count(planid)',ascending=False)
    Num_DentalPlans=Num_DentalPlans.withColumnRenamed('statecode','statecode_dental').withColumnRenamed('BusinessYear','BusinessYear_dental')
    Num_DentalPlans=Num_DentalPlans.withColumnRenamed('count(planid)','DentalPlans')
    #Num_DentalPlans.show(100)

    # Merging Num_Plans and Num_DentalPlans
    Num_PlansCombined=Num_Plans.join(Num_DentalPlans,((Num_Plans.statecode==Num_DentalPlans.statecode_dental)&(Num_Plans.BusinessYear==Num_DentalPlans.BusinessYear_dental)),"inner")
    Num_PlansCombined=Num_PlansCombined.drop('statecode_dental','BusinessYear_dental')
    #Num_PlansCombined.show(100)

    # Analyzing Inurance Benefits Penetration across states
    benefits_penetration=Benefits_select.groupBy('StateCode').agg({"BenefitName": "count"}).orderBy('count(BenefitName)',ascending=False)
    benefits_penetration=benefits_penetration.withColumnRenamed('count(BenefitName)','consumption')
    #benefits_penetration.show(100)
    
    # Joining benefits_penetration data with latitude and longitude to plot a map visual
    benefits_penetration=benefits_penetration.join(lat_long,benefits_penetration.StateCode==lat_long.State,"inner")
    #benefits_penetration.show(100)

    # Average Insurance Rate for Tobacco users
    # For Tobacco Users, a seperate price is column (IndividualTobaccoRate)
    AvgRate_Tobacco=rate_select.filter(rate_select.Tobacco=='Tobacco User/Non-Tobacco User').select('BusinessYear','statecode','IndividualTobaccoRate').groupBy('statecode','BusinessYear').agg({"IndividualTobaccoRate": "avg"})
    AvgRate_Tobacco=AvgRate_Tobacco.withColumnRenamed('avg(IndividualTobaccoRate)','avg_rate_tobacco')
    #AvgRate_Tobacco.show(100)
    # And also repartitioning to 1 file
    Num_Plans.repartition(1).write.parquet(output+'/NumberOfPlans/Num_Plans',mode='overwrite')
    Num_DentalPlans.repartition(1).write.parquet(output+'/NumberOfPlans/Num_DentalPlans',mode='overwrite')
    Num_PlansCombined.repartition(1).write.parquet(output+'/NumberOfPlans/Num_PlansCombined',mode='overwrite')
    benefits_penetration.repartition(1).write.parquet(output+'/NumberOfPlans/Benefits_Penetration',mode='overwrite')
    AvgRate_Tobacco.repartition(1).write.parquet(output+'/NumberOfPlans/Avg_rate_Tobacco',mode='overwrite')
    
if __name__ == '__main__':
    inputs='america_insurance/parquet_dataset'
    output='america_insurance/s3'
    spark = SparkSession.builder.appName('Health_Insuarnce').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs,output)
