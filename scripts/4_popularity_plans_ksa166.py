##################################
#Author: Karthik Srinatha (ksa166)
##################################


import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import regexp_replace
from schemas import updated_plan_schema



def popular_plans(inputs, output_plans, output_source):

	#This function finds popular plans and source providers

	# Reading the dataframe
	plans_df = spark.read.parquet(inputs, header = True, schema = updated_plan_schema)

	plans_df.createOrReplaceTempView("PlanAttributes")

	#Selecting the required fields
	plans_df_sel = plans_df.select(plans_df.PlanMarketingName, plans_df.StateCode)

	#Analysing the popular plans
	plans_df_upd = plans_df_sel.groupby(plans_df_sel.PlanMarketingName).agg(functions.count(plans_df_sel.PlanMarketingName).alias('Popularity'))

	#Ordering by popularity
	output_df_plans = plans_df_upd.select(plans_df_upd.PlanMarketingName,plans_df_upd.Popularity).orderBy(plans_df_upd.Popularity, ascending=False)

	# Cleaning coloum names
	output_df_plans = output_df_plans.withColumn('PlanMarketingName', regexp_replace('PlanMarketingName', '\?', ''))

	#Selecting the required fields
	source_df = plans_df.select(plans_df.PlanMarketingName, plans_df.SourceName)

	#Analysing the popular source providers
	source_df_up = source_df.groupby(source_df.SourceName).agg(functions.count(source_df.PlanMarketingName).alias('Popularity'))

	#Ordering by popularity
	output_df_source = source_df_up.select(source_df_up.SourceName,source_df_up.Popularity).orderBy(source_df_up.Popularity, ascending=False)

	#Writing the results in parquet
	output_df_plans.repartition(1).write.mode('overwrite').parquet(output_plans)

	#Writing the results in parquet
	output_df_source.repartition(1).write.mode('overwrite').parquet(output_source)



if __name__ == '__main__':

	spark = SparkSession.builder.appName('queries').getOrCreate()
	assert spark.version >= '3.0' # make sure we have Spark 3.0+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext

	#This parameter is set because in cluster the default filesystem is hdfs where as in local it is default filesystem

	running_in_cluster = 1

	if running_in_cluster == 0:
		inputs = '/home/karthik/Desktop/project_files/parquet_dataset/plan-attributes-puf'
		output_plans = '/home/karthik/Desktop/project_files/s3/plan-attributes-puf/'
		output_source = '/home/karthik/Desktop/project_files/s3/plan-attributes-puf_source/'
	else:
		inputs = 'america_insurance/parquet_dataset/plan-attributes-puf'
		output_plans = 'america_insurance/s3/plan-attributes-puf/'
		output_source = 'america_insurance/s3/plan-attributes-puf_source/'

	popular_plans(inputs, output_plans, output_source)