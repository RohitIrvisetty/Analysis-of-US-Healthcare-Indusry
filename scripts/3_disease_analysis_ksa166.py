##################################
#Author: Karthik Srinatha (ksa166)
##################################

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types



def disease_program_analysis(csv, inputs, output):

	# The goal of the program is to check weather the plans after 2018 has focused on the below mentioned most searched diseases

	#Diseases that has been searched till 2017 according to google from their analytics
	values = ['cancer', 'cardiovascular', 'stroke', 'depression', 'rehab', 'vaccine', 'diarrhea', 'obesity', 'diabetes']

	#Years data available in supporting csv
	years = ['2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017']

	#Reading csv
	usa_df = spark.read.csv(csv,  header = True)

	#Processing for calculation
	drop_list = []
	for dise in values:
		cols = []
		for year in years:
			coloum_name = year + '+' + dise
			cols.append(coloum_name)
			drop_list.append(coloum_name)
		usa_df = usa_df.withColumn(dise, sum(usa_df[col] for col in cols))

	#Dropping unwanted coloums
	usa_df = usa_df.drop(*drop_list)
	usa_df = usa_df.drop('dma', 'geoCode')

	# Aggreating the values
	usa_df = usa_df.agg({'cancer': 'sum', 'cardiovascular': 'sum', 'stroke': 'sum', 'depression': 'sum', 'rehab': 'sum', 'vaccine': 'sum',
				   'diarrhea': 'sum',  'obesity': 'sum',  'diabetes': 'sum'})

	#Renaming coloumns
	usa_df = usa_df.withColumnRenamed("sum(depression)","depression")
	usa_df = usa_df.withColumnRenamed("sum(diabetes)","diabetes")
	usa_df = usa_df.withColumnRenamed("sum(cardiovascular)","cardiovascular")
	usa_df = usa_df.withColumnRenamed("sum(vaccine)","vaccine")
	usa_df = usa_df.withColumnRenamed("sum(stroke)","stroke")
	usa_df = usa_df.withColumnRenamed("sum(obesity)","obesity")
	usa_df = usa_df.withColumnRenamed("sum(rehab)","rehab")
	usa_df = usa_df.withColumnRenamed("sum(cancer)","cancer")
	usa_df = usa_df.withColumnRenamed("sum(diarrhea)","diarrhea")

	#Constructing the required df
	usa_df = [{"name":i,"value":usa_df.select(i).collect()[0][0]} for i in usa_df.columns ]

	usa_df_changed = sc.parallelize(usa_df).toDF()

	#Ordering
	usa_df_changed = usa_df_changed.orderBy(usa_df_changed.value, ascending=False)

	#Loaded data for checking
	check_df = spark.read.parquet(inputs, header = True)

	check = check_df.where(check_df.DiseaseManagementProgramsOffered.isNotNull())

	# As DiseaseManagementProgramsOffered coloum is a desprective coloum, each disease calculations has been done seperately

	df = check.select('DiseaseManagementProgramsOffered').cache()

	cancer_df = df.filter(df.DiseaseManagementProgramsOffered.contains('Cancer'))

	cancer_count = (cancer_df.count() / df.count()) * 100

	heart_df = df.filter(df.DiseaseManagementProgramsOffered.contains('Heart Disease'))

	heart_count = (heart_df.count() / df.count()) * 100

	storke_df = df.filter(df.DiseaseManagementProgramsOffered.contains('Stroke'))

	storke_count = (storke_df.count() / df.count()) * 100

	depression_df = df.filter(df.DiseaseManagementProgramsOffered.contains('Depression'))

	depression_count = (depression_df.count() / df.count()) * 100

	rehab_df = df.filter(df.DiseaseManagementProgramsOffered.contains('Rehab'))

	rehab_count = (rehab_df.count() / df.count()) * 100

	vaccine_df = df.filter(df.DiseaseManagementProgramsOffered.contains('Vaccine'))

	vaccine_count = (vaccine_df.count() / df.count()) * 100

	diarrhea_df = df.filter(df.DiseaseManagementProgramsOffered.contains('Diarrhea'))

	diarrhea_count =  (diarrhea_df.count() / df.count()) * 100

	obesity_df = df.filter(df.DiseaseManagementProgramsOffered.contains('High Blood Pressure & High Cholesterol'))

	obesity_count =  (obesity_df.count() / df.count()) * 100

	diabetes_df = df.filter(df.DiseaseManagementProgramsOffered.contains('Diabetes'))

	diabetes_count = (diabetes_df.count() / df.count()) * 100

	#Constructing the dataframe

	cdf_schema = types.StructType([
	types.StructField('name', types.StringType()),
	types.StructField('coverage_percent', types.DoubleType())
	])

	calculated_data = [("cancer", cancer_count),
		("cardiovascular", heart_count),
		("stroke", storke_count),
		("depression", depression_count),
		("rehab", rehab_count),
		("diarrhea", diarrhea_count),
		("obesity", obesity_count),
		("diabetes", diabetes_count)
	]

	cdf = spark.createDataFrame(data = calculated_data, schema=cdf_schema)

	#Renaming to avoid ambiguity
	cdf = cdf.withColumnRenamed('name', 'disease')

	#Joining for comparing
	final_df = usa_df_changed.join(cdf, cdf.disease ==  usa_df_changed.name,"inner")

	final_df = final_df.drop('name')

	#Selecting the required fields
	output_df = final_df.select(final_df.disease, final_df.value, final_df.coverage_percent).orderBy(final_df.coverage_percent, ascending=True)
	output_df.show()
	output_df.repartition(1).write.mode('overwrite').parquet(output)



if __name__ == '__main__':

	spark = SparkSession.builder.appName('queries').getOrCreate()
	assert spark.version >= '3.0' # make sure we have Spark 3.0+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext

	#This parameter is set because in cluster the default filesystem is hdfs where as in local it is default filesystem

	running_in_cluster = 1

	if running_in_cluster == 0:
		csv = "/home/karthik/Desktop/project_files/supporting_csv/RegionalInterestByConditionOverTime.csv"
		inputs = '/home/karthik/Desktop/project_files/parquet_dataset/plan-attributes-puf'
		output = '/home/karthik/Desktop/project_files/s3/us_analysis/'
	else:
		csv =  'america_insurance/supporting_csv/RegionalInterestByConditionOverTime/'
		inputs = 'america_insurance/parquet_dataset/plan-attributes-puf'
		output = 'america_insurance/s3/us_analysis/'

	disease_program_analysis(csv, inputs, output)