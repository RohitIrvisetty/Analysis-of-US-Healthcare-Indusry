##################################
#Author: Karthik Srinatha (ksa166)
##################################

import sys
import datetime
from pyspark.sql import SparkSession, types
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator


stroke_predict_schema = types.StructType([
    types.StructField('gender', types.StringType()),
    types.StructField('age', types.DoubleType()),
    types.StructField('hypertension', types.IntegerType()),
    types.StructField('heart_disease', types.IntegerType()),
    types.StructField('ever_married', types.StringType()),
    types.StructField('work_type', types.StringType()),
    types.StructField('Residence_type', types.StringType()),
    types.StructField('avg_glucose_level', types.DoubleType()),
    types.StructField('bmi', types.DoubleType()),
    types.StructField('smoking_status', types.StringType()),

])

def stroke_predictor(model):

    #Datapoint example
    data_point = ('Female', 23.0, 1, 1, 'Yes', 'Private', 'Urban', 174.96, 40.6, 'smokes')

    #Creating the dataframe for those two points
    data_points_df = spark.createDataFrame([data_point], stroke_predict_schema)

    #Loading the trained model and predicting 
    predictions = PipelineModel.load(model).transform(data_points_df)
    
    #Getting the prediction
    output = predictions.select("prediction").collect()
    print(output[0][0])


if __name__ == '__main__':

    spark = SparkSession.builder.appName('stroke_predictor').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    #This parameter is set because in cluster the default filesystem is hdfs where as in local it is default filesystem
    running_in_cluster = 1

    if running_in_cluster == 0:
        model = '/home/karthik/Desktop/project_files/s3/stroke_model/'
    else:
        model = 'america_insurance/s3/stroke_model/'

    stroke_predictor(model)
