##################################
#Author: Karthik Srinatha (ksa166)
##################################

import pyspark.sql as sparksql
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean
from pyspark.sql import SparkSession, types
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import (OneHotEncoder,StringIndexer, VectorAssembler)


# Schema for stroke data
stroke_schema = types.StructType([
    types.StructField('id', types.IntegerType()),
    types.StructField('gender', types.StringType()),
    types.StructField('age', types.DoubleType()),
    types.StructField('hypertension', types.IntegerType()),
    types.StructField('heart_disease', types.IntegerType()),
    types.StructField('ever_married', types.StringType()),
    types.StructField('work_type', types.StringType()),
    types.StructField('Residence_type', types.StringType()),
    types.StructField('avg_glucose_level', types.DoubleType()),
    types.StructField('bmi', types.StringType()),
    types.StructField('smoking_status', types.StringType()),
    types.StructField('stroke', types.IntegerType()),
])



def prepare_stroke_model(inputs, output):

    #This function aims at preparing a model for stroke prediction

    stroke_data = spark.read.csv(inputs, inferSchema = True, header=True)

    # fill up the missing up values
    data_train = stroke_data.na.fill('No Info', subset=['smoking_status'])

    # filling it with mean found
    data_mean = data_train.select(mean(data_train['bmi'])).collect()

    mean_value_bmi = data_mean[0][0]

    data_train = data_train.na.fill(mean_value_bmi,['bmi'])

    data_train = data_train.limit(600)

    # Converting gender coloum to selectable feature
    indexer_gender_stage = StringIndexer(inputCol='gender', outputCol = 'indexer_gender')
    encoder_gender_stage = OneHotEncoder(inputCol='indexer_gender', outputCol = 'vec_gender')

    # Converting ever_married coloum to selectable feature
    indexer_ever_married_stage = StringIndexer(inputCol='ever_married', outputCol = 'indexer_ever_married')
    encoder_ever_married_stage= OneHotEncoder(inputCol='indexer_ever_married', outputCol = 'vec_ever_married')

    # Converting work_type  coloum to selectable feature
    indexer_work_type_stage = StringIndexer(inputCol='work_type', outputCol = 'indexer_work_type')
    encoder_work_type_stage = OneHotEncoder(inputCol='indexer_work_type', outputCol = 'vec_work_type')

    # Converting work_type coloum to selectable feature
    indexer_residence_type_stage = StringIndexer(inputCol='Residence_type', outputCol = 'indexer_residence_type')
    encoder_residence_type_stage = OneHotEncoder(inputCol='indexer_residence_type', outputCol = 'vec_residence_type')

    # Converting work_type coloum to selectable feature
    indexer_smoking_status_stage = StringIndexer(inputCol='smoking_status', outputCol = 'indexer_smoking_status')
    encoder_smoking_status_stage = OneHotEncoder(inputCol='indexer_smoking_status', outputCol = 'vec_smoking_status')

    # Using all the features to assembeler 
    model_assembler = VectorAssembler(inputCols=['vec_gender',
     'age',
     'hypertension',
     'heart_disease',
     'vec_ever_married',
     'vec_work_type',
     'vec_residence_type',
     'avg_glucose_level',
     'vec_smoking_status'],outputCol='features')


    # Selected Decisiontreeclassifier for training the model
    classifier = DecisionTreeClassifier(labelCol='stroke',featuresCol='features')

    # Creating pipeline and using all transformations here
    stroke_pipeline = Pipeline(stages=[indexer_gender_stage, indexer_ever_married_stage, indexer_work_type_stage, indexer_residence_type_stage,
                                       indexer_smoking_status_stage, encoder_gender_stage, encoder_ever_married_stage, encoder_work_type_stage,
                                       encoder_residence_type_stage, encoder_smoking_status_stage, model_assembler, classifier])
    
    
    train_data, test_data = data_train.randomSplit([0.6,0.4])

    stroke_model = stroke_pipeline.fit(train_data)

    # Using the model transformation on the created pipeline
    stroke_predictions = stroke_model.transform(test_data)

    # Select (prediction, true label) and compute test error
    stroke_evaluator = MulticlassClassificationEvaluator(labelCol="stroke", predictionCol="prediction", metricName="accuracy")

    # Evaluating the model
    # stroke_acc = stroke_evaluator.evaluate(stroke_predictions)
    # print('A stroke predictor has accuracy: {0:2.2f}%'.format(stroke_acc*100))

    stroke_model.write().overwrite().save(output)


if __name__ == '__main__':

    spark = SparkSession.builder.appName('stroke').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    #This parameter is set because in cluster the default filesystem is hdfs where as in local it is default filesystem
    running_in_cluster = 1

    if running_in_cluster == 0:
        inputs = '/home/karthik/Desktop/project_files/supporting_csv/healthcare-dataset-stroke-data.csv'
        output = '/home/karthik/Desktop/project_files/s3/stroke_model/'
    else:
        inputs =  'america_insurance/supporting_csv/healthcare-dataset-stroke-data/'
        output = 'america_insurance/s3/stroke_model/'

    prepare_stroke_model(inputs, output)