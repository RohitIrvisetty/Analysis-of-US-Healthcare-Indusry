import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession,types
from pyspark.sql import functions
import pandas as pd
from pyspark.ml.feature import StandardScaler
from schemas import benefits_schema, rates_schema, plan_schema
from pyspark.ml.feature import StringIndexer, VectorAssembler,SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline,PipelineModel

def create_dummies(df,dummylist):
    for input in dummylist:
        categories=df.select(input).distinct().count()
        exprs=[functions.when(functions.col(input)==category,1)\
        .otherwise(0)\
        .alias(category) for category in categories]
        for index,column in enumerate(exprs):
            df=df.withColumn(inputcol+str(index),column)
    return df

def main(inputs,output):
    insurance_schema = types.StructType([
    types.StructField('ID', types.IntegerType()),
    types.StructField('City_Code', types.StringType()),
    types.StructField('Region_Code', types.IntegerType()),
    types.StructField('Accomodation_Type', types.StringType()),
    types.StructField('Reco_Insurance_Type', types.StringType()),
    types.StructField('Upper_Age', types.IntegerType()),
    types.StructField('Lower_Age', types.IntegerType()),
    types.StructField('Is_Spouse', types.StringType()),
    types.StructField('Health Indicator', types.StringType()),
    types.StructField('Holding_Policy_Duration', types.StringType()),
    types.StructField('Holding_Policy_Type', types.FloatType()),
    types.StructField('Reco_Policy_Cat', types.IntegerType()),
    types.StructField('Reco_Policy_Premium', types.FloatType()),
    types.StructField('Response', types.IntegerType()),
    ])  
    # main logic starts here
    purchase_data=spark.read.csv(inputs+'/purchase_data', header=True,schema=insurance_schema)
    #spark.read.csv(inputs+'/purchase_data.csv', header=True,inferSchema=True).createTempView("purchase_sql")
    purchase_data.show()

    # Preprocessing the data
    # Dropping column that are not necessary and with too many missing values
    purchase_data.withColumnRenamed('Health Indicator','Health_Indicator').createTempView("purchase_sql")
    purchase_data=purchase_data.withColumnRenamed('Health Indicator','Health_Indicator')

    # Filling missing values of rquired columns with mode
    mode=spark.sql("select Health_Indicator,count(Health_Indicator) as count from purchase_sql group by Health_Indicator order by count desc")
    mode=mode.select('Health_Indicator').collect()[0][0]
    print(mode)
    purchase_data=purchase_data.na.fill(value=mode,subset=['Health_Indicator'])
    purchase_data=purchase_data.drop('Holding_Policy_Duration', 'Holding_Policy_Type','Region_Code')
    # Binary encoding
    purchase_data=purchase_data.na.replace('Rented','0')
    purchase_data=purchase_data.na.replace('Owned','1')
    purchase_data=purchase_data.withColumn("Accomodation_Type_I", purchase_data.Accomodation_Type.cast(types.IntegerType()))
    purchase_data=purchase_data.na.replace('Individual','0')
    purchase_data=purchase_data.na.replace('Joint','1')
    purchase_data=purchase_data.withColumn("Reco_Insurance_Type_I", purchase_data.Reco_Insurance_Type.cast(types.IntegerType()))
    purchase_data=purchase_data.na.replace('No','0')
    purchase_data=purchase_data.na.replace('Yes','1')
    purchase_data=purchase_data.withColumn("Is_Spouse_I", purchase_data.Is_Spouse.cast(types.IntegerType()))
    purchase_data=purchase_data.drop('Accomodation_Type','Reco_Insurance_Type','Is_Spouse')
    purchase_data.show()
    purchase_data = purchase_data.toPandas()
    for col in ['City_Code','Health_Indicator','Reco_Policy_Cat']:
        dummies=pd.get_dummies(purchase_data[col], prefix=col)
        purchase_data=pd.concat([purchase_data, dummies], axis=1)

    purchase_data=spark.createDataFrame(purchase_data) 
    x=purchase_data
    # Spliting data training and testing dataset
    x_train,x_test=x.randomSplit([0.75, 0.25])
    #x_train.show()
    # Creating a tuple
    rows_tuple=VectorAssembler(inputCols=['Accomodation_Type_I','Reco_Insurance_Type_I','Upper_Age','Lower_Age','Is_Spouse_I','Reco_Policy_Premium','City_Code_C1','City_Code_C10','City_Code_C11','City_Code_C12','City_Code_C13','City_Code_C14','City_Code_C15','City_Code_C16','City_Code_C17','City_Code_C18','City_Code_C19','City_Code_C2','City_Code_C20','City_Code_C21','City_Code_C22','City_Code_C23','City_Code_C24','City_Code_C25','City_Code_C26','City_Code_C27','City_Code_C28','City_Code_C29','City_Code_C3','City_Code_C30','City_Code_C31','City_Code_C32','City_Code_C33','City_Code_C34','City_Code_C35','City_Code_C36','City_Code_C4','City_Code_C5','City_Code_C6','City_Code_C7','City_Code_C8','City_Code_C9','Health_Indicator_X1','Health_Indicator_X2','Health_Indicator_X3','Health_Indicator_X4','Health_Indicator_X5','Health_Indicator_X6','Health_Indicator_X7','Health_Indicator_X8','Health_Indicator_X9','Reco_Policy_Cat_1','Reco_Policy_Cat_2','Reco_Policy_Cat_3','Reco_Policy_Cat_4','Reco_Policy_Cat_5','Reco_Policy_Cat_6','Reco_Policy_Cat_7','Reco_Policy_Cat_8','Reco_Policy_Cat_9','Reco_Policy_Cat_10','Reco_Policy_Cat_11','Reco_Policy_Cat_12','Reco_Policy_Cat_13','Reco_Policy_Cat_14','Reco_Policy_Cat_15','Reco_Policy_Cat_16','Reco_Policy_Cat_17','Reco_Policy_Cat_18','Reco_Policy_Cat_19','Reco_Policy_Cat_20','Reco_Policy_Cat_21','Reco_Policy_Cat_22'],outputCol='features')
    # Training using GBTRegresor
    classifier=GBTRegressor(featuresCol='features', labelCol='Response')
    pipeline=Pipeline(stages=[rows_tuple, classifier])
    model=pipeline.fit(x_train)
    model.write().overwrite().save('purchase-model')
    predictions=model.transform(x_test)
    
    r2_evaluator=RegressionEvaluator(predictionCol='prediction', labelCol='Response',metricName='r2')
    rmse_evaluator=RegressionEvaluator(predictionCol='prediction', labelCol='Response',metricName='rmse')
    r2=r2_evaluator.evaluate(predictions)
    rmse=rmse_evaluator.evaluate(predictions)
    #print(r2,rmse)
    # Producing Final Output (Customers with a Response Predicion > 0.5)
    # Predicting on the testing dataset
    predictions.select('ID','City_Code','prediction').show(100)
    outdata=predictions.select('ID','prediction').filter(predictions['prediction']>0.5)
    #outdata.show(100)
    # rate_join_medicalonly.coalesce(1)
    # writing files into HDFS Storage
    outdata.repartition(1).write.csv(output+'/Purchase_Prediction/Insurance_Predicion',mode='overwrite')
    
    
if __name__ == '__main__':
    inputs='america_insurance/supporting_csv'
    output='america_insurance/s3'
    spark = SparkSession.builder.appName('Health_Insuarnce').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs,output)
