#!/bin/bash


#Initial Setup

echo "=================================="
echo "Initail Setup"
echo "=================================="

hdfs dfs -mkdir america_insurance
hdfs dfs -mkdir america_insurance/prepared_dataset
hdfs dfs -mkdir america_insurance/parquet_dataset
hdfs dfs -mkdir america_insurance/s3
hdfs dfs -mkdir america_insurance/supporting_csv/
hdfs dfs -mkdir america_insurance/supporting_csv/RegionalInterestByConditionOverTime/
hdfs dfs -mkdir america_insurance/supporting_csv/healthcare-dataset-stroke-data/
hdfs dfs -mkdir america_insurance/supporting_csv/statelatlong/
hdfs dfs -mkdir america_insurance/supporting_csv/purchase_data/
hdfs dfs -copyFromLocal ../supporting_csv/healthcare-dataset-stroke-data.csv america_insurance/supporting_csv/healthcare-dataset-stroke-data/
hdfs dfs -copyFromLocal ../supporting_csv/RegionalInterestByConditionOverTime.csv america_insurance/supporting_csv/RegionalInterestByConditionOverTime/
hdfs dfs -copyFromLocal ../supporting_csv/statelatlong.csv america_insurance/supporting_csv/statelatlong/
hdfs dfs -copyFromLocal ../supporting_csv/statelatlong.csv america_insurance/supporting_csv/purchase_data/


#Download the dataset and structure it

echo "=================================="
echo "Running scrapper"
echo "=================================="

python3 1_scrapper_ksa166.py


#Copy the prepared_dataset to hdfs

echo "=================================="
echo "Copying Data to hdfs"
echo "=================================="

hdfs dfs -copyFromLocal prepared_dataset/* america_insurance/prepared_dataset/


#Data cleaning and etl

echo "=================================="
echo "Data cleaning and etl"
echo "=================================="

spark-submit 2_etl_parquet_ksa166.py


#Data Transformation for analysis

echo "=================================="
echo "Data Transformation for analysis"
echo "=================================="

spark-submit 3_disease_analysis_ksa166.py

echo "==============================================="
echo "3_disease_analysis_ksa166.py done"
echo "==============================================="

spark-submit 4_popularity_plans_ksa166.py

echo "==============================================="
echo "4_popularity_plans_ksa166.py done"
echo "==============================================="

spark-submit 5_rate_analysis_ytc17.py america_insurance

echo "==============================================="
echo "5_rate_analysis_ytc17.py done"
echo "==============================================="

spark-submit 6_plan_count_ytc17.py america_insurance

echo "==============================================="
echo "6_plan_count_ytc17.py done"
echo "==============================================="

spark-submit 7_crosswalk_analysis_ytc17.py america_insurance

echo "==============================================="
echo "7_crosswalk_analysis_ytc17.py done"
echo "==============================================="

spark-submit 8_family_rate_analysis_ytc17.py america_insurance

echo "==============================================="
echo "8_family_rate_analysis_ytc17.py done"
echo "==============================================="

spark-submit 9_AverageRates_States_rri.py

echo "==============================================="
echo "9_AverageRates_States_rri.py done"
echo "==============================================="

spark-submit 10_AverageRates_Age_rri.py

echo "==============================================="
echo "10_AverageRates_Age_rri.py done"
echo "==============================================="

spark-submit 11_Insurance_num+penetration+tobacco_rri.py

echo "==============================================="
echo "11_Insurance_num+penetration+tobacco_rri.py done"
echo "==============================================="

spark-submit 12_Purchase_Prediction_rri.py

echo "==============================================="
echo "12_Purchase_Prediction_rri.py done"
echo "==============================================="

spark-submit 13_aca_transform_rri.py

echo "==============================================="
echo "13_aca_transform_rri.py done"
echo "==============================================="

spark-submit 14_stroke_trainer_ksa166.py

echo "==============================================="
echo "14_stroke_trainer_ksa166.py done"
echo "==============================================="

spark-submit 15_stroke_predictor_ksa166.py

echo "==============================================="
echo "15_stroke_predictor_ksa166.py done"
echo "==============================================="

spark-submit 17_benefits_ksa166.py

echo "==============================================="
echo "17_benefits_ksa166.py done"
echo "==============================================="

echo "==============================================="
echo "All transformation done"
echo "==============================================="
