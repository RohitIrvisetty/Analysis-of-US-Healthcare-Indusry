

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import regexp_replace
from schemas import updated_plan_schema, updated_benefits_schema

spark = SparkSession.builder.appName('queries').getOrCreate()
assert spark.version >= '3.0' # make sure we have Spark 3.0+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def benefits_analysis(inputs, inputs_ben, output_top , output_bot):

    # Reading the dataframe
    plans_df = spark.read.parquet(inputs, header = True, schema = updated_plan_schema)# Selecting the required fields

    # Selecting the required fields
    plans_df_sel = plans_df.select(plans_df.StandardComponentId, plans_df.PlanMarketingName)

    # Removing the outlayers in the data 
    array = ['Individual', 'SHOP (Small Group)']
    plans_df_sel = plans_df_sel.filter(plans_df_sel.PlanMarketingName.isin(array) == False)

    # Getting Popular plans first
    plans_df_ck = plans_df_sel.groupby(plans_df_sel.PlanMarketingName).agg(functions.count(plans_df_sel.PlanMarketingName).alias('Popularity'))

    # Required ordering
    output_df_ck = plans_df_ck.select(plans_df_ck.PlanMarketingName, plans_df_ck.Popularity).orderBy(plans_df_ck.Popularity, ascending=False)
    output_df_ck_2 = plans_df_ck.select(plans_df_ck.PlanMarketingName, plans_df_ck.Popularity).orderBy(plans_df_ck.Popularity, ascending=True)

    # Getting Popular plans first
    plans_df_ck = plans_df_sel.groupby(plans_df_sel.PlanMarketingName).agg(functions.count(plans_df_sel.PlanMarketingName).alias('Popularity'))

    # Required ordering
    output_df_ck = plans_df_ck.select(plans_df_ck.PlanMarketingName, plans_df_ck.Popularity).orderBy(plans_df_ck.Popularity, ascending=False)
    output_df_ck_2 = plans_df_ck.select(plans_df_ck.PlanMarketingName, plans_df_ck.Popularity).orderBy(plans_df_ck.Popularity, ascending=True)

    #Getting top 100 plans
    top_100 = output_df_ck.limit(100)

    #Getting bottom 100 plans
    bottom_100 = output_df_ck_2.limit(100)

    #Having the list so reduce the time in join operatio by filtering
    plan_lists = list(top_100.select('PlanMarketingName').toPandas()['PlanMarketingName'])
    plan_lists_bot = list(bottom_100.select('PlanMarketingName').toPandas()['PlanMarketingName'])

    #Reintiaziling
    plans_df_sel_2 = plans_df_sel

    # Selecting the top 100 Marketing plan to select standard component id through PlanMarketingName
    plans_df_sel = plans_df_sel.filter(plans_df_sel.PlanMarketingName.isin(plan_lists) == True)
    plans_df_sel_bot = plans_df_sel_2.filter(plans_df_sel_2.PlanMarketingName.isin(plan_lists_bot) == True)

    # Now using the best plans standardcomponent id's in both top and bootom benefits analysis
    plans_df_upd = plans_df_sel.groupby(plans_df_sel.PlanMarketingName, plans_df_sel.StandardComponentId).agg(functions.count(plans_df_sel.PlanMarketingName).alias('Popularity'))
    plans_df_upd_bot = plans_df_sel_bot.groupby(plans_df_sel_bot.PlanMarketingName, plans_df_sel_bot.StandardComponentId).agg(functions.count(plans_df_sel_bot.PlanMarketingName).alias('Popularity'))

    # Required ordering
    output_df_plans = plans_df_upd.select(plans_df_upd.PlanMarketingName, plans_df_upd.StandardComponentId, plans_df_upd.Popularity).orderBy(plans_df_upd.Popularity, ascending=False)
    output_df_plans_bot = plans_df_upd_bot.select(plans_df_upd_bot.PlanMarketingName, plans_df_upd_bot.StandardComponentId, plans_df_upd_bot.Popularity).orderBy(plans_df_upd_bot.Popularity, ascending=True)

    # Feteching only the StandardComponentId to avoid join on bigger table and use in for filtering
    std_lists = list(output_df_plans.select('StandardComponentId').toPandas()['StandardComponentId'])
    std_lists_bottom = list(output_df_plans_bot.select('StandardComponentId').toPandas()['StandardComponentId'])

    # Reading the benefits
    bene_df = spark.read.parquet(inputs_ben, header = True, schema = updated_benefits_schema)

    # Selecting the required fields
    bene = bene_df.select(bene_df.StandardComponentId, bene_df.BenefitName)

    #Using the std_lists to obtain shorter df
    bene_df_sel = bene.filter(bene.StandardComponentId.isin(std_lists) == True)
    bene_df_sel_bot = bene.filter(bene.StandardComponentId.isin(std_lists_bottom) == True)

    # Joining both dfs to obtain benefits
    joined_df = output_df_plans.join(bene_df_sel, output_df_plans.StandardComponentId ==  bene_df_sel.StandardComponentId, "inner")
    joined_df_bot = output_df_plans_bot.join(bene_df_sel_bot, output_df_plans_bot.StandardComponentId ==  bene_df_sel_bot.StandardComponentId, "inner")

    # Calculating the benefit popular
    jn_df = joined_df.groupby(joined_df.BenefitName).agg(functions.count(joined_df.BenefitName).alias('Benefit_Popularity'))
    jn_df_bot = joined_df_bot.groupby(joined_df_bot.BenefitName).agg(functions.count(joined_df_bot.BenefitName).alias('Benefit_Popularity'))

    # Selecting the required fields
    benefits = jn_df.select(jn_df.BenefitName, jn_df.Benefit_Popularity).orderBy(jn_df.Benefit_Popularity, ascending=False)
    benefits_bottom = jn_df_bot.select(jn_df_bot.BenefitName, jn_df_bot.Benefit_Popularity).orderBy(jn_df_bot.Benefit_Popularity, ascending=True)

    # Limit to 50
    benefits = benefits.limit(50)
    benefits_bot = benefits_bottom.limit(50)

    # Cleaning the coloum
    benefits_bot = benefits_bot.withColumn('BenefitName', regexp_replace('BenefitName', '\\t', ''))

    #Writing the results in parquet
    benefits.repartition(1).write.mode('overwrite').parquet(output_top)

    #Writing the results in parquet
    benefits_bot.repartition(1).write.mode('overwrite').parquet(output_bot)

if __name__ == '__main__':

    spark = SparkSession.builder.appName('queries').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    inputs = 'america_insurance/parquet_dataset//plan-attributes-puf'
    inputs_ben = 'america_insurance/parquet_dataset/benefits-and-cost-sharing-puf'
    output_top = 'america_insurance/s3/bene_top/'
    output_bot = 'america_insurance/s3/bene_bot/'
    
    benefits_analysis(inputs, inputs_ben, output_top , output_bot)
