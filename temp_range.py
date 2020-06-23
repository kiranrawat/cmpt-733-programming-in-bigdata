import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('temp range').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+

# add more functions as necessary

def main(inputs,output):
    # main logic starts here
    observation_schema = types.StructType([
    types.StructField('station', types.StringType(), False),
    types.StructField('date', types.StringType(), False),
    types.StructField('observation', types.StringType(), False),
    types.StructField('value', types.IntegerType(), False),
    types.StructField('mflag', types.StringType(), False),
    types.StructField('qflag', types.StringType(), False),
    types.StructField('sflag', types.StringType(), False),
    types.StructField('obstime', types.StringType(), False),
])
    weather = spark.read.csv(inputs, schema=observation_schema) #read input file into a dataframe
    filtered_weather_records = weather.filter(weather.qflag.isNull()).cache() #keep records where qflag is null
    #creating two separate tables here one with tmin obeservation and another with tmax observation
    records_min_temp = filtered_weather_records.filter(filtered_weather_records['observation']=='TMIN').withColumnRenamed('value','tmin')
    minimum = records_min_temp.select(records_min_temp['station'], records_min_temp['tmin'], records_min_temp['date'])
    records_max_temp = filtered_weather_records.filter(filtered_weather_records['observation']=='TMAX').withColumnRenamed('value','tmax')
    maximum = records_max_temp.select(records_max_temp['station'], records_max_temp['tmax'], records_max_temp['date'])
    
    #joining small and large dataframes with join operation
    joined_df = maximum.join(minimum,['date','station'],'inner')
    df_withrange = joined_df.select(joined_df['date'],joined_df['station'],((joined_df['tmax']-joined_df['tmin'])/10).alias('range')).cache()
    df_with_maxrange = df_withrange.groupby(df_withrange['date']).max('range').withColumnRenamed('max(range)','range')
    df_join_withmax = df_withrange.join(df_with_maxrange,['date','range'],'inner').orderBy('date','station')
    df_out = df_join_withmax.select(df_join_withmax['date'],df_join_withmax['station'],df_join_withmax['range'])
    df_out.write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
