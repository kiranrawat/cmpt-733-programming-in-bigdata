import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('temp range with sql').getOrCreate()
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
    #crating temporary view as we cannot directly perform sql operation on data frames.
    weather.createTempView("weather_one")
    with_qflag = spark.sql("SELECT * FROM weather_one where qflag is null")
    with_qflag.createTempView("with_qflag")
    #separating data in  two tables, one with tmin observation and another with tmax observation
    temp_max = spark.sql("SELECT date, station, value as tmaxx from with_qflag WHERE observation in ('TMAX')")
    temp_max.createTempView("max_df")
    temp_min = spark.sql("SELECT date, station, value as tminn from with_qflag WHERE observation in ('TMIN')")
    temp_min.createTempView("min_df")
    #performing join between small and large views 
    joined_df = spark.sql("SELECT max1.date, max1.station, ((max1.tmaxx-min1.tminn)/10) as range FROM max_df max1 join min_df min1 on max1.station==min1.station and max1.date==min1.date")
    joined_df.createTempView("final_df")
    max_df = spark.sql("SELECT date, max(range) as range FROM final_df GROUP BY date").createOrReplaceTempView("maxDF")
    result_df = spark.sql("SELECT final_df.date, final_df.station, final_df.range FROM final_df JOIN maxDF on (final_df.date == maxDF.date and final_df.range == maxDF.range) order by final_df.date,final_df.station")
    result_df.write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
