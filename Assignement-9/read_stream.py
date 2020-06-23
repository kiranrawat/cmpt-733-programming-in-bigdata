import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from kafka import KafkaConsumer
from pyspark.sql import SparkSession,functions

spark = SparkSession.builder.appName('data streaming').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3' # make sure we have Spark 2.3+

def main(topic):
    messages = spark.readStream.format('kafka').option('kafka.bootstrap.servers','199.60.17.210:9092,199.60.17.193:9092').option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string'))
    datapoints = values.select(functions.split(values['value']," ").alias("val"))
    each_pts = datapoints.select((datapoints['val'][0]).cast('float').alias("x"),
                                 (datapoints['val'][1]).cast('float').alias("y"),
                                 functions.lit(1).alias("n"))

    #calculating the sum of datapoints to put them in formulas to calculate intercept and slope
    sum_pts = each_pts.select(functions.sum(each_pts[0]).alias('sum(x)'), functions.sum(each_pts[1]).alias('sum(y)'),
                              functions.sum(each_pts[0] * each_pts[1]).alias('sum(xy)'),functions.sum(each_pts[0]**2).alias('sum(x2)'),
                              functions.sum(each_pts['n']).alias('sum(n)'))
    
    #calculating intercept and slope from data streaming and adding both as a column to dataframe
    finaldf = sum_pts.withColumn('slope',(((sum_pts['sum(xy)']) - (sum_pts['sum(x)'] * sum_pts['sum(y)'])/sum_pts['sum(n)']) / ((sum_pts['sum(x2)'])-(sum_pts['sum(x)']**2)/sum_pts['sum(n)'])))
    result = finaldf.withColumn('intercept',((finaldf['sum(y)'])/finaldf['sum(n)'] - (finaldf['slope'] * (finaldf['sum(x)']))/finaldf['sum(n)']))
    
    #select intercept and slope from dataframe
    int_slope_df = result.select('slope', 'intercept')
    stream = int_slope_df.writeStream.format('console').outputMode('update').start()
    stream.awaitTermination(600)


if __name__ == '__main__':
    topic = sys.argv[1]
    main(topic)   
