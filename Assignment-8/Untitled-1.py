import sys
from kafka import KafkaConsumer
from pyspark.sql import SparkSession,functions


def process(datapoint):
    sum_datapoints = datapoint.select(functions.sum(datapoint[0]).alias('x'), functions.sum(datapoint[1]).alias('y'),
                                      functions.sum(datapoint[0]**2).alias('x_sq'), functions.sum(datapoint[1]**2).alias('y_sq'),
                                      functions.sum(datapoint[0] * datapoint[1]).alias('xy'))
    clct_datap = sum_datapoints.collect()[0]
    n = datapoint.count()
    beta = (((clct_datap['xy']) - (clct_datap['x'] * clct_datap['y'])/n) / ((clct_datap['x_sq'])-(clct_datap['x']**2)/n))
    print(beta)


def main(topic):
    messages = spark.readStream.format('kafka').option('kafka.bootstrap.servers','199.60.17.210:9092,199.60.17.193:9092').option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string'))
    stream = values.writeStream.format('console').outputMode('update').start()
    stream.awaitTermination(3600)


if __name__ == '__main__':
    topic = sys.argv[1]
    spark = SparkSession.builder.appName('data streaming').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    main(topic)
    