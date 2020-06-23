import sys
import re
import math
import datetime
from cassandra.cluster import Cluster

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, Row, functions

def main(inputs, keyspace, table):
    #read input file 
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).load()
    #calculating the correlation logs
    datapoint = df.groupby('host').agg(functions.count('*').alias('count_req'), functions.sum('bytes').alias('sum_req_bytes')).cache()
    sum_datapoints = datapoint.select(functions.sum(datapoint['count_req']).alias('x'), functions.sum(datapoint['sum_req_bytes']).alias('y'), 
    functions.sum(datapoint['count_req']**2).alias('x_sq'), functions.sum(datapoint['sum_req_bytes']**2).alias('y_sq'), 
    functions.sum(datapoint['count_req'] * datapoint['sum_req_bytes']).alias('xy'))
    clct_datap = sum_datapoints.collect()[0]
    n = datapoint.count()
    #calculating the r estimates
    r = (((n * clct_datap['xy']) - (clct_datap['x'] * clct_datap['y'])) / ((math.sqrt((n * clct_datap['x_sq'])-(clct_datap['x']**2))) * (math.sqrt((n * clct_datap['y_sq'])-(clct_datap['y']**2)))))
    r_sqr = (r**2)
    print('value of r=',r)
    print('value of r^2=',r_sqr)


if __name__ == '__main__':
    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spark = SparkSession.builder.appName('Spark Cassandra correlation logs').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    assert spark.version >= '2.3' # make sure we have Spark 2.3+
    sc = spark.sparkContext
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table    = sys.argv[3]
    main(input_dir,keyspace,table)
