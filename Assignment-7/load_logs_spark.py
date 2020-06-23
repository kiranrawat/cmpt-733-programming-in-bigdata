import sys
import re
import datetime
from cassandra.cluster import Cluster
import gzip
import os
from uuid import uuid4

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, types, Row

#keep only values matching with pattern
def match_rdd(line):
    pattern = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    result = re.match(pattern,line)
    if result:
        result = result.groups()
        result = list(result)
        result[1] = datetime.datetime.strptime(result[1], "%d/%b/%Y:%H:%M:%S") 
        result[3] = int(result[3])
        yield tuple(result)


def main(inputs, keyspace, table):
    #read input file 
    text = sc.textFile(inputs).repartition(40) 
    data = text.flatMap(match_rdd)
    selected_fields = data.map(lambda logdata: (Row(host = logdata[0], uid = str(uuid4()), bytes = logdata[3], datetime = logdata[1], path = logdata[2])))
    #converting rdd to df here 
    dframe = selected_fields.toDF()
    dframe.write.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).save()


if __name__ == '__main__':
    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spark = SparkSession.builder.appName('Spark Cassandra code').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    assert spark.version >= '2.3' # make sure we have Spark 2.3+
    sc = spark.sparkContext
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table    = sys.argv[3]
    main(input_dir,keyspace,table)
