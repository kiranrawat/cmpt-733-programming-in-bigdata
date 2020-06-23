import sys
from cassandra.cluster import Cluster
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession,functions

def main(keyspace,outkeyspace):
    #read the tables here
    orders_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders', keyspace=keyspace).load()
    lineitem_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='lineitem', keyspace=keyspace).load()
    part_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='part', keyspace=keyspace).load()
    #join the order dataframe and lineitem dataframe
    df_one = orders_df.join(lineitem_df,['orderkey'],'inner')
    #join the above resulted df and the part df
    df_two = df_one.join(part_df,['partkey'],'inner')
    #using collect_set to get the part names in a single line for each orderkey
    partnames = df_two.groupBy('orderkey').agg(functions.collect_set('name').alias('part_names'))
    result = partnames.join(orders_df,'orderkey','inner')
    #writing data to order_parts table in my keyspace
    result.write.format("org.apache.spark.sql.cassandra").options(table='orders_parts', keyspace=outkeyspace).save()


if __name__ == '__main__':
    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spark = SparkSession.builder.appName('Spark Cassandra code').config(
                                         'spark.cassandra.connection.host',
                                         ','.join(cluster_seeds)).config(
                                         'spark.dynamicAllocation.maxExecutors', 16).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    assert spark.version >= '2.3' # make sure we have Spark 2.3+
    sc = spark.sparkContext
    keyspace = sys.argv[1]
    outkeyspace = sys.argv[2]
    main(keyspace,outkeyspace)

