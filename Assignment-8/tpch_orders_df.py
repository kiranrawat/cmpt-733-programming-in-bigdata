import sys
from cassandra.cluster import Cluster
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession,functions

#output_line function to change the formatting of output
def output_line(orderkey, price, names):
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)


def main(keyspace,outdir,orderkeys):
    #read the tables here
    orders_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders', keyspace=keyspace).load()
    lineitem_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='lineitem', keyspace=keyspace).load()
    part_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='part', keyspace=keyspace).load()
    df_one = orders_df.join(lineitem_df,['orderkey'],'inner')
    #join and select the columns from 
    df_two = df_one.join(part_df,['partkey'],'inner')
    part_names = df_two.select(df_two['orderkey'],df_two['totalprice'],df_two['name'])
    #select the orders based on order keys provided in command line argument
    filtered_orders = part_names.filter(part_names['orderkey'].isin(orderkeys))
    result = filtered_orders.groupBy('orderkey','totalprice').agg(functions.collect_set('name')).orderBy('orderkey')
    #converting the dataframe in RDD and changing the output in expected format
    #result.explain()
    final_result = result.rdd.map(lambda line: output_line(line[0],line[1],line[2]))
    final_result.saveAsTextFile(outdir)


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
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    main(keyspace,outdir,orderkeys)
