import sys
from cassandra.cluster import Cluster
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions


#output_line function to change the formatting of output
def output_line(orderkey, price, names):
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)


def main(keyspace,outdir,orderkeys):
    #read the tables here
    orders_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders_parts', keyspace=keyspace).load()
    orders_detail = orders_df.select(orders_df['orderkey'],orders_df['totalprice'],orders_df['part_names'])
    filtered_orders = orders_detail.filter(orders_detail['orderkey'].isin(orderkeys)).sort('orderkey')
    #get the partnames and totalscore for only the orderkeys entered in command line space
    final_result = filtered_orders.rdd.map(lambda line: output_line(line[0],line[1],line[2]))
    #writing data to order_parts table in my keyspace
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
