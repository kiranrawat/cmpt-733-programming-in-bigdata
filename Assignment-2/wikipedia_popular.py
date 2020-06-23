from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def get_key(kv):
    return kv[0]

def tab_separated(kv):
    return "%s\t%s" % (kv[0],(kv[1][0],kv[1][1]))

text = sc.textFile(inputs)
tuples = text.map(lambda x: x.split()).map(lambda y: (y[0], y[1], y[2], int(y[3]), y[4]))
filtered_tuples = tuples.filter(lambda x : ('en' in x) and ('Main_Page' not in x[2]) and not(x[2].startswith('Special:')))

rdd_pairs = filtered_tuples.map(lambda x: (x[0], (x[3], x[2])))
max_count = rdd_pairs.reduceByKey(max)

outdata = max_count.sortBy(get_key).map(tab_separated)
outdata.saveAsTextFile(output)