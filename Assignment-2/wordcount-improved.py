from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('improved word count')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def words_once(line):
    for w in line.split():
        yield (w.lower(), 1)

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

text = sc.textFile(inputs)
wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
words_withoutpunct = text.flatMap(wordsep.split)
nonemptystring = words_withoutpunct.filter(lambda x : len(x) > 0)
words = nonemptystring.flatMap(words_once)
wordcount = words.reduceByKey(operator.add)

outdata = wordcount.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)