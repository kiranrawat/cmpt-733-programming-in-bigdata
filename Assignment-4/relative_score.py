from pyspark import SparkConf, SparkContext
import sys
import json
import operator
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary
def add_pairs(t1,t2):
     return tuple(map(operator.add,t1,t2))
    
def main(inputs, output):
    # main logic starts here
    input = sc.textFile(inputs)
    commentdata = input.map(lambda x: json.loads(x)).cache()

    key_values = commentdata.map(lambda x: (x['subreddit'], (1, int(x['score']))))
    sum_tuples = key_values.reduceByKey(add_pairs)
    avg_reddit = sum_tuples.map(lambda x: (x[0],x[1][1]/x[1][0])).filter(lambda x: x[1]>0)   #calculating reddit average
    commentbysub = commentdata.map(lambda c: (c['subreddit'], c))
    combined_rdd = commentbysub.join(avg_reddit)   #joining two rdds
    relative_rdd = combined_rdd.map(lambda obj: (obj[1][0]['score']/obj[1][1],obj[1][0]['author']))
    relative_rdd.sortByKey(ascending= False).map(json.dumps).saveAsTextFile(output)
    

if __name__ == '__main__':
    conf = SparkConf().setAppName('Reddit Relative Score')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
