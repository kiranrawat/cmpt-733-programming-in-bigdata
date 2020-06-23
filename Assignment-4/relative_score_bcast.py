from pyspark import SparkConf, SparkContext
import sys
import json
import operator
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary
def add_pairs(t1,t2):
     return tuple(map(operator.add,t1,t2))

def get_relative_averages(broadcastvar,comment):     
     rel_avg = int(comment['score'])/broadcastvar.value.get(comment['subreddit'])
     return rel_avg

def main(inputs, output):
    # main logic starts here
    input = sc.textFile(inputs)
    commentdata = input.map(lambda x: json.loads(x)).cache()

    key_values = commentdata.map(lambda x: (x['subreddit'], (1, int(x['score']))))
    sum_tuples = key_values.reduceByKey(add_pairs)
    avg_reddit = sum_tuples.map(lambda x: (x[0],x[1][1]/x[1][0])).filter(lambda x: x[1]>0)   #calculating reddit average
    #broadcasting the avg_reddit
    broadcast_avg_reddit = sc.broadcast(dict(avg_reddit.collect()))
    relative_avg_rdd = commentdata.map(lambda comment: ((get_relative_averages(broadcast_avg_reddit,comment)), comment['author']))
    relative_avg_rdd.sortByKey(ascending= False).map(json.dumps).saveAsTextFile(output)
    

if __name__ == '__main__':
    conf = SparkConf().setAppName('Broadcast Relative Score')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
