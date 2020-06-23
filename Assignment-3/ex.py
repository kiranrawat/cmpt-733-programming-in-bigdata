from pyspark import SparkConf, SparkContext
import sys
import json
import operator
import random

def euler_const(inRdd):
    total_iterations = 0
    for i in range(inRdd+100):
        random.seed()
        sum = 0.0
        while sum < 1:
         sum += random.random()
         total_iterations+= 1
    return total_iterations


def main(inputs):
    #main logic starts here
    samples = int(inputs)
    inRdd = sc.range(0, samples,100,numSlices=8)
    outRdd= inRdd.map(euler_const)
    average = outRdd/samples
    print(average)


if __name__ == '__main__':
    conf = SparkConf().setAppName('Euler constant')
    sc = SparkContext(conf=conf)
    assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
   # output = sys.argv[2]
    main(inputs)