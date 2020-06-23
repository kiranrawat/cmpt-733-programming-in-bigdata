from pyspark import SparkConf, SparkContext
import sys
import json
import operator
import random

def euler_const(num):
    total_iterations = 0
    random.seed()
    for i in range(num):
       sum = 0.0
       while sum < 1:
          sum += random.random()
          total_iterations += 1
    return total_iterations


def main(inputs):
    #main logic starts here
    samples = int(inputs)
    p = 200
    iterations = [samples//p]*p
    inRdd = sc.parallelize(iterations, numSlices=40)
    eulerRdd= inRdd.map(euler_const)
    average = eulerRdd.reduce(operator.add)/samples
    print(average)


if __name__ == '__main__':
    conf = SparkConf().setAppName('Euler constant')
    sc = SparkContext(conf=conf)
    assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    main(inputs)