# -*- coding: utf-8 -*-
"""
Created on Tue Sep 25 18:23:48 2018

@author: Krishna Chatanya Gopaluni
"""

from pyspark import SparkConf, SparkContext
import sys
import random as rand 
assert sys.version_info >= (3, 5)

#samples = 100000000 #sys.argv[1]
#conf = SparkConf()
#conf.setMaster('local')
#conf.setAppName('euler')
#sc = SparkContext(conf=conf)


def main(samples):
    total_iterations=0
    for x in range (1,samples):
        sum=0.0
        while sum <= 1:
            sum +=rand.random()
            total_iterations+=1
    print('ans',total_iterations/samples)
        
        
    
if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    conf.setMaster('local')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3' 
    samples = 100000#sys.argv[1]
    main(samples)
    
    


