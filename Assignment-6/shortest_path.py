from pyspark import SparkConf, SparkContext
import sys

# Run only if python above v3.5
assert sys.version_info >= (3, 5) 

# add more functions as necessary
def min_distance(first_node, second_node):
        if first_node[1] <= second_node[1]:
            return (first_node[0], first_node[1])
        else:
            return (second_node[0], second_node[1])

def main(inputs, output, source, destination):
    # main logic starts here
    text = sc.textFile(inputs)
    sides = text.map(lambda line: line.split(":"))
    split_sides = sides.map(lambda edge : (int(edge[0]), list(map(int, edge[1].split())))).flatMapValues(lambda value : value)
    
    givenPath = sc.parallelize([(int(source), ('',0))])
    #forming path iterations
    for k in range(6):
        filteredPath = split_sides.join(givenPath.filter(lambda dist: dist[1][1] == k))
        routes = filteredPath.map(lambda x: (x[1][0], (x[0], x[1][1][1]+1)) )
        givenPath = givenPath.union(routes).reduceByKey(min_distance)
        givenPath.saveAsTextFile(output + '/iter-' + str(k))
    compltPath = [int(destination)]

    #performing the backtracing
    while destination!= source and destination != '':
        backTrackVal = givenPath.lookup(int(destination))
        if backTrackVal==[]:
            break
        else:
            destination = backTrackVal[0][0]
            compltPath.append(destination)

    compltPath = compltPath[::-1]
    compltPath = sc.parallelize(compltPath)
    compltPath.saveAsTextFile(output + '/path')


if __name__ == '__main__':
    conf = SparkConf().setAppName('Shortest Path Algorithm')
    sc = SparkContext(conf=conf)
    # Spark 2.3+ is required.
    assert sc.version >= '2.3' 
    # fetch arguments and call main routine 
    input_file = sys.argv[1] +'/links-simple-sorted.txt'
    output = sys.argv[2]
    source = sys.argv[3]
    destination = sys.argv[4]
    main(input_file, output, source, destination)

