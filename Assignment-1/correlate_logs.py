import sys
import re
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, Row
spark = SparkSession.builder.appName('correlation logs').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+

sc = spark.sparkContext

def match_rdd(line):
    pattern = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    result = re.match(pattern,line)
    if result:
        result = result.groups()
        result = list(result)
        result[3] = int(result[3])
        yield tuple(result)

def main(inputs):
    # main logic starts here
    logs_schema = types.StructType([
    types.StructField('hostname', types.StringType(), False),
    types.StructField('date', types.StringType(), False),
    types.StructField('reqpath', types.StringType(), False),
    types.StructField('transbytes', types.IntegerType(), False),
])

    text = sc.textFile(inputs)
    data = text.flatMap(match_rdd)
    selected_fields = data.map(lambda logdata: (Row(hostname = logdata[0], date = logdata[1], reqpath = logdata[2], transbytes = logdata[3])))
    dframe = selected_fields.toDF(logs_schema)
    datapoint = dframe.groupby('hostname').agg(functions.count('*').alias('count_req'), functions.sum('transbytes').alias('sum_req_bytes')).cache()
    sum_datapoints = datapoint.select(F.sum(datapoint['count_req']).alias('x'), functions.sum(datapoint['sum_req_bytes']).alias('y'), functions.sum(datapoint['count_req']**2).alias('x_sq'), functions.sum(datapoint['sum_req_bytes']**2).alias('y_sq'), functions.sum(datapoint['count_req'] * datapoint['sum_req_bytes']).alias('xy'))
    clct_datap = sum_datapoints.collect()[0]
    n = clct_datap.count()
    r = (((n * clct_datap['xy']) - (clct_datap['x'] * clct_datap['y'])) / ( (math.sqrt((n * clct_datap['x_sq'])-(clct_datap['x']**2))) * (math.sqrt((n * clct_datap['y_sq'])-(clct_datap['y']**2)))))
    r_sqr = math.sqrt(r)
    print('value of r=',r)
    print('value of r^2=',r_sqr)

if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)

