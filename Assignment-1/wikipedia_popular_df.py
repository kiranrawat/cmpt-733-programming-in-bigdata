import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('wikipedia popular').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+

'''
alternate way of regsitering the udf
@functions.udf(returnType=types.StringType())
'''
  
def path_to_hour(path):
    last_col = path.split('/')[-1]
    full_date_time = last_col.split('-',1)[-1]
    if ".gz" not in full_date_time:
        hour = full_date_time[:-4]
    else:
        hour = full_date_time[:-7]
    return hour


def main(inputs,output):
    # main logic starts here
    wikipedia_schema = types.StructType([
    types.StructField('type', types.StringType(), False),
    types.StructField('title', types.StringType(), False),
    types.StructField('views', types.IntegerType(), False),
    types.StructField('size', types.LongType(), False),
]) 
    wikipedia = spark.read.csv(inputs, schema=wikipedia_schema, sep=' ').withColumn('filename', functions.input_file_name())   #read wikipedia input file separated by space
    filtered_wiki_records = wikipedia.where((wikipedia.type == "en") & (~(wikipedia.title.startswith('Special:'))) & (wikipedia.title != "Main_Page")).cache()
    #registering the udf
    date_time_udf = functions.udf(path_to_hour, types.StringType())
    wiki_records_per_hour = filtered_wiki_records.withColumn('hour',date_time_udf(filtered_wiki_records.filename)).cache()
    #find the max page views per hour
    max_hit_counts = wiki_records_per_hour.groupby('hour').max('views')
    #broadcast the small dataframe
    max_hit_counts = functions.broadcast(max_hit_counts)
    #forming my conditions for join 
    condition = [wiki_records_per_hour.views == max_hit_counts['max(views)'],(wiki_records_per_hour.hour == max_hit_counts.hour)]
    joined_df = max_hit_counts.join(wiki_records_per_hour,condition).select(wiki_records_per_hour.hour,wiki_records_per_hour.title,wiki_records_per_hour.views).sort(wiki_records_per_hour.hour).coalesce(1)
    joined_df.write.json(output,mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
