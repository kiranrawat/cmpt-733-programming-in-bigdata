import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax model for tmax tomorrow').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel

def test_model(model_file):
    #get the data
    inputs = spark.createDataFrame([('sfu','2018-11-12',49.2771,-122.9146,330,12.0),
                                    ('sfu','2018-11-13',49.2771,-122.9146,330,1.0)],
                                   ['station', 'date', 'latitude', 'longitude', 'elevation','tmax'])

    #load the model
    model = PipelineModel.load(model_file)
    
    #use the model to make predictions
    predictions = model.transform(inputs)
    predict = predictions.select('prediction').take(1)[0][0]
    
    # evaluate the predictions
    print('Predicted tmax tomorrow:', predict)

if __name__ == '__main__':
    model_file = sys.argv[1]
    test_model(model_file)

