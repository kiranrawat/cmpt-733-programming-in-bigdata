import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax model trainer').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import PipelineModel


tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def train_model(model_file, inputs): 
    # get the data
    train_tmax = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = train_tmax.randomSplit([0.75, 0.25], seed=110)
   
    #query ="SELECT station,date, dayofyear(date) as doy, latitude, longitude, elevation,tmax  FROM __THIS__"
    
    query = """SELECT today.station, dayofyear(today.date) as doy, today.latitude, today.longitude, today.elevation, today.tmax, yesterday.tmax AS yesterday_tmax FROM __THIS__ as today INNER JOIN __THIS__ as yesterday ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station"""
    
    #weather_assembler = VectorAssembler(inputCols=['latitude','longitude','elevation', 'doy'], outputCol="features")
    weather_assembler = VectorAssembler(inputCols=['latitude','longitude','elevation', 'doy', 'yesterday_tmax'], outputCol="features")
    regressor =  GBTRegressor(maxIter=50,maxDepth=5,featuresCol="features",labelCol="tmax")
    transquery = SQLTransformer(statement=query)
    pipeline = Pipeline(stages=[transquery,weather_assembler,regressor])
    model = pipeline.fit(train)
    model.write().overwrite().save(model_file)
 
    # use the model to make predictions
    predictions = model.transform(validation)
    #predictions.show()
    
    # evaluate the predictions
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)

    print('r2 =', r2)
    print('rmse =', rmse)

if __name__ == '__main__':
    inputs = sys.argv[1]
    model_file = sys.argv[2]
    train_model(model_file, inputs)

