import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3' # make sure we have Spark 2.3+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from colour_tools import colour_schema, rgb2lab_query, plot_predictions


def main(inputs):
    data = spark.read.csv(inputs, schema=colour_schema)
    train, validation = data.randomSplit([0.75, 0.25], seed=110) #use seed here 
    train = train.cache()
    validation = validation.cache()

    word_indexer = StringIndexer(inputCol="word", outputCol="target", handleInvalid="error",stringOrderType="frequencyDesc")
    classifier = MultilayerPerceptronClassifier(maxIter=100, featuresCol="features",labelCol="target", layers=[3, 25, 25], seed=120)
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="target")

    #Evaluating RGB color space
    rgb_assembler = VectorAssembler(inputCols=['R', 'G', 'B'], outputCol="features")
    rgb_pipeline = Pipeline(stages=[rgb_assembler, word_indexer, classifier])
    rgb_model = rgb_pipeline.fit(train)
    rgb_validation = rgb_model.transform(validation)
    score = evaluator.evaluate(rgb_validation, {evaluator.metricName: "accuracy"})

    print('Validation score for RGB model: %g' % (score, ))
    plot_predictions(rgb_model, 'RGB', labelCol='target')

    #Evaluating LAB color space
    rgb_to_lab_query = rgb2lab_query(passthrough_columns=['word'])
    sqlTrans = SQLTransformer(statement=rgb_to_lab_query)
    lab_assembler = VectorAssembler(inputCols=['labL', 'labA', 'labB'], outputCol="features")
    lab_pipeline = Pipeline(stages=[sqlTrans,lab_assembler, word_indexer, classifier])
    lab_model = lab_pipeline.fit(train)
    lab_validation = lab_model.transform(validation)
    labscore = evaluator.evaluate(lab_validation, {evaluator.metricName: "accuracy"})

    print('Validation score for LAB model: %g' % (labscore, ))
    plot_predictions(lab_model, 'LAB', labelCol='word')


if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)

