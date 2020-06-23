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
    train, validation = data.randomSplit([0.75, 0.25]) #use seed here
    train = train.cache()
    validation = validation.cache()

    #creating a pipeline to predict RGB colours -> word
    rgb_assembler = VectorAssembler(inputCols=['R', 'G', 'B'], outputCol="features")
    #dataframe1 = rgb_assembler.transform(data)
    word_indexer = StringIndexer(inputCol="word", outputCol="target", handleInvalid="error",stringOrderType="frequencyDesc")
    classifier = MultilayerPerceptronClassifier(featuresCol="features",labelCol="target", layers=[3, 25, 25])
    rgb_pipeline = Pipeline(stages=[rgb_assembler, word_indexer, classifier])
    rgb_model = rgb_pipeline.fit(train)

    #creating an evaluator and score the validation data
    #model_train = rgb_model.transform(train)
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="target")
    rgb_validation = rgb_model.transform(validation)
    score = evaluator.evaluate(rgb_validation, {evaluator.metricName: "accuracy"})

    print('Validation score for RGB model: %g' % (score, ))
    plot_predictions(rgb_model, 'RGB', labelCol='target')

    rgb_to_lab_query = rgb2lab_query(passthrough_columns=['word'])
    # creating a pipeline to predict RGB colours -> word; train and evaluate.
    sqlTrans = SQLTransformer(statement=rgb_to_lab_query)
    labdata = sqlTrans.transform(data)
    ltrain,lvalidation = labdata.randomSplit([0.75, 0.25])
    lrgb_assembler = VectorAssembler(inputCols=['labL', 'labA', 'labB'], outputCol="LAB")
    lword_indexer = StringIndexer(inputCol="word", outputCol="labTarget", handleInvalid="error",stringOrderType="frequencyDesc")
    lclassifier = MultilayerPerceptronClassifier(featuresCol="LAB",labelCol="labTarget", layers=[3, 25, 25])
    lrgb_pipeline = Pipeline(stages=[sqlTrans,lrgb_assembler, lword_indexer, lclassifier])
    lrgb_model = lrgb_pipeline.fit(ltrain)
    #lmodel_train = lrgb_model.transform(ltrain)
    lrgb_validation = lrgb_model.transform(lvalidation)
    print(lrgb_validation.show())
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="labTarget")
    lscore = evaluator.evaluate(lrgb_validation, {evaluator.metricName: "accuracy"})

    print('Validation score for LAB model: %g' % (lscore, ))
    plot_predictions(lrgb_model, 'LAB', labelCol='word')


if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)