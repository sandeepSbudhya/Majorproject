from pyspark.sql import SparkSession
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import StandardScaler, VectorAssembler, StringIndexer


def stringIndexerDf(tdf):
    Indexer = StringIndexer(inputCol='Label', outputCol="class_index").setHandleInvalid("skip")
    classIndexedDf = Indexer.fit(tdf).transform(tdf)
    return classIndexedDf

spark = SparkSession.builder.config("spark.executor.memory", "5g").config("spark.driver.memory", "15g").getOrCreate()
df = spark.read.csv('cleaned2017file1.csv', sep=',', inferSchema=True, header=True)
df = stringIndexerDf(df)

rf = RandomForestClassificationModel.load('hdfs://master-node:9000/user/hadoopkumar/RfModel')
numcols = [x for (x, dataType) in df.dtypes if ((dataType =="int") | (dataType == "double") & (x != "class_index"))]
vectorAssembledDf = VectorAssembler(inputCols = numcols, outputCol = "Features").setHandleInvalid("skip").transform(df).select("Features")
standardizedDf = StandardScaler(inputCol = "Features", outputCol="ScaledFeatures").fit(vectorAssembledDf).transform(vectorAssembledDf)
predictions = rf.predict(standardizedDf)
evaluator = MulticlassClassificationEvaluator( labelCol="class_index", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print(accuracy)