#!/usr/bin/env python3

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StandardScaler, VectorAssembler, StringIndexer
from matplotlib.pyplot import figure
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from matplotlib import pyplot as plt


class TrainRandomForest:
    def __init__(self,):
        self.spark = SparkSession.builder.config("spark.executor.memory", "5g").config("spark.driver.memory", "15g").getOrCreate()

    def stringIndexerDf(self, training_file, ):
        df = self.spark.read.csv('hdfs://master-node:9000/user/hadoopkumar/data/'+ training_file, sep=',', inferSchema=True, header=True).drop('_c1')
        Indexer = StringIndexer(inputCol='Label', outputCol="class_index").setHandleInvalid("skip")
        classIndexedDf = Indexer.fit(df).transform(df)
        return classIndexedDf

    def train(self, trainingDf, ):
        numcols = [x for (x, dataType) in trainingDf.dtypes if ((dataType =="int") | (dataType == "double") & (x != "class_index"))]
        vectorAssembledDf = VectorAssembler(inputCols = numcols, outputCol = "Features").setHandleInvalid("skip").transform(trainingDf).select("class_index","Features")
        standardizedDf = StandardScaler(inputCol = "Features", outputCol="ScaledFeatures").fit(vectorAssembledDf).transform(vectorAssembledDf)
        train, test = standardizedDf.randomSplit([0.7, 0.3], seed = 2022)
        rf = RandomForestClassifier(featuresCol = 'ScaledFeatures', labelCol = 'class_index').fit(train)
        predictions = rf.transform(test)
        evaluator = MulticlassClassificationEvaluator( labelCol="class_index", predictionCol="prediction", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)
        with open('accuracy.txt', 'w') as f:
            f.write(str(accuracy)+'\n')
            f.close()
        figure(figsize=(12, 10), dpi=100)
        plt.barh(numcols, rf.featureImportances)
        plt.savefig('featureimportances.png')
        rf.write().overwrite().save('hdfs://master-node:9000/user/hadoopkumar/RfModel')

if __name__ == "__main__":
    """ This is executed when run from the command line """
    trainer = TrainRandomForest()
    df = trainer.stringIndexerDf('2018Training_Data.csv')
    # print(df)
    trainer.train(df)

    
   
    
