#!/usr/bin/env python3

from pyspark.mllib.feature import StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StandardScaler, VectorAssembler 
from matplotlib.pyplot import figure
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def main():
    spark = SparkSession.builder.config("spark.executor.pyspark.memory", "6g") \
            .config("spark.executor.memory", "6g").config("spark.driver.memory", "6g") \
            .config("spark.driver.maxResultSize", "6g").getOrCreate()

    df = spark.read.csv('data/datasetfinal.csv', sep=',', inferSchema=True, header=True).select("Duration","Dst_bytes","Flag","Hot","Num_compromised","Count","Serror_rate","Rerror_rate","Same_srv_rate","Diff_srv_rate","Srv_count","Srv_serror_rate","Srv_rerror_rate","Srv_diff_host_rate","Dst_host_count","Dst_host_srv_count","Dst_host_same_srv_rate","Dst_host_diff_srv_rate","Dst_host_same_src_port_rate","Dst_host_srv_diff_host_rate","Dst_host_serror_rate","Dst_host_srv_serror_rate","Dst_host_rerror_rate","class","class_index")

    numcols = [x for (x, dataType) in df.dtypes if ((dataType =="int") | (dataType == "double") & (x != "class_index"))]

    
    vector_assembler = VectorAssembler(inputCols = numcols, outputCol = "features")
    temptrain = vector_assembler.setHandleInvalid("skip").transform(df).select("class_index","features")

    df = StandardScaler(inputCol = "features", outputCol="scaledfeatures").fit(temptrain).transform(temptrain)

    train, test = df.randomSplit([0.7, 0.3], seed = 2022)


    rf = RandomForestClassifier(featuresCol = 'scaledfeatures', labelCol = 'class_index').fit(train)

    predictions = rf.transform(test)

    evaluator = MulticlassClassificationEvaluator( labelCol="class_index", predictionCol="prediction", metricName="accuracy")

    accuracy = evaluator.evaluate(predictions)

    print(accuracy)

if __name__ == "__main__":
    """ This is executed when run from the command line """
    main()