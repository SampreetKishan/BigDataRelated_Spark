# Dataset: https://bookdown.org/tpinto_home/Regression-and-Classification/logistic-regression.html
# Code: https://medium.com/featurepreneur/logistic-regression-with-pyspark-in-10-steps-3edecd6fe3c5

from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator


spark = SparkSession.builder.appName("FracturedOrNot").getOrCreate()

rawdata = spark.read.csv("file:///Users/samkishan/server/bmd.csv", header=True,inferSchema=True)
#rawdata.show(10)
#print(rawdata.count()) = 169
#rawdata.printSchema()
#root
# |-- id: integer (nullable = true)
# |-- age: double (nullable = true)
# |-- sex: string (nullable = true)
# |-- fracture: string (nullable = true) (we are going to predict this dependent variable)
# |-- weight_kg: double (nullable = true)
# |-- height_cm: double (nullable = true)
# |-- medication: string (nullable = true)
# |-- waiting_time: integer (nullable = true)
# |-- bmd: double (nullable = true)


StringIndexer_1  = StringIndexer(inputCol="sex", outputCol="sex_int")
StringIndexer_2  = StringIndexer(inputCol="medication", outputCol="medication_int")
StringIndexer_3 = StringIndexer(inputCol="fracture", outputCol="fracture_int")

StringIndexer_1 = StringIndexer_1.fit(rawdata)
StringIndexer_2 = StringIndexer_2.fit(rawdata)
StringIndexer_3 = StringIndexer_3.fit(rawdata)
rawdata = StringIndexer_1.transform(rawdata)
rawdata = StringIndexer_2.transform(rawdata)
rawdata = StringIndexer_3.transform(rawdata)

VectorAssembler1 = VectorAssembler(inputCols=["id", "age", "sex_int", "weight_kg",
                                              "height_cm", "medication_int","waiting_time","bmd"],
                                   outputCol="InputFeatures")

rawdata_1 = VectorAssembler1.transform(rawdata)

rawdata_1 = rawdata_1.select("InputFeatures","fracture","fracture_int")
training_data, testing_data = rawdata_1.randomSplit([0.8,0.2])

log_reg = LogisticRegression(labelCol="fracture_int", featuresCol="InputFeatures")
log_reg_learn = log_reg.fit(training_data)

log_reg_learn_summary = log_reg_learn.summary
log_reg_learn_summary.predictions.select("fracture","fracture_int","probability","prediction").show()

#Let's work on the testing dataset

predictions = log_reg_learn.evaluate(testing_data)
predictions.predictions.show()


#print(f"Area Under ROC Curve: {area_under_roc}")




spark.stop()