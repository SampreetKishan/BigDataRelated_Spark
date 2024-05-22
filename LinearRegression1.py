#https://www.kaggle.com/datasets/ankanhore545/cost-of-living-index-2022/


from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.appName("LinearRegression1").getOrCreate()
rawdata = spark.read.csv("file:///Users/samkishan/server/COL.csv", inferSchema=True, header=True)
print("Sample of the rawdata: ")
#rawdata.show(20)
#rawdata.printSchema()
#print("The number of entries: ", rawdata.count())

feature_assemble = VectorAssembler(inputCols=["Rent Index", "Cost of Living Plus Rent Index",
                                              "Groceries Index", "Restaurant Price Index",
                                              "Local Purchasing Power Index"], outputCol="CombinedFeatures")
output = feature_assemble.transform(rawdata)

print("After transforming the rawdata with the vector assembler object: ")
#output.show()
output = output.select("Cost of Living Index", "CombinedFeatures")
output.show()

training_data, testing_data = output.randomSplit([0.8,0.2])
linear_model = LinearRegression(featuresCol="CombinedFeatures", labelCol="Cost of Living Index")
linear_fit = linear_model.fit(training_data)

print("The linear regression model's coefficients are: ", linear_fit.coefficients)
print("The linear regression model's intercept is: ", linear_fit.intercept)

testing_prediction = linear_fit.evaluate(testing_data)
testing_prediction.predictions.show()

print("The mean absolute erorr:", testing_prediction.meanAbsoluteError)
print("The mean squared error: ", testing_prediction.meanSquaredError)


spark.stop()