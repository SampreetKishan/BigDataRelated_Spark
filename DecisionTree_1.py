from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier

spark = SparkSession.builder.appName("DecisionTree").config("spark.driver.maxResultSize", "4g").getOrCreate()

#Let's ingest the drug data into spark
rawdata_drugs = spark.read.csv("file:///Users/samkishan/server/drug200.csv", inferSchema=True, header=True)
#rawdata_drugs.show(20)

#Let's convert categorical columns "Sex", "BP", and "Cholestrol" to numerical values using StringIndexer

#For Sex
Sex_idx = StringIndexer(inputCol="Sex", outputCol="Sex_idx")
Sex_idx_model = Sex_idx.fit(rawdata_drugs)
rawdata_drugs_sex_idexed = Sex_idx_model.transform(rawdata_drugs)

#For BP
BP_idx = StringIndexer(inputCol="BP", outputCol="BP_idx")
BP_idx_model = BP_idx.fit(rawdata_drugs_sex_idexed)
sex_BP_Indexed_df = BP_idx_model.transform(rawdata_drugs_sex_idexed)

#For Cholestrol
Chl_idx = StringIndexer(inputCol="Cholesterol", outputCol="Chl_idx")
Chl_idx_model = Chl_idx.fit(sex_BP_Indexed_df)
sex_BP_Chl_Indexed_df = Chl_idx_model.transform(sex_BP_Indexed_df)

#For Drug
Drug_idx = StringIndexer(inputCol="Drug",outputCol="Drug_idx")
Drug_idx_model = Drug_idx.fit(sex_BP_Chl_Indexed_df)
sex_BP_Chl_drug_Indexed_df = Drug_idx_model.transform(sex_BP_Chl_Indexed_df)

#print("UnIndexed and Indexed raw data: ")
#sex_BP_Chl_drug_Indexed_df .show(20)

indexed_df = sex_BP_Chl_drug_Indexed_df.select("Age", "Sex_idx", "BP_idx","Chl_idx", "Na_to_K","Drug_idx") #.withColumnRenamed("Chl_idx", "Cholesterol_idx")
#let's cache this dataframe
indexed_df = indexed_df.cache()


#Let's provide a count
#print("Count")
#print(indexed_df.count())


#let's provide a sample
#print("\nSample of data")
#indexed_df.show(10)

#let's provide a count of all Ages
#print("Distribution of ages")
#indexed_df.groupBy("Age").count().show()

#print("\nDistribution of sexes")
#indexed_df.groupBy("Sex_idx").count().show()

#print("\nDistribution of BP")
#indexed_df.groupBy("BP_idx").count().show()

#print("\nDistribution of Chl")
#indexed_df.groupBy("Chl_idx").count().show()


#We need to combine the features of the index_df into one column
assembler = VectorAssembler(inputCols=["Age","Sex_idx","BP_idx","Chl_idx","Na_to_K"],outputCol="features" )
indexed_df_features = assembler.transform(indexed_df)
indexed_df_features = indexed_df_features.select("features","Drug_idx")
print("---- Sample of dataset that contains a vector of the features ------")
indexed_df_features.show(5)

#Now that we have the data in the right format, let's perform decision tree to determine
#But first let's split the data into training:test data set (80:20)
drugs_training, drugs_test = indexed_df_features.randomSplit([0.7, 0.3],seed=1)

#Create a DecisionTreetClassiciation object
drug_classifier = DecisionTreeClassifier(labelCol="Drug_idx", featuresCol="features")

print(" -------------------------  ")
print("Count of drugs training records:", drugs_training.count())
print("Count of drugs testing", drugs_test.count())
print(" -------------------------  ")
#print("------- Sample of the training dataset ---------")
#drugs_training.show(10)

#Fit the object with the training data
drugs_classification_model = drug_classifier.fit(drugs_training)

#Let's predict how good our model does against the test data
print("\n---- Predictions --------- ")
drug_classifier_predictor_efficiency = drugs_classification_model.transform(drugs_test)
drug_classifier_predictor_efficiency.groupBy("Drug_idx","prediction").count().show()

print("Number of predictions: ", drug_classifier_predictor_efficiency.count())

#print("\n --------- Sample of drugs Training dataset  ---------")
#drugs_training.show(5)
#print("\n --------- Sample of drugs Test dataset  ---------")
#drugs_test.show(5)
#drug_classifier_predictor_efficiency.show()

#drugs_training.select("features").show(10)

#Let's determine the accuracy of the model

CorrectPredictions = drug_classifier_predictor_efficiency.filter("Drug_idx = prediction").count()
IncorrectPredictions = drug_classifier_predictor_efficiency.filter("Drug_idx != prediction").count()

accuracy = CorrectPredictions / (CorrectPredictions + IncorrectPredictions)
accuracy = 100 * accuracy
accuracy = round(accuracy,2)
print("The accuracy of this model is: ", accuracy, "%")

spark.stop()