from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import  StructType, StructField, IntegerType, FloatType, StringType

spark = SparkSession.builder.appName("totalCustomerPurchases").getOrCreate()

schema = StructType([
    StructField("customer_id", IntegerType()),
    StructField("item_id", IntegerType()),
    StructField("purchase_value",FloatType())
])

rawdata = spark.read.schema(schema).csv("file:///Users/samkishan/server/customer-orders.csv")

#Method1

rawdata.createOrReplaceTempView("customer_purchases")

customer_purchases = spark.sql("""
    select customer_id, round(sum(purchase_value),2) as total_sales
    from customer_purchases 
    group by customer_id
    order by total_sales desc
""")

customer_purchases.show(customer_purchases.count())


#method2
grouped_data = rawdata.groupby("customer_id").agg(func.round(func.sum("purchase_value"),2).alias("TotalPurchase")).orderBy(func.desc("TotalPurchase"))

grouped_data.show(grouped_data.count())




spark.stop()