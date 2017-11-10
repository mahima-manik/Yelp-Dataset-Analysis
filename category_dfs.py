from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import json
import pyspark.sql.functions as func
sc = SparkContext()
sqlContext = SQLContext(sc)
spark=SparkSession(sc)
bus_df = sqlContext.read.json("/Users/apple/Desktop/dataset/business.json")
cat_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/Users/apple/Desktop/dataset/out.csv")
def customFunction(row):
    #person = rdd.map(lambda r: row(*r))
    #temp_df = sqlContext.createDataFrame(person)
    print "reaching\n", row['categories']
    t = str(row['categories'])
    #review_df.createTempView("rev")
    #temp_df = review_df.filter(review_df["business_id"]==row["business_id"])
    temp_df = bus_df.withColumn('cat_true', func.array_contains(bus_df['categories'], t))
    #temp_df = bus_df.where((t in bus_df["categories"]))
    #temp_df = sqlContext.sql("SELECT * FROM rev where rev['business_id']=t")
    #print (temp_df)
    df_x = temp_df.filter(temp_df.cat_true == True).drop('cat_true')
    df_x.write.json("/Users/apple/Desktop/dataset/businesses/"+str(t)+".json")
    #join(review_df, "business_id").select("business_id", "cool", "date", "funny", "review_id", "stars", "text", "useful", "user_id").write.json("/Users/apple/Desktop/dataset/test/rev"+str(i))
    #i=i+1

#sqlContext.udf.register("customFunction", customFunction)
for row in cat_df.rdd.collect():
    customFunction(row)
#bus_df.foreach(customFunction)
