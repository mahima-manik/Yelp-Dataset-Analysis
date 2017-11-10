from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import json
sc = SparkContext()
sqlContext = SQLContext(sc)
spark=SparkSession(sc)
bus_df = sqlContext.read.json("/Users/apple/Desktop/food_NorthYork.json")
review_df = sqlContext.read.json("/Users/apple/Desktop/dataset/review.json")

def customFunction(row):
    #person = rdd.map(lambda r: row(*r))
    #temp_df = sqlContext.createDataFrame(person)
    print "reaching\n", row['business_id']
    t = str(row['business_id'])
    #review_df.createTempView("rev")
    #temp_df = review_df.filter(review_df["business_id"]==row["business_id"])
    temp_df = review_df.where(review_df["business_id"] == t)
    #temp_df = sqlContext.sql("SELECT * FROM rev where rev['business_id']=t")
    #print (temp_df)
    temp_df.write.json("/Users/apple/Desktop/dataset/test/rev"+str(t)+".json")
    #join(review_df, "business_id").select("business_id", "cool", "date", "funny", "review_id", "stars", "text", "useful", "user_id").write.json("/Users/apple/Desktop/dataset/test/rev"+str(i))
    #i=i+1

#sqlContext.udf.register("customFunction", customFunction)
for row in bus_df.rdd.collect():
    customFunction(row)
#bus_df.foreach(customFunction)
