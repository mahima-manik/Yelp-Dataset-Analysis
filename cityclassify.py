#SAVING all the businesses relating to the food in food_NorthYork.json
from pyspark.sql import SQLContext
from pyspark import SparkContext
import csv, json
sc = SparkContext()
sqlContext = SQLContext(sc)
business_df = sqlContext.read.json("/home/baadalvm/cop/Documents/output/file895.json")
business_df.registerTempTable("bus")
sqlContext.registerFunction("cc", lambda x, y: y in x)
f=open("/home/baadalvm/cop/Documents/hotel_stuttgart.json", "w")
l=(sqlContext.sql('select * from bus where bus.city="Stuttgart"')).toJSON().collect()
for j in l:
    f.write(j.encode('utf8')+'\n')

