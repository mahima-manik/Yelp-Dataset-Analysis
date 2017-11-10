from pyspark.sql import SQLContext
from pyspark import SparkContext
import csv, json
sc = SparkContext()
sqlContext = SQLContext(sc)
review_df = sqlContext.read.json("review.json")
review_df.registerTempTable("rev")
sqlContext.registerFunction("cc", lambda x, y: y == x)
list = []
f = open("/home/baadalvm/cop/Documents/hotel_stuttgart.json", "r")
i=0
for rw in f:
    f=open("/home/baadalvm/cop/Documents/hotel_reviews/rev"+str(json.loads(rw)["business_id"])+".json", "w")
    list.append(sqlContext.sql('select * from rev where cc(rev.business_id, "{}")=true'.format(json.loads(rw)["business_id"])))
    l=list[i].toJSON().collect()
    for j in l:
        print j.encode('utf8')
        f.write(j.encode('utf8')+'\n')

    i=i+1
