from pyspark.sql import SQLContext
from pyspark import SparkContext
import csv, json
sc = SparkContext()
sqlContext = SQLContext(sc)
business_df = sqlContext.read.json("business.json")
business_df.registerTempTable("bus")
sqlContext.registerFunction("cc", lambda x, y: y in x)
list = []
f = open("/home/baadalvm/cop/Documents/out.csv", "r")

sr = csv.reader(f, delimiter='\n')
i=0
for rw in sr:
    f=open("/home/baadalvm/cop/Documents/output/file"+str(i)+".json", "w")    
    list.append(sqlContext.sql('select * from bus where cc(bus.categories, "{}")=true'.format(rw[0])))
    l=list[i].toJSON().collect()
    for j in l:
        print j.encode('utf8')
	f.write(j.encode('utf8')+'\n')
	
    i=i+1
