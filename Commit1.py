from pyspark.streaming import StreamingContext
import json
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext

sparkcontext=SparkContext("local[2]",appName="spam_data")
streamingsparkcontext=StreamingContext(sparkcontext, 1)
sql_context=SQLContext(sparkcontext)
def mapper(dat):
    jsondata=json.loads(dat)
    listrec = list()
    for i in jsondata:
        tup = tuple(jsondata[i].values())
        listrec.append(tup)
    return listrec
def readMyStream(rdd):
  if not rdd.isEmpty():
    ss = SparkSession(rdd.context)
    d = rdd.collect()[0]
    n = len(data[0])
    col = [f"feature{l}" for l in range(n)]
    df = ss.createDataFrame(d, col)
    df.show()

x= streamingsparkcontext.socketTextStream("localhost",6100).map(mapper)
if (x is not None):
       x.foreachRDD(lambda rdd: print(rdd))

streamingsparkcontext.start()
streamingsparkcontext.awaitTermination()
streamingsparkcontext.stop()