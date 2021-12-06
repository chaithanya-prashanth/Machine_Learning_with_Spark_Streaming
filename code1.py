import sys
from pyspark.sql import SparkSession 
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


streamcontent = SparkContext.getOrCreate()
spark = SparkSession(streamcontent)
streamcontent.setLogLevel( 'OFF' )
ssc = StreamingContext(streamcontent , 1)

streamed_data = ssc.socketTextStream("localhost",6100)



def MyStreamReading(rdd):
    df=spark.read.json(rdd)
    df.printSchema()
    #print('here')
    df.show()

#stream_data.pprint()
streamed_data.foreachRDD(lambda x:MyStreamReading(x))

ssc.start()
ssc.awaitTermination()