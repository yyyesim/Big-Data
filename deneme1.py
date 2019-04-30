from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working threads and a batch interval of 2 seconds
sc = SparkContext("local[2]", "Sensor")
ssc = StreamingContext(sc, 20)

# Create a DStream
lines = ssc.socketTextStream("sandbox-hdp.hortonworks.com", 3333)

# Bazic reduceByKey example in python
# creating PairRDD x with key value pairs
x = sc.parallelize([("a", 1), ("b", 1), ("a", 1), ("a", 1),
                    ("b", 1), ("b", 1), ("b", 1), ("b", 1)], 3)
 
# Applying reduceByKey operation on x
y = x.reduceByKey(lambda accum, n: accum + n)
print(y.collect())
# [('b', 5), ('a', 3)]
 
# Define associative function separately 
def sumFunc(accum, n):
    return accum + n
 
y = x.reduceByKey(sumFunc)
print(y.collect())
# [('b', 5), ('a', 3)]
