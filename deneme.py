from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working threads and a batch interval of 2 seconds
sc = SparkContext("local[2]", "Sensor")
ssc = StreamingContext(sc, 20)

# Create a DStream
lines = ssc.socketTextStream("sandbox-hdp.hortonworks.com", 3333)

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))
words.pprint()

# Count each word in each batch
#pairs = words.map(lambda word: (word, 1))
#pairs = words.flatMap(lambda word: (word.split(",")[0], word))
pairs = words.map(lambda word: word.split(","))
pairs.pprint()


my_list = [1,2,3,4,5,6,7,8,9,10]
# Lets say I want to square each term in my_list.
squared_list = map(lambda x:x**2,my_list)
print squared_list

#cntValue = pairs.reduceByKey(lambda accum, n: accum.cast('float') + n.cast('float'))
#cntValue.pprint()

avgValue3 = pairs.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (float(x[0]) + float(y[0]) , float(x[1]) + float(y[1]) , x/y))
avgValue3.pprint()

#rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

#avgValue = pairs.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: int(x[0]) + int(y[0]) , int(x[1]) + int(y[1]))
#avgValue = pairs.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (y[0] + y[1], x[1] + y[1]))



#wordCounts = pairs.reduceByKey(lambda x, y: x + y)
minValue = pairs.reduceByKey(min)
# Print each batch
minValue.pprint()


#print("For (sensor,max)")
#wordCounts = pairs.reduceByKey(lambda x, y: x + y)
maxValue = pairs.reduceByKey(max)
# Print each batch
maxValue.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
