from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working threads and a batch interval of 2 seconds
sc = SparkContext("local[2]", "Sensor")
ssc = StreamingContext(sc, 20)

# Create a DStream
lines = ssc.socketTextStream("sandbox-hdp.hortonworks.com", 3333)

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch
#pairs = words.map(lambda word: (word, 1))
pairs = words.map(lambda word: (word.split(",")[0], word))

#min = "For (sensor,min)"
#minString = sc.parallelize(List(min)).collect()
#minString.pprint()

#wordCounts = pairs.reduceByKey(lambda x, y: x + y)
minValue = pairs.reduceByKey(min)
# Print each batch
minValue.pprint()

#print("For (sensor,max)")
#wordCounts = pairs.reduceByKey(lambda x, y: x + y)
maxValue = pairs.reduceByKey(max)
# Print each batch
maxValue.pprint()

#aTuple = (0,0) # As of Python3, you can't pass a literal sequence to a function.
avg = pairs.aggregateByKey((0,0), lambda a,b: (a[0] + b[1], a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1]))
avgValue = avg.mapValues(lambda v: v[0]/v[1]).collect()
avgValue.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
