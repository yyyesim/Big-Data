from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working threads and a batch interval of 2 seconds
sc = SparkContext("local[2]", "Sensor")
ssc = StreamingContext(sc, 20)

# Create a DStream
lines = ssc.socketTextStream("sandbox-hdp.hortonworks.com", 3333)

# Split each line into words
data = lines.flatMap(lambda line: line.split(" "))

# Map each data key value pair by splitting 
pairs = data.map(lambda sensor: sensor.split(","))

#Calculating minimum values for each sensor
print("For (sensor,min)")
minValue = pairs.reduceByKey(min)
minValue.pprint()

#Calculating minimum values for each sensor
print("For (sensor,max)")
maxValue = pairs.reduceByKey(max)
maxValue.pprint()

#Calculating sum of measurements for each sensor
print("For (sensor,avg)")
total = pairs.reduceByKey(lambda x, y: float(x)+ float(y))

#Calculating occcurance values for each sensor
occurance = pairs.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)

#Joining sum and occurance value and calculating average for each sensor
joined = total.join(occurance)
avg = joined.map(lambda (k,(v,w)): (k,v/w))
avg.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
