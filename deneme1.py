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
