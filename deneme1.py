





# Defining Seqencial Operation and Combiner Operations
# Sequence operation : Finding Maximum Marks from a single partition
def seq_op(accumulator, element):
    if(accumulator > element[1]):
        return accumulator 
    else: 
        return element[1]
 
 
# Combiner Operation : Finding Maximum Marks out Partition-Wise Accumulators
def comb_op(accumulator1, accumulator2):
    if(accumulator1 > accumulator2):
        return accumulator1 
    else:
        return accumulator2
 
# Zero Value: Zero value in our case will be 0 as we are finding Maximum Marks
zero_val = 0
aggr_rdd = student_rdd.map(lambda t: (t[0], (t[1], t[2]))).aggregateByKey(zero_val, seq_op, comb_op) 
 
# Check the Outout
for tpl in aggr_rdd.collect():
    print(tpl)
 
# Output
# (Tina,87)
# (Thomas,93)
# (Jackeline,83)
# (Joseph,91)
# (Juan,69)
# (Jimmy,97)
# (Cory,71)
 
#####################################################
# Let's Print Subject name along with Maximum Marks #
#####################################################
 
# Defining Seqencial Operation and Combiner Operations
def seq_op(accumulator, element):
    if(accumulator[1] > element[1]):
        return accumulator 
    else: 
        return element
 
 
# Combiner Operation : Finding Maximum Marks out Partition-Wise Accumulators
def comb_op(accumulator1, accumulator2):
    if(accumulator1[1] > accumulator2[1]):
        return accumulator1 
    else:
        return accumulator2
    
 
# Zero Value: Zero value in our case will be 0 as we are finding Maximum Marks
zero_val = ('', 0)
aggr_rdd = student_rdd.map(lambda t: (t[0], (t[1], t[2]))).aggregateByKey(zero_val, seq_op, comb_op) 
 
# Check the Outout
for tpl in aggr_rdd.collect():
    print(tpl)
 
# Output
# ('Thomas', ('Physics', 93))
# ('Tina', ('Biology', 87))
# ('Jimmy', ('Chemistry', 97))
# ('Juan', ('Physics', 69))
# ('Joseph', ('Chemistry', 91))
# ('Cory', ('Chemistry', 71))
# ('Jackeline', ('Maths', 86))
 
#####################################################################
# Printing over all percentage of all students using aggregateByKey #
#####################################################################
 
# Defining Seqencial Operation and Combiner Operations
def seq_op(accumulator, element):
    return (accumulator[0] + element[1], accumulator[1] + 1)
    
 
# Combiner Operation : Finding Maximum Marks out Partition-Wise Accumulators
def comb_op(accumulator1, accumulator2):
    return (accumulator1[0] + accumulator2[0], accumulator1[1] + accumulator2[1])
    
 
# Zero Value: Zero value in our case will be 0 as we are finding Maximum Marks
zero_val = (0, 0)
aggr_rdd = student_rdd.map(lambda t: (t[0], (t[1], t[2])))
                      .aggregateByKey(zero_val, seq_op, comb_op)
                      .map(lambda t: (t[0], t[1][0]/t[1][1]*1.0))
  
 
# Check the Outout
for tpl in aggr_rdd.collect():
    print(tpl)
