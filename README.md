# BigDataCourse

`sc.defaultMinParitions` determines the minimum number of paritions rdd has when loading from a file

`sc.defaultParallelism` to check the parallelism level when using a list


# Repartition 


we have 500 mb file in hdfs and we have a spark cluster of 20 machines.

then default block size is 128 mb so there will be 4 blocks.

then the number of partitions is 4 and only 4 machines are being used.

we can increase the number of partitions to use more resources.

`rdd.repartition(10)`

we can also decrease the number of partitions using repartition 

it is a wide transformation as shuffling is involved 

# Coalesce 

it can only decrease the number of partitions and cannot increase

# To decrease the partitions coalesce is preferred as it minimize the shuffling 


# Structured API's

in spark dataframes drivers will convert the high level code to low level code using the driver 

spark compiler will convert the high level code to low level code and then passes it to the executors 


# Read modes while loading data 

1. PERMISSIVE -(default mode) -> set all fields to null when there are nulls 
2. DROPMALFORMED -> it will ignore the null
3. FAILFAST -> raise an exception