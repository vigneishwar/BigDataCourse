# BigDataCourse

`sc.defaultMinParitions` determines the minimum number of paritions rdd has when loading from a file

`sc.defaultParallelism` to check the parallelism level when using a list


# Repartition 
#####===================

we have 500 mb file in hdfs and we have a spark cluster of 20 machines.

then default block size is 128 mb so there will be 4 blocks.

then the number of partitions is 4 and only 4 machines are being used.

we can increase the number of partitions to use more resources.

`rdd.repartition(10)`
