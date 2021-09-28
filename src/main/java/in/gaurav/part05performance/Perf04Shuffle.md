# Shuffle
* The Spark shuffle is a mechanism for redistributing or re-partitioning data so that the data grouped differently across partitions.
* This is required when a transformation requires information from other partitions, such as summing all the values in a column. Spark will gather the required data from each partition and combine it into a new partition, likely on a different executor.
* Spark shuffling triggers for transformation operations like groupByKey(), reduceByKey(), join() etc.
* Spark shuffle is a very expensive operation as it moves the data between executors or even between worker nodes in a cluster. It involves the following:
  1. Disk I/O 
  2. Data serialization and deserialization 
  3. Network I/O
 of processing cannot begin until all three partitions are evaluated, the overall results from the stage will be delayed.
* When you have a performance issue on Spark jobs, you should look at the Spark transformations that involve shuffling.
* During a shuffle, data is written to disk and transferred across the network, halting Spark’s ability to do processing in-memory and causing a performance bottleneck. 
* Consequently, we want to try to reduce the number of shuffles being done or reduce the amount of data being shuffled.

### Map-Side Reduction
* When aggregating data during a shuffle, rather than pass all the data, it is preferred to combine the values in the current partition and pass only the result in the shuffle. 
* This process is known as Map-Side Reduction and improves performance by reducing the quantity of data being transferred during a shuffle.

## Map-Side Reduction: ReduceByKey vs GroupByKey
* Let us use an example:
  * Node 1 has this data: (big, 1), (big, 2), ... (big, 2000) and (data, 1), (data, 2), ... (data, 1000).
  * Node 2 has this data: (big, 1), (big, 2), ... (big, 1000) and (data, 1), (data, 2), ... (data, 2000).
  * Node 3 has this data: (big, 1), (big, 2), ... (big, 3000).
* We want to count how many times each word occurs.
* We’ll start with the _reduceByKey()_ method, which is the "better" one.
```
1: Initial data
Node 1 -> (big, 1), (big, 2), ... (big, 2000), (data, 1), (data, 2), ... (data, 1000)
Node 2 -> (big, 1), (big, 2), ... (big, 1000), (data, 1), (data, 2), ... (data, 2000)
Node 3 -> (big, 1), (big, 2), ... (big, 3000)

2: Counting
Node 1 -> (big, 2000), (data, 1000)
Node 2 -> (big, 1000), (data, 2000)
Node 3 -> (big, 3000)

3: Shuffle
Node 1 -> (big, 2000), (big, 1000), (big, 3000)
Node 2 -> (data, 1000), (data, 2000)
Node 3 -> 

4: Result
Node 1 -> (big, 6000)
Node 2 -> (data, 3000)
Node 3 -> 
```
* When we use _reduceByKey()_, sums for each word will be calculated "locally" on each of the nodes, i.e. the **Reduce** operation was first performed on each node based on its local portion of data, before the data was sent to the nodes, where the final "reduce" phase will take place, which will give us the result.
* Thanks to the reduce operation, we locally limit the amount of data that **circulates** between nodes in the cluster. In addition, we reduce the amount of data subjected to the process of Serialization and Deserialization.
* Now let’s look at what happens when we use the _groupByKey()_ method. 
```
1: Initial data
Node 1 -> (big, 1), (big, 2), ... (big, 2000), (data, 1), (data, 2), ... (data, 1000)
Node 2 -> (big, 1), (big, 2), ... (big, 1000), (data, 1), (data, 2), ... (data, 2000)
Node 3 -> (big, 1), (big, 2), ... (big, 3000)

2: Shuffle
Node 1 -> (big, 1), (big, 2), ... (big, 2000), (big, 1), (big, 2), ... (big, 1000), (big, 1), (big, 2), ... (big, 3000)
Node 2 -> (data, 1), (data, 2), ... (data, 1000), (data, 1), (data, 2), ... (data, 2000)
Node 3 -> 

3: Result
Node 1 -> (big, 6000)
Node 2 -> (data, 3000)
Node 3 -> 
```
* As you can see, there is no reduce phase. As a result, the exchange of data between nodes is greater. 
* In addition, remember the process of Serialization and Deserialization, which in this case must be done on a larger amount of data.