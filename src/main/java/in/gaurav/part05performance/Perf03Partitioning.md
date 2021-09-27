# Spark Partitions
## Partitioning
* To distribute work across the cluster and reduce the memory requirements of each node, Spark will split the data into smaller parts called Partitions. 
* Each of these is then sent to an Executor to be processed. 
* Only one partition is computed per executor thread at a time, therefore the size and quantity of partitions passed to an executor is directly proportional to the time it takes to complete.

### Data Skew
* Often the data is split into partitions based on a key, for instance the first letter of a name.
* If values are not evenly distributed throughout this key, then more data will be placed in one partition than another.
* An example would be:
```
{Adam, Alex, Anja, Beth, Claire}
-> A: {Adam, Alex, Anja}
-> B: {Beth}
-> C: {Clair}
```
* Here the A partition is 3 times larger than the other two, and therefore will take approximately 3 times as long to compute.
* As the next stage

### Scheduling
* The other problem that may occur when splitting into partitions is that there are too few partitions to correctly cover the number of executors available. 
* An example is: 
```
-> There are 2 Executors and 3 partitions. 
-> Executor 1 has an extra partition to compute and therefore takes twice as long as Executor 2. 
-> This results in Executor 2 being idle and unused for half the job time.
```

### Solution
* The simplest solution to the above two problems is to increase the number of partitions used for computations. 
* This will reduce the effect of skew into a single partition and will also allow better matching of scheduling to CPUs. 
* A common recommendation is to have 4 partitions per CPU, however settings related to Spark performance are very case dependent, and so this value should be fine-tuned with your given scenario.

### Salting Method to fix Data Skewness
* If a key has more records compared to the other key, the corresponding partition would become very large or _SKEWED_ (compared to the other partitions). 
* As such whichever executor will be processing that specific partition , will need comparatively more time to process.
* This causes the overall Spark job to Standstill, Low utilization of CPU on other nodes, and sometimes even memory issues.
* **Salting or Key-Salting:** The idea is to modify the existing key to make an even distribution of data.

```New-Key = Existing-Key + "_" + Random number in [1,10]```

* We split the key=x into say x_1, x_2 ...etc. 
* Key-value "x" was the cause of the Data-skew due to large number of records belonging to key "x". 
* Now, all the records associated with just one key=x, get split across several keys (x_1, x_2, ...). This makes the data to be more distributed.
