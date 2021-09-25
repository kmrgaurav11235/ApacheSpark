# What is Spark RDD?
Spark has RDD (Resilient Distributed Dataset), an immutable fault-tolerant, distributed collection of objects that can be operated on in parallel.
* **Resilient** – capable of rebuilding data on failure
* **Distributed** – distributes data among various nodes in cluster
* **Dataset** – collection of partitioned data with values

RDD is a logical reference of a dataset which is partitioned across many server machines in the cluster. RDDs are Immutable and are self-recover in case of failure.

## RDD Operations
Apache Spark RDD supports two types of Operations-
* Transformations
* Actions

## Transformations
* Spark RDD Transformations are functions that take an RDD as the input and produce one or many RDDs as the output. 
* They do not change the input RDD (since RDDs are immutable and hence one cannot change it), but always produce one or more new RDDs by applying the computations they represent e.g. Map(), filter(), reduceByKey() etc. 
* When you apply the transformation on any RDD it will not perform the operation immediately. It will create a **DAG(Directed Acyclic Graph)** using the applied operation, source RDD and function used for transformation. 
* It will keep on building this graph using the references till you apply any **Action operation** on the last lined up RDD. That is why the **Transformation in Spark are lazy**.
* e.g. map(), filter(), join() etc.

### Narrow vs Wide Transformations
There are two types of transformations:
1. **Narrow transformation** — They don’t require the data to be shuffled across the partitions. for example, Map, filter etc.
2. **Wide transformation** — They require the data to be shuffled for example, reduceByKey etc.

## Actions
* Transformations create RDDs from each other, but when we want to work with the actual dataset, at that point action is performed. 
* When the **Action** is triggered after the result, new RDD is not formed. Thus, Actions are Spark RDD operations that give non-RDD values. 
* The values of action are stored to drivers or to the external storage system. 
* Action is one of the ways of sending data from Executor to the driver. Executors are agents that are responsible for executing a task. While the driver is a JVM process that coordinates workers and execution of the task.
* e.g. count(), collect(), take() etc.

## Pair RDD
* Spark Paired RDDs are nothing but RDDs containing a key-value pair. Here, the key is the identifier, whereas value is the data corresponding to the key value. 
* However, Pair RDDs contain few special operations in it. Such as, distributed “shuffle” operations, grouping or aggregating the elements by a key etc. 
