# Resource Allocation and Memory Problems

Resource allocation problems occur when a Spark job does not have sufficient or adequate resources to complete its tasks efficiently or at all.

## Insufficient Executor Memory
When executors don't have enough memory allocated, they'll struggle to process data partitions and my fail with OOM errors.
Exit code 137 typically indicates that the process was killed by the operating system due to memory constraints

```
ERROR ExecutorLostFailure: Executor 2 exited with exit code 137 (OOM)
WARN TaskSetManager: Lost task 5.0 in stage 2.0 (TID 42): ExecutorLostFailure (executor 2 exited with exit code 137)
```

#### Solution 1
You need to increase the executor memory.

``` scala
// In Scala
spark.conf.set("spark.executor.memory", "8g")
```

``` bash
spark-submit --executor-memory 8g --class MySparkApp app.jar
```

#### Solution 2
If you can't increase container memory (for example, if you're using `maximizeResourceAllocation` on the node), then increase the number of Spark partitions.

``` scala
val numPartitions = 500
val newDF = df.repartition(numPartitions)
```

If the error happens during a wide transformation (for example join or groupBy), add more shuffle partitions changing the conf `spark.sql.shuffle.partitions`

#### Solution 3
Reducing the number of executor cores reduces the maximum number of tasks that the executor processes simultaneously. Doing this reduces the amount of memory that the container uses.

``` bash
spark-submit --executor-core 1 --class MySparkApp app.jar
```

## Too few Executors or Cores | TODO: search for other error examples
With insufficient parallelism, jobs process data slowly or time out when handling large datasets.

```
INFO DAGScheduler: Job 2 finished: count at MyApp.scala:67, took 342.159837 seconds
```

#### Solution
Increase the number of executors and cores:
```
spark-submit --num-executors 10 --executor-cores 4 --class MySparkApp app.jar
```

``` python
# pySpark
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.executor.instances", "10") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()
```

## Insufficient Driver Memory
The driver program orchestrates the entire Spark application. When it does not have enough memory, it can fail collecting results back together, broadcasting a variable or even by managing tasks scheduling.
It is more rare than having insufficient memory on executors.

```
java.lang.OutOfMemoryError: Java heap space
    at org.apache.spark.sql.execution.CollectLimitExec.executeCollect(limit.scala:47)
    at org.apache.spark.sql.Dataset.collectToPython(Dataset.scala:3383)
```

#### Solution
Just increase the driver memory, by specifing it in spark configurations or in spark-submit.

``` python
# In PySpark
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.driver.memory", "10g") \
    .getOrCreate()
```

```
spark-submit --driver-memory 10g --class MySparkApp app.jar


#### Solution
Just increase the driver memory, by specifing it in spark configurations or in spark-submit.

``` python
# In PySpark
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.driver.memory", "10g") \
    .getOrCreate()
```

```
spark-submit --driver-memory 10g --class MySparkApp app.jar
```

## Resource Manager Constraints
The underlying resource manager (YARN/Kubernetes) may impose constraints that prevent Spark from getting needed resources.

```
ERROR YarnScheduler: Lost executor 3 on host123: Container killed by YARN for exceeding memory limits.
Container killed on request. Exit code is 143
Container exited with a non-zero exit code 143
```

```
Container [pid=xxxx,containerID=container_xxx] is running beyond physical memory limits.
Current usage: xGB of yGB physical memory used; zGB of JVM heap used
Container killed on request. Exit code is 143
Container exited with a non-zero exit code 143
```

#### Solution
Calculate proper memory requirements, for example, if you're processing 100GB of data with 5x expansion factor `Per-executor memory needed = 100GB * 5 / number_of_executors`.
The expansion factor refers to how much memory your data might require during processing compared to its original size: data can expand during deserialization; during transformations like join, groupBy, and repartition creating intermediate data structures; Java/Scala objects have memory overhead beyond raw data; When you cache DataFrames/RDDs; shuffling data can create multiple copies of data during the exchange between executors.

You can gather some information about memory overhead inspecting the GC logs by setting `spark.executor.extraJavaOptions` to `-verbose:gc -XX:+PrintGCDetails`.

Then you can calculate the total memory required as `Per-executor memory + overhead` and adjust container sizing and resource requests by tuning `executor-memory` and `spark.yarn.executor.memoryOverhead=<number-of-gigabytes>g`.

**Default behavior**: If not specified, Spark sets this value to either: 10% of executor memory or 384MB.
## Dynamic Allocation Issues 
Dynamic allocation may not scale up quickly enough for bursty workloads or may not request enough resources.

```
INFO ExecutorAllocationManager: Requesting 1 new executor because tasks are backlogged
WARN ExecutorAllocationManager: Request for executor 5 rejected (maxExecutors limit reached: 5)
```

#### Solution
Adjust dynamic allocation parameters
```
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.initialExecutors", "5") \
    .config("spark.dynamicAllocation.minExecutors", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "20") \
    .config("spark.dynamicAllocation.schedulerBacklogTimeout", "30s") \
    .getOrCreate()
```
##### Parameters description
- ```spark.dynamicAllocation.enabled```  
  Enables or disables dynamic allocation of executors.  
  ***Values: true or false (default: false)  
  Note: You must also enable spark.shuffle.service.enabled (set to true) when using YARN or Standalone.***

- ```spark.dynamicAllocation.minExecutors```  
	The minimum number of executors the application will maintain.  
	***Default: 0*** 

- ```spark.dynamicAllocation.maxExecutors```  
	The maximum number of executors the application can request.  
	***Default: unlimited (Int.MaxValue)***

- ```spark.dynamicAllocation.initialExecutors```
The number of executors to start with when the application launches.  
***Default: equal to minExecutors  
Note: If spark.executor.instances or --num-executors is set and greater, that will be used instead.***

- ```spark.dynamicAllocation.executorIdleTimeout```  
How long (in seconds) an executor can be idle before it is removed.  
***Default: 60s***

- ```spark.dynamicAllocation.schedulerBacklogTimeout```  
How long (in seconds) tasks must remain pending before requesting new executors.  
***Default: 1s***

- ```spark.dynamicAllocation.sustainedSchedulerBacklogTimeout```  
The interval (in seconds) between subsequent executor requests if the backlog continues.  
***Default: same as schedulerBacklogTimeout***

- ```spark.dynamicAllocation.executorAllocationRatio```  
Scaling factor for how many executors to request relative to task parallelism.  
***Default: 1.0 (match full parallelism)  
Note: Set to a lower value (e.g., 0.5) to reduce resource usage in jobs with small tasks.***

## Memory Leaks
Memory usage grows continuously over time, eventually leading to OOM errors.
Memory leaks don't always produce distinct exceptions - they often manifest as gradually degrading performance followed by OOM errors.

Signs of memory leaks are:
- Increasing memory usage over time without corresponding data growth
- Growing number of objects in heap dumps
- Progressively slower processing as job continues
#### Broadcast variables
Avoid capturing large objects in closures and use broadcast variables instead.
``` python
large_data = {"key": "very large value"}
def process_data(record):
	# Use broadcast variables instead
	broadcast_data.value["key"]
```

#### Accumulators
Avoid to accumulates in driver memory and use proper Spark aggregations or accumulators.
``` python
total_results = []
for i in range(100):
    results = df.filter(df.id == i).collect()
    total_results.extend(results)  # Memory grows unbounded

# User proper Spark aggregations
results = df.groupBy("id").agg({"value": "sum"})
```
#### Release unused DataFrame/RDDs
Unpersist cached data when no longer needed.
``` python
cached_df = df.cache()
cached_df.count()  # Force caching

# Use the data...

cached_df.unpersist()
```

# Troubleshooting Memory Issues
When memory issues aren't clearly identified in logs, follow these steps:

1 - **Enable verbose logging**
``` scala
spark.conf.set("spark.executor.extraJavaOptions", "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps")
```

2 - **Use Spark UI** to:
- Check the "Storage" tab for cached RDDs/DataFrames and memory usage
- Review "Executors" tab for memory/GC metrics
- Examine "Stages" tab for task failures and spill metrics

3 - **Monitor executor metrics**:
``` scala
spark.conf.set("spark.metrics.conf.*.sink.jmx.class", "org.apache.spark.metrics.sink.JmxSink")
```

4 - **Analyze problematic operations**:

- Wide transformations (join, groupByKey, repartition)
- UDFs, especially in Python (which use additional process memory)
- Operations that trigger shuffles

5 - **Test with smaller datasets**:

- Gradually scale up to identify memory usage patterns
- Profile executor memory usage at each scale

6 - **Use explain()** to understand execution plans:
``` python
df.join(other_df, "key").groupBy("column").count().explain(True)
```
