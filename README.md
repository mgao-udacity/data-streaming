# data-streaming
1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

Some values affecting the throughput and latency are the number of threads in master, spakr.driver.cores, spark.driver.memory, spark.executor.memory. By increasing the memory it allows us to process more data in a batch, thus can increase the throughput and decrease latency. By increasing spark.default.parallelism it helps do operations like reduceByKey and join more efficiently. spark.streaming.kafka.maxRatePerPartition limits the maximum records processed per second, a higher value can increase throughtput. spark.sql.shuffle.partitions configures number of partitions to use when shuffling data for joins or aggregations.

2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

One property is setting the spark.executor.memory to 8g. This helps reduce the time processing each task. Increasing the memory to 16g does not speed up the task. Memory of 8g is enough for processing this job. Adding more memory seems redundant. 

Another property is setting the spark.default.parallelism to 6. The workspace has 2 cpu cores, and it's recommended to let each worker has 2-3 tasks. So the parallelism set to 6 is best. It gives enough parallelism while not too much for the cpu the schedule many small tasks.