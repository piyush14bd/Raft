# MapReduce: Simplified Data Processing on Large Clusters

## Overview
MapReduce is a programming model and an associated implementation designed to process and generate large datasets. It abstracts the complexities of parallelization, fault tolerance, data distribution, and load balancing, allowing programmers to easily utilize the resources of large distributed systems. The model is particularly useful for processing massive datasets, often running on clusters of thousands of machines.

> **Note:** The following sections primarily describe the ideal implementation of MapReduce. In my specific implementation, certain aspects differ from this standard model. These differences are highlighted in the respective sections.

## Programming Model

### Map Function
- **Input:** A file
- **Output:** A set of intermediate key/value pairs.

The Map function processes input data by producing intermediate key/value pairs. These pairs are then grouped by key and passed to the Reduce function.

### Reduce Function
- **Input:** An intermediate key and a list of values associated with that key.
- **Output:** A final output, usually a reduced set of values.

The Reduce function merges all intermediate values associated with the same key to generate a smaller set of values, often producing one output per key.

## Execution Flow

1. **Input Splitting:** The MapReduce library splits the input data into multiple pieces (M tasks).

2. **Task Distribution:** A master program assigns Map and Reduce tasks to worker machines across a cluster.

3. **Map Execution:** Map workers process the input splits, generate intermediate key/value pairs, and store them locally, partitioned into R regions.

4. **Shuffling and Sorting:** Reduce workers fetch the intermediate data, sort it by key, and group identical keys together.

5. **Reduce Execution:** Reduce workers process the grouped data, producing the final output, which is stored in R output files.

## Fault Tolerance
MapReduce is designed to handle machine failures gracefully:

- **Worker Failure:** The master periodically pings workers and if a response isn't received within a set timeframe, workers are marked as failed. Its tasks are reassigned to other available workers. Map tasks are re-executed if necessary, while reduce tasks typically do not need re-execution as their output is stored on a global file system.
  
**In my implementation,** The master does not actively ping the workers. Instead, it employs a ticker mechanism that triggers a callback function every second to identify tasks that have been running for more than a specified time. These tasks are then marked as pending and made available for other workers to pick up.

- **Master Failure:** The master periodically writes checkpoints. If the master fails, a new master can be started from the last checkpoint.

**In my implementation,** this logic to handle master failure is not present right now.

## Performance and Scalability
MapReduce is highly scalable, with the ability to handle computations involving thousands of machines and terabytes of data. Its design ensures efficient load balancing, fault tolerance, and data locality optimization, making it suitable for a wide range of large-scale data processing tasks.

## Conclusion
MapReduce simplifies large-scale data processing by abstracting the complexities of distributed systems. Its ease of use and robust fault tolerance make it an ideal choice for a variety of data-intensive applications.

## References
Dean, Jeffrey and Ghemawat, Sanjay, "MapReduce: Simplified Data Processing on Large Clusters," OSDI'04: Sixth Symposium on Operating System Design and Implementation, San Francisco, CA.
