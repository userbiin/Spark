# Project 2: Better Inverted Index using Spark

* Author: Subin Min
* Class: CS435-535 Section 1
* Semester: Fall 2026

## Overview
This project improves the inverted index MapReduce example using Spark. The original version emits, for each word, every filename in which the word appears — including duplicates, so a word occur many copies of that filename. This version instead counts the occurrences of each word per file and emits each filename once, prefixed by its count. The filenames for a given word are ordered by count in descending order, and filenames with equal counts are ordered ascending by filename. The program is written in Python and runs under Apache Spark.

## Reflection

At first, I treated the output of `wholeTextFiles` as line-by-line strings, the same way as in the wordcount example. Cells that only chained transformations finished without any error, so I mistakenly thought the code was correct. The actual error showed up only when I called an action such as `take()`. This was a good way to see Spark's lazy evaluation in practice.

The most interesting part on the performance side was a result that was the opposite of Project 1. In Project 1, the cluster was 8.4x faster than the local machine, but in Project 2 the 8-node cluster (51.4 s) was initially slower than the local run on my Mac (27.1 s). When I broke down the time using the logs, the difference in YARN startup was only about 2.5 s, and most of the difference came from the first stage (reading files, splitting words, `reduceByKey`). When I extracted task-level statistics from the event log, I found that the 594 small files on HDFS had been combined into only 9 input splits, so only 9 of the 32 executor cores were doing work. Increasing only the number of shuffle partitions to 64 did not make it faster (58.6 s), but increasing the input splits to 44 with `spark.files.maxPartitionBytes=8m` brought the time down to 33.4 s, about 35% less. Through this process, I learned that in Spark, the number of partitions determines the actual parallelism more than how many executors and cores are allocated.

## Compiling and Using

This program is written in Python (PySpark), so no compilation is needed. It runs with `spark-submit` on Apache Spark 4.2 and Python 3.11.

```
spark-submit [options] BetterInvertedIndex_P2.py <input directory> <output directory>
```

### Local run (Mac, using all cores)

```bash
rm -rf ~/p2-out
time spark-submit --master "local[*]" BetterInvertedIndex_P2.py file://$HOME/etext-all file://$HOME/p2-out
```

### Cluster run (cscluster00, YARN)

```bash
ln -s /home/amit/CS435-535-data/etext-data/etext-all ~/etext-all
hdfs dfs -put ~/etext-all etext-all

hdfs dfs -rm -r -f p2-out
time spark-submit --master yarn --num-executors 8 --executor-cores 4 \
    --conf spark.files.maxPartitionBytes=8m \
    BetterInvertedIndex_P2.py etext-all p2-out

hdfs dfs -cat p2-out/part-* | head
```

`--conf spark.files.maxPartitionBytes=8m` is a setting that splits the small files into smaller input splits to increase parallelism. The reason this setting is needed is explained in the Testing and Results sections.

## Testing

### 1. Correctness Testing

1. **Step-by-step check on a small dataset.** In a Jupyter notebook, I used the word count example input (14 files) and checked the shape of the intermediate results with `take()` and `filter()` at each step: `flatMap` → `reduceByKey` → `map` → `groupByKey` → sorting → output formatting.

2. **Checking the sorting rule.** I checked the example words from the assignment (`people`, `permit`, etc.) directly, and verified that filenames with the same count are sorted in ascending order by filename.

3. **Comparison with Project 1 (small dataset).** I sorted both outputs and compared them with `diff`. Both outputs had 80,436 lines, and their contents matched exactly.

4. **Comparison with Project 1 (full dataset).** I compared the number of output lines from Project 2 on the full dataset (594 files, 408 MB) with the Reduce output records of Project 1 (420,968).

    | Version | Input method | Output lines |
    |---|---|---|
    | Project 1 MapReduce | Hadoop Streaming (`iso-8859-1`) | 420,968 |
    | Project 2 initial version | `wholeTextFiles` (UTF-8) | 433,085 |
    | Project 2 final version (Mac) | `binaryFiles` + `iso-8859-1` | 420,968 |
    | Project 2 final version (cluster) | `binaryFiles` + `iso-8859-1` | 420,968 |

    The 12,117-line difference in the initial version was caused by encoding. When decoding as UTF-8, `iso-8859-1` bytes (e.g., `0xE9`) are turned into a replacement character, and since this character is not a word character in the regular expression, it creates word boundaries that did not exist in Project 1. After changing the input to be decoded as `iso-8859-1`, both the Mac and the cluster produced the same number of lines as Project 1.

### 2. Performance Testing

#### Experiment environment

| | Local | Cluster |
|---|---|---|
| Machine | MacBook Air, Apple M4 (10 cores), 24GB RAM | cscluster00 Hadoop YARN cluster |
| Spark setting | `local[*]` (10 threads inside the driver) | 8 executors × 4 cores = 32 cores |
| Input | `etext-all` (594 files, 408 MB), local file system | Same data, HDFS |


#### 2-1. First experiment results

| Environment | Project 1 MapReduce | Project 2 Spark | Project 2 speedup |
|---|---|---|---|
| Local (Mac) | 813.85 s | 27.1 s | 30.0x |
| Cluster | 96.78 s | 51.4 s | 1.9x |

Spark was faster than MapReduce in both environments. However, unlike Project 1, where the cluster was 8.4x faster than the local machine, in Project 2 **the 32-core cluster was 1.9x slower than the 10-core Mac.**

#### 2-2. Problem and hypotheses

**Problem:** The cluster is slower than the Mac even though it has more than 3x as many cores.

| # | Hypothesis | How to check |
|---|---|---|
| A | YARN startup overhead (container allocation, executor registration) is large | Separate startup time and computation time using log timestamps |
| B | Not all of the requested executors were allocated | Count the `Registered executor` lines in the log |
| C | Shuffle and saving are slow because they go through the network between nodes and HDFS replication | Compare per-stage times in the log with the Mac |
| D | Stage 0 has too few tasks to use all the cores | Check the number of tasks per stage with the event log |

#### 2-3. Experiment results for each hypothesis

**Experiment 1: Log timeline comparison (default setting)**

| Phase | Mac `local[*]` | Cluster YARN |
|---|---|---|
| Startup (SparkContext created → first job starts) | 8.4 s | 10.9 s |
| Stage 0: reading files + splitting words + `reduceByKey` | 15.8 s | 22.6 s |
| Stage 1: `groupByKey` | 1.3 s | 3.8 s |
| `sortByKey` (2 jobs to compute partition ranges) | 0.9 s | 3.2 s |
| Sort shuffle + saving | 1.2 s | 5.5 s |
| Total computation | 19.2 s | 35.2 s |

All 8 requested executors were allocated and placed on 7 nodes.

**Experiment 2: Task-level analysis with the event log (cluster)**

I enabled the event log with `spark.eventLog.enabled=true` and used a script I wrote to extract the number of tasks per stage and the task time distribution.

| Setting | Stage 0 tasks | Stage 0 median | Stage 0 max | Later stage tasks | real |
|---|---|---|---|---|---|
| Default (`minPartitions=64`) | 9 | 15.0 s | 22.3 s | 9 | 51.4 s |
| `spark.default.parallelism=64` | 9 | 15.6 s | 22.4 s | 64 | 58.6 s |
| `spark.files.maxPartitionBytes=8m` | 44 | 4.4 s | 12.2 s | 44 | 33.4 s |

- **Default setting:** Although I set `minPartitions=64` in the code, the 594 small files on HDFS were combined into 9 input splits. The later stages inherited the same number of partitions, so every stage ran with 9 tasks.
- **`spark.default.parallelism=64`:** This setting only increases the number of partitions for the stages after `reduceByKey`. The later stages increased to 64 tasks, but Stage 0 stayed at 9.
- **`spark.files.maxPartitionBytes=8m`:** This setting reduces the maximum size that one input split can hold from the default 128 MB to 8 MB. Stage 0 was split into 44 tasks and spread across 7 nodes.

**Hypothesis results**

| # | Hypothesis | Result | Evidence |
|---|---|---|---|
| A | YARN startup overhead | Rejected | The startup time difference was about 2.5 s (8.4 s vs 10.9 s), only a small part of the total difference (24 s) |
| B | Not enough executors | Rejected | All 8 requested executors were allocated |
| C | Network and HDFS cost of shuffle and saving | △ Partially supported | The small stages were 3–7x slower on the cluster, but their absolute time added up to only about 9 s |
| D | Too few tasks in Stage 0 | Supported (main cause) | With only 9 tasks, only 9 of the 32 cores were used. Increasing only the shuffle partitions had no effect (58.6 s), but increasing the input splits to 44 reduced the time from 51.4 s to 33.4 s, a 35% decrease |

## Results

### Final running times

| Environment | Project 1 MapReduce | Project 2 Spark | Project 2 speedup |
|---|---|---|---|
| Local (Mac, `local[*]`) | 813.85 s | 27.1 s | 30.0x |
| Cluster (default setting) | 96.78 s | 51.4 s | 1.9x |
| Cluster (`maxPartitionBytes=8m`) | 96.78 s | 33.4 s | 2.9x |

The output was 420,968 lines in every environment, matching Project 1.

### Final conclusions

1. **Spark was faster than MapReduce in both environments.** The difference was especially large locally, at 30x. Project 1 launched a new container and Python process for each of the 594 map tasks and wrote intermediate results to disk, whereas Spark reuses Python workers inside a single JVM, processes operations that do not need a shuffle back-to-back within one stage, and reuses already-computed shuffle results in later jobs.

2. **The main reason the cluster was slower than the Mac was the number of input partitions.** YARN startup was not the cause; the key issue was that the small files on HDFS were combined into 9 input splits, so only 9 of the 32 cores were working. Increasing only the number of shuffle partitions had no effect, and Stage 0, the bottleneck, only became faster when the input splits were increased. In other words, in Spark, **the number of partitions determines the actual parallelism** more than how many executors and cores are allocated. This is the same principle as in Project 1, where the number of map tasks running at the same time (4.7 locally vs 50.5 on the cluster) made the 8.4x difference.



## Sources used
- Apache Spark Examples — https://spark.apache.org/examples.html
- Spark API Reference — https://spark.apache.org/docs/latest/api/python/reference/index.html
- Claude (Anthropic) — used to translate the project specification, to translate my Korean draft of this README into English, to debug the code, to analyze the logs and validate my hypotheses, and to format the results into Markdown tables.
