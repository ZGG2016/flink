[TOC]

**版本：flink-1.10.1**

#### jobmanager.heap.size

```yaml
# The heap size for the JobManager JVM

jobmanager.heap.size: 1024m
```

JobManager 是运行在节点上的一个 JVM 进程，这就是它所使用的堆内存的大小。

#### taskmanager.memory.process.size

```yaml
# The total process memory size for the TaskManager.
#
# Note this accounts for all memory usage within the TaskManager process, including JVM metaspace and other overhead.

taskmanager.memory.process.size: 1728m

# To exclude JVM metaspace and overhead, please, use total Flink memory size instead of 'taskmanager.memory.process.size'.
# It is not recommended to set both 'taskmanager.memory.process.size' and Flink memory.
#
# taskmanager.memory.flink.size: 1280m
```

当前 TaskManager 使用的总的内存大小，包括了堆内存和非堆内存。

flink 计算过程中产生的状态存在非堆内存中。 

#### taskmanager.numberOfTaskSlots

```yaml
# The number of task slots that 【each TaskManager（是每个）】 offers. Each slot runs one parallel pipeline.

taskmanager.numberOfTaskSlots: 1
```

TaskSlot 相当于计算资源。通过划分多个 slot, 划分出多块资源，执行并行计算。

那么每个 slot 就可以运行一个单独的线程，所以有几个 slot 就能运行几个线程。

所以，这个配置项就是【这个】 taskmanager 的 slot 只有一个，只能运行一个线程。

这个配置项就是这个 taskmanager 的最大并行能力。

集群的最大并行能力就是 taskmanager 的数量乘以这里指定的值，所以为程序指定的并行度不要超过这个集群的最大并行能力，而不是这里指定的值。所以，`parallelism.default` 值不一定要比 `taskmanager.numberOfTaskSlots` 值小。

#### parallelism.default

```yaml
# The parallelism used for programs that did not specify and other parallelism.

parallelism.default: 1
```

程序的默认并行度，1 表示只有一个线程执行。

这个配置项和 `taskmanager.numberOfTaskSlots` 的区别就是，这个配置项是程序执行过程中实际的并行度，而 `taskmanager.numberOfTaskSlots` 是这个 taskmanager 的最大并行能力，在程序执行过程中的并行度不一定就是这个项指定的值。