Best Practices:
------------------
Keep 4-6 processor cores per executor.

6 machines with 64 GB RAM/machine and 16 processor cores per machine.

16*6 = 96 processor cores

64*6 = 384 GB RAM(Total)

96/4 = 24 executors each with 4 processor cores and 384/24 = 16 GB RAM per executor will be ideal cluster

In reality we will get around 10GB RAM and 4 cores.

You can very well load 10 partitions per executor.

Broadcast variable:
-----------------------
Varible whose value will be available across the cluster(each and every node).

Broadcast objects to the executors, such that they're always there whenever needed.

just use sc.braodcast to shipoff whatever you want.

the use .value() to get the object back.


How to run the job:
-------------------
spark-submit hello-sparkapp_2.11-1.0.jar --class HelloSpark ------> This will run the program in local mode

spark-submit hello-sparkapp_2.11-1.0.jar --class HelloSpark --master yarn--> This will run the program in yarn mode





