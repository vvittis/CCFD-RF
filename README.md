# [CCFD-RF] Credit Card Fraudulent Detection with Random Forest

This is a project for Credit Card Fraudulent Detection with Random Forest using Spark Structured Streaming

## In the code:
There are 3 options if you want to run CCFD-RF 
1. **Option 1:** Run job locally, reading from a file and writing to console
2. **Option 2:** Run job locally, reading from a kafka source and writing to a kafka sink
3. **Option 3:** Run job in SoftNet cluster, reading from HDFS and writing to HDFS

### Option 1 & 2 Run locally:
<pre>
In line 25-30 [StructuredRandomForest]: Configure SparkSession variable
</pre>
```scala
    val spark = SparkSession.builder()
      .appName("SparkStructuredStreamingExample")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "checkpoint_saves/")
      .getOrCreate()
```
### Option 3 Run on the cluster:
<pre>
In line 25-30 [StructuredRandomForest]: Configure SparkSession variable
</pre>
```scala
    val spark = SparkSession.builder()
       .appName("SparkStructuredRandomForest")
       .config("spark.sql.streaming.checkpointLocation", "/user/vvittis")
       .getOrCreate()
```
<pre>
In line 35-43 [StructuredRandomForest]: Read from Source
</pre>
### Option 1:
```scala
 val rawData = spark.readStream.text("dataset_source/")
```
### Option 2:
```scala
 val rawData = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "testSource")
          .option("startingOffsets", "earliest")
          .load()
          .selectExpr("CAST(value AS STRING)")
```
### Option 3:
```scala
 val rawData = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "testSource")
          .option("startingOffsets", "earliest")
          .load()
          .selectExpr("CAST(value AS STRING)")
```
## RUN the project. 

### If you have Intellij 
<pre>
Step 1: Clone CCFD-RF File > New > Project From Version Control... 
Step 2: In the URL: copy https://github.com/vvittis/CCFD-RF.git 
        In the Directory: Add your preferred directory
Step 3: Go to src > main > scala > StructuredRandomForest.scala and click Run
</pre>
#### A typical Console showing the state: 
![alt text](images_readme/Job1_locally_run_showing_init_trees.JPG)
#### A typical Console showing the output: 
![alt text](images_readme/Job1_locally_run_showing_some_typical_results.PNG)

Licensed under the [MIT Licence](LICENSE).

