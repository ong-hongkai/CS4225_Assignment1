#!/bin/bash
#DO NOT MODIFIY!!!
#SBATCH --job-name=ASSIGN_0
#SBATCH -o ASSIGN_0.out

echo "Java path $JAVA_HOME"
echo "Hadoop path $HADOOP_HOME"

echo "Compiling assignment 0"
hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class
echo "Uploading input files"
hdfs dfs -mkdir -p ./wordcount/input
hdfs dfs -put file01.txt file02.txt ./wordcount/input
echo "Clear previous output folder"
hdfs dfs -rm -r -f ./wordcount/output
echo "Submit job"
hadoop jar wc.jar WordCount ./wordcount/input ./wordcount/output
echo "Job finished. Print results."
hdfs dfs -cat ./wordcount/output/part-r-00000
if [[ "$(hdfs dfs -cat wordcount/output/part-r-00000)" == "$(cat answer.txt)" ]]
then
  echo "Test passed."
else
  echo "Wrong answer."
fi
