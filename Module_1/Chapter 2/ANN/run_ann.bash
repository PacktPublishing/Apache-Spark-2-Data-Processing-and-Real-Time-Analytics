#!/bin/bash

/home/hadoop/spark/spark/bin/spark-submit \
  --class $1 \
  --master spark://hc2nn.semtech-solutions.co.nz:8077  \
  --executor-memory 700M \
  --total-executor-cores 100 \
  /home/hadoop/spark/ann/target/scala-2.10/a-n-n_2.10-1.0.jar \
  1000

