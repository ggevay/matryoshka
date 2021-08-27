# Matryoshka

This repository contains the material to run the experiments for the Matryoshka paper, which explores nested parallelism in distributed dataflow systems, such as Spark:
<https://www.researchgate.net/publication/350021175_The_Power_of_Nested_Parallelism_in_Big_Data_Processing_--_Hitting_Three_Flies_with_One_Slap>

Build with `mvn clean package -DskipTests`

You will need a cluster with
* Spark 3.1.0
* Java 14
* HDFS (for Spark checkpoints)
* pdsh

The `scripts` directory contains Bash scripts to run the experiments.

You should grep for the string `TOBEFILLED` to fill in machine names, directories, etc. (both in scripts and the Scala sources)

The DIQL experiment is in `DIQL/tests/spark/BounceRateDIQL.scala`. (Please refer to DIQL's own documentation on how to build DIQL.)
