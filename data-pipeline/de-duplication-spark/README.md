# Deduplication Spark

This job is used to de-duplicate data by running the job using Spark Streaming. It can be run either on local mode, Yarn or Kubernetes.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a yarn or kubernetes.

### Prerequisites

1. Download spark-2.4.4 from [spark-download](https://www.apache.org/dyn/closer.lua/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz). 
2. export SPARK_HOME=`SPARK_EXTRACTED_DIR` either in .bashrc or current execution shell.
4. Docker installed.
5. A running yarn cluster or a kubernetes cluster.

### Build

mvn clean install

## Deployment

### Local mode

Change the following parameters in the [submit_job.sh bash script](https://github.com/anandp504/sunbird-data-pipeline/blob/kubernetes/data-pipeline/de-duplication-spark/submit_job.sh) 

1. JOB_JAR: Location of the binary de-duplication-spark.jar file
2. Change driver_memory, storage_memory_fraction and shuffle_memory_fraction according to the data requirements.

```
./submit_job.sh
```

### Yarn

Change the following parameters in the [submit_job.sh bash script](https://github.com/anandp504/sunbird-data-pipeline/blob/kubernetes/data-pipeline/de-duplication-spark/submit_job.sh) 

1. JOB_JAR: Location of the binary de-duplication-spark.jar file
2. --master yarn
3. Change driver_memory, storage_memory_fraction and shuffle_memory_fraction according to the data requirements.

```
./submit_job.sh
```