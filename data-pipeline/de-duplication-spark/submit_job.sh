#!/bin/bash
export app="org.ekstep.dp.task.DeduplicationStreamTask"
# export SPARK_KUBERNETES_IMAGE=registry.local:5000/de-duplication-spark:0.0.1
export SPARK_KUBERNETES_IMAGE=anandp504/de-duplication-spark:1.0

spark_ui_port=10020
network_timeout=300s
# JOB_JAR=local:///Users/anand/Documents/sunbird-source/sunbird-data-pipeline/data-pipeline/de-duplication-spark/target/de-duplication-spark-0.0.1.jar
JOB_JAR=local:///opt/spark/jars/de-duplication-spark-0.0.1.jar
# master=local\[*\]
# master=yarn
# export master=k8s://https://localhost:6443
export master=k8s://https://kube-clust-sunbird-devnew-e-d9908e-8596f01a.hcp.centralindia.azmk8s.io:443
export DRIVER_NAME="dedup-spark-job-driver"
export DRIVER_PORT=35000
export NAMESPACE=spark-namespace
export SERVICE_ACCOUNT_NAME=spark

# default to 1G for driver memory
driver_memory="${driver_memory:-"512m"}"
# default number of executors to 1
num_executors="${num_executors:-"1"}"
# default executor memory to 1G
executor_memory="${executor_memory:-"1024m"}"
# default executor memory overhead to 1G
# executor_memory_overhead="${executor_memory_overhead:-"1024"}"
# default to use 40% of executor memory for storage
storage_memory_fraction="${storage_memory_fraction:-"0.4"}"
# default to use 60% of executor memory for shuffle
shuffle_memory_fraction="${shuffle_memory_fraction:-"0.6"}"
# spark default parallelism
spark_default_parallelism="${spark_default_parallelism:-"2"}"

log4j="log4j.properties"
javaopts="${javaopts:-"-XX:+AggressiveOpts -XX:+UseG1GC -XX:OldSize=100m -XX:MaxNewSize=100m"}"

executor_conf=(
  "--conf" "spark.storage.memoryFraction=${storage_memory_fraction}"
  "--conf" "spark.shuffle.memoryFraction=${shuffle_memory_fraction}"
  "--conf" "spark.executor.extraJavaOptions=${javaopts} -Dlog4j.configuration=file://${log4j}"
  # "--conf" "spark.yarn.executor.memoryOverhead=${executor_memory_overhead}"
  "--conf" "spark.executor.memory=${executor_memory}"
  "--conf" "spark.default.parallelism=${spark_default_parallelism}"
)

# yarn

#set -x
#"${SPARK_HOME}/bin/spark-submit" \
#  --master ${master} \
#  --deploy-mode cluster \
#  --verbose \
#  --conf spark.ui.port="${spark_ui_port:-10020}" \
#  --conf spark.port.maxRetries=10 \
#  --conf spark.rdd.compress=true \
#  --conf spark.network.timeout="${network_timeout:-"300s"}" \
#  --conf spark.executor.instances=1 \
#  "${executor_conf[@]}" \
#  --driver-java-options "${javaopts}" \
#  --driver-memory "${driver_memory}" \
#  --class "${app}" \
#  "${JOB_JAR}" "${@}"

set -x
"${SPARK_HOME}/bin/spark-submit" \
  --master ${master} \
  --deploy-mode cluster \
  --verbose \
  --conf spark.port.maxRetries=10 \
  --conf spark.rdd.compress=true \
  --conf spark.network.timeout="${network_timeout:-"300s"}" \
  --conf spark.kubernetes.container.image=${SPARK_KUBERNETES_IMAGE} \
  --conf spark.kubernetes.driver.pod.name=${DRIVER_NAME} \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=${SERVICE_ACCOUNT_NAME}  \
  --conf spark.kubernetes.namespace=${NAMESPACE} \
  --conf spark.executor.instances=1 \
  "${executor_conf[@]}" \
  --driver-java-options "${javaopts}" \
  --driver-memory "${driver_memory}" \
  --class "${app}" \
  "${JOB_JAR}" "${@}"