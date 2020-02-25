#!/bin/bash
export app="org.ekstep.dp.task.PipelinePreprocessorSparkStreamTask"
export SPARK_KUBERNETES_IMAGE=registry.local:5000/pipeline-preprocessor-spark:0.0.1

spark_ui_port=4040
network_timeout=300s
# JOB_JAR=local:///Users/anand/Documents/sunbird-source/sunbird-data-pipeline/data-pipeline/pipeline-preprocessor-spark/target/pipeline-preprocessor-spark-0.0.1.jar
JOB_JAR=local:///opt/spark/jars/pipeline-preprocessor-spark-0.0.1.jar
# master=local\[*\]
# export master=yarn
export master=k8s://https://localhost:6443
export DRIVER_NAME="pipeline-processor-job-driver"
export DRIVER_PORT=35000
export NAMESPACE=spark-namespace
export SERVICE_ACCOUNT_NAME=spark

# default to 1G for driver memory
driver_memory="${driver_memory:-"512m"}"
# default number of executors to 1
num_executors="${num_executors:-"1"}"
# default executor memory to 1G
executor_memory="${executor_memory:-"512m"}"
# default executor memory overhead to 1G
# executor_memory_overhead="${executor_memory_overhead:-"1024"}"
# default to use 40% of executor memory for storage
storage_memory_fraction="${storage_memory_fraction:-"0.4"}"
# default to use 60% of executor memory for shuffle
shuffle_memory_fraction="${shuffle_memory_fraction:-"0.6"}"

#log4j="${LIB_JAR_PATH}/log4j.properties"
log4j="log4j.properties"
# -XX:InitiatingHeapOccupancyPercent=35
javaopts="${javaopts:-"-XX:+AggressiveOpts -XX:+UseG1GC -XX:OldSize=100m -XX:MaxNewSize=100m"}"

executor_conf=(
  "--conf" "spark.storage.memoryFraction=${storage_memory_fraction}"
  "--conf" "spark.shuffle.memoryFraction=${shuffle_memory_fraction}"
  "--conf" "spark.executor.extraJavaOptions=${javaopts} -Dlog4j.configuration=file://${log4j}"
  # "--conf" "spark.yarn.executor.memoryOverhead=${executor_memory_overhead}"
  "--conf" "spark.executor.memory=${executor_memory}"
)

# yarn

#set -x
#"${SPARK_HOME}/bin/spark-submit" \
#  --master ${master} \
#  --deploy-mode cluster \
#  --verbose \
#  --conf spark.ui.port=4040 \
#  --conf spark.port.maxRetries=10 \
#  --conf spark.rdd.compress=true \
#  --conf spark.network.timeout="${network_timeout:-"300s"}" \
#  --conf spark.executor.instances=1 \
#  "${executor_conf[@]}" \
#  --driver-java-options "${javaopts}" \
#  --driver-memory "${driver_memory}" \
#  --class "${app}" \
#  "${JOB_JAR}" "${@}"

# kubernetes

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