#!/bin/bash
export app="org.ekstep.dp.task.DeduplicationStreamTask"
spark_ui_port=4040
network_timeout=300s
JOB_JAR=/Users/anand/Documents/sunbird-source/sunbird-data-pipeline/data-pipeline/de-duplication-spark/target/de-duplication-spark-0.0.1.jar

# default to 1G for driver memory
driver_memory="${driver_memory:-"1G"}"
# default number of executors to 4
# num_executors="${num_executors:-"4"}"
# default executor memory to 2G
# executor_memory="${executor_memory:-"2G"}"
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
  # "--conf" "spark.executor.memory=${executor_memory}"
)

set -x
"${SPARK_HOME}/bin/spark-submit" \
  --master local\[*\] \
  --verbose \
  --conf spark.ui.port="${spark_ui_port}" \
  --conf spark.port.maxRetries=10 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.rdd.compress=true \
  --conf spark.network.timeout="${network_timeout:-"300s"}" \
  "${executor_conf[@]}" \
  --driver-java-options "${javaopts}" \
  --driver-memory "${driver_memory}" \
  --class "${app}" \
  "${JOB_JAR}"