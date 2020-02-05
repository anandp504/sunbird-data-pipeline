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

```bash
./submit_job.sh
```

### Yarn

Change the following parameters in the [submit_job.sh bash script](https://github.com/anandp504/sunbird-data-pipeline/blob/kubernetes/data-pipeline/de-duplication-spark/submit_job.sh) 

1. JOB_JAR: Location of the binary de-duplication-spark.jar file
2. --master yarn
3. Change driver_memory, storage_memory_fraction and shuffle_memory_fraction according to the data requirements.

```bash
./submit_job.sh
```

### Kubernetes

#### Build docker image with spark distribution and application jar

```bash
    # Create a local docker registry to publish the docker image with both 
    # spark and the application jar

    docker container run -d --name registry.local -v /data/docker_registry:/var/lib/registry -p 5000:5000 registry:2

    # Add the following entry into /etc/hosts file (You will need sudo access 
    #	to edit the file)

    <host_ip>	registry.local

    # We will need to build a docker image with Spark and the application jar. 
    # Follow the steps mentioned below to build a docker image.

    # spark-2.4.4 comes with kubernetes model and client jar version 4.1.2. 
    # However, there is a bug which throws the following exception:

    io.fabric8.kubernetes.client.KubernetesClientException: 
      at io.fabric8.kubernetes.client.dsl.internal.WatchConnectionManager$2.onFailure(WatchConnectionManager.java:188)
      at okhttp3.internal.ws.RealWebSocket.failWebSocket(RealWebSocket.java:543)
      at okhttp3.internal.ws.RealWebSocket$2.onResponse(RealWebSocket.java:185)

    # To resolve this issue, we will need to replace the kubernetes client and 
    # model jars with version 4.4.2 or use spark-3.0.0.

    rm -f <spark_2.4.4_extracted_dir>/jars/kubernetes*.jar
    curl -Lo <spark_2.4.4_extracted_dir>/jars/kubernetes-client-4.4.2.jar https://repo1.maven.org/maven2/io/fabric8/kubernetes-client/4.4.2/kubernetes-client-4.4.2.jar
    curl -Lo <spark_2.4.4_extracted_dir>/jars/kubernetes-model-common-4.4.2.jar https://repo1.maven.org/maven2/io/fabric8/kubernetes-client/4.4.2/kubernetes-model-common-4.4.2.jar
    curl -Lo <spark_2.4.4_extracted_dir>/jars/kubernetes-model-4.4.2.jar https://repo1.maven.org/maven2/io/fabric8/kubernetes-client/4.4.2/kubernetes-model-4.4.2.jar

    # Copy the de-duplication-spark-0.0.1.jar from the spark-streaming job build 
    target directory to the <spark_2.4.4_extracted_dir>

    # Add the following line after COPY ${spark_jars} /opt/spark/jars in 
    # <spark_2.4.4_extracted_dir>/kubernetes/dockerfiles/spark/Dockerfile. 
    # This will ensure that the application jar gets copied into the 
    # ${SPARK_HOME}/jars directory inside the docker image

    COPY de-duplication-spark-0.0.1.jar /opt/spark/jars

    # Use the docker-image-tool.sh within the spark distribution binary to 
    build a docker image. I have give a tag with just the spark version in this case.

    ./bin/docker-image-tool.sh -t 2.4.4 -f kubernetes/dockerfiles/spark/Dockerfile build

    # Tag the image again with the registry.local (local docker registry we created 
    # above) and push the image to the local registry. This image will then be 
    # available for the pods to pull from. If we don't perform this step, the 
    # image will be pulled from docker.io docker hub.

    docker tag spark:2.4.4 registry.local:5000/de-duplication-spark:0.0.1
    docker push registry.local:5000/de-duplication-spark:0.0.1
```

#### Create a cluster and service account to run the spark application

```bash
    # Create a cluster to run the spark application. Please note that we use the 
    # registry-name, registry-port and --enable-registry to make sure that this 
    # cluster can pull images from the local registry. Also, note that 
    # the --workers=2 has been specified since we need to run the spark-driver 
    # and spark-executor on two different workers.

    k3d create --server-arg --no-deploy --server-arg traefik --registry-name registry.local --registry-port 5000 --enable-registry --name spark-cluster --workers=2 --image rancher/k3s:v1.0.0

    # Create a separate namespace and also a service account spark. This service 
    account will be given a clusterrole edit. This step is necessary for the 
    spark-job-driver to create a new pod to run the executor.

    export KUBECONFIG="$(k3d get-kubeconfig --name='spark-cluster')"
    kubectl create namespace spark-namespace
    kubectl create serviceaccount spark --namespace=spark-namespace
    kubectl create rolebinding spark-role --clusterrole=edit --serviceaccount=spark-namespace:spark --namespace=spark-namespace
```

#### Submiting the spark streaming job using spark-submit to the k8s cluster

```bash
    # Run the https://github.com/anandp504/sunbird-data-pipeline/blob/kubernetes/data-pipeline/de-duplication-spark/submit_job.sh
    # to submit the job to the kubernetes cluster
    # export KUBECONFIG="$(k3d get-kubeconfig --name='spark-cluster')"

    ./submit_job.sh

    # Execute the following command to check the pods running for the spark job

    kubectl get all -n="spark-namespace"
    NAME                                                                  READY   STATUS    RESTARTS   AGE
    pod/dedup-spark-job-driver                                            1/1     Running   0          2m7s
    pod/org-ekstep-dp-task-deduplicationstreamtask-1580898795208-exec-1   1/1     Running   0          115s

    NAME                                     TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)             AGE
    service/spark-1580898796079-driver-svc   ClusterIP   None         <none>        7078/TCP,7079/TCP   2m6s
```