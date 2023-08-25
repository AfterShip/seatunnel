FROM us.gcr.io/aftership-admin/jenkins/java-base:maven-3.6.3-jdk-8 as builder

ENV SEATUNNEL_SOURCE="/opt/seatunnel/source"

RUN mkdir -p ${SEATUNNEL_SOURCE}
COPY ./ ${SEATUNNEL_SOURCE}/
WORKDIR ${SEATUNNEL_SOURCE}
RUN mvn clean package -pl seatunnel-dist -am -T 1C -Dmaven.test.skip=true -Dmaven.compile.fork=true

FROM us-docker.pkg.dev/aftership-admin/cloud-build/other:spark-v3.3.2

ENV SEATUNNEL_HOME="/opt/seatunnel"
ENV SPARK_HOME="/opt/spark"
ENV CONFIG_CENTER_HOME="/opt/config_center"
ENV SPARK_CONF_HOME=${SPARK_HOME}/conf
ENV GCS_CONNECTOR_HADOOP_VERSION="2.2.12"
ENV SPARK_METRIC_VERSION="1.0.1"
ENV SEATUNNEL_SOURCE="${SEATUNNEL_HOME}/source/"
ENV SEATUNNEL_DIST="${SEATUNNEL_SOURCE}/seatunnel-dist"
ENV SEATUNNEL_TOOL="${SEATUNNEL_SOURCE}/tools"

USER root
RUN apt-get update && apt-get -y --force-yes install wget && apt-get install python3.6
RUN wget -P ${SPARK_HOME}/jars/ https://nexus.automizely.org/repository/maven-releases/com/google/cloud/bigdataoss/gcs-connector/hadoop3-${GCS_CONNECTOR_HADOOP_VERSION}/gcs-connector-hadoop3-${GCS_CONNECTOR_HADOOP_VERSION}-shaded.jar
RUN wget -P ${SPARK_HOME}/jars/ https://nexus.automizely.org/repository/maven-releases/com/automizely/data/data-dw-integration-spark-metric/${SPARK_METRIC_VERSION}/data-dw-integration-spark-metric-${SPARK_METRIC_VERSION}.jar

RUN mkdir -p ${SEATUNNEL_HOME}
RUN chmod 755 ${SEATUNNEL_HOME}
RUN mkdir ${SPARK_CONF_HOME}
RUN mkdir ${CONFIG_CENTER_HOME}
COPY --from=builder ${SEATUNNEL_SOURCE}/config/metrics.properties ${SPARK_CONF_HOME}
COPY --from=builder ${SEATUNNEL_SOURCE}/config/jmxCollector.yaml ${SPARK_CONF_HOME}
COPY --from=builder ${SEATUNNEL_SOURCE}/config/podtemplate.yaml ${SPARK_CONF_HOME}
COPY --from=builder ${SEATUNNEL_DIST}/target/apache-seatunnel*bin.tar.gz ${SEATUNNEL_HOME}/
COPY --from=builder ${SEATUNNEL_TOOL}/config_center/ ${CONFIG_CENTER_HOME}/

WORKDIR ${SEATUNNEL_HOME}
RUN tar -zxvf ./apache-seatunnel*bin.tar.gz --strip-components 1 -C ./
RUN rm ${SEATUNNEL_HOME}/apache-seatunnel*bin.tar.gz
