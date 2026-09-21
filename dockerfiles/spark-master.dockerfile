FROM apache/spark:4.2.0 

ARG SPARK_MASTER_HADOOP_AWS_VERSION
ARG SPARK_MASTER_AWS_SDK_VERSION
ARG SPARK_MASTER_PORT
ARG SPARK_MASTER_WEBUI_PORT
ARG SPARK_MASTER_POSTGRES_JDBC_VERSION
ARG HOME
ARG SPARK_NO_DAEMONIZE

ENV HADOOP_AWS_VERSION=${SPARK_MASTER_HADOOP_AWS_VERSION}
ENV AWS_SDK_VERSION=${SPARK_MASTER_AWS_SDK_VERSION}
ENV SPARK_HADOOP_FS_S3A_IMPL=org.apache.hadoop.fs.s3a.S3AFileSystem
ENV SPARK_CONNECT_OPTS="--packages org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION},com.amazonaws:aws-java-sdk-bundle:${AWS_SDK_VERSION}"
ENV HOME=${HOME}
ENV SPARK_MODE=MASTER
ENV SPARK_NO_DAEMONIZE=${SPARK_NO_DAEMONIZE}
ENV SPARK_MASTER_PORT=${SPARK_MASTER_PORT}
ENV SPARK_MASTER_WEBUI_PORT=${SPARK_MASTER_WEBUI_PORT}
ENV SPARK_MASTER_POSTGRES_JDBC_VERSION=${SPARK_MASTER_POSTGRES_JDBC_VERSION}

RUN curl -sS https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -sS https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar  \ 
    curl -sS https://repo1.maven.org/maven2/org/postgresql/postgresql/${SPARK_MASTER_POSTGRES_JDBC_VERSION}/postgresql-${SPARK_MASTER_POSTGRES_JDBC_VERSION}.jar \
    -o /opt/spark/jars/postgresql-${SPARK_MASTER_POSTGRES_JDBC_VERSION}.jar

EXPOSE 7077
EXPOSE 8080
EXPOSE 4040
EXPOSE 4041