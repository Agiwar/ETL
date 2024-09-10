# builder step used to download and configure spark environment
FROM openjdk:11.0.11-jre-slim-buster as builder

# Add Dependencies for PySpark
RUN apt-get update && \
    apt-get install -y cron curl vim wget software-properties-common ssh net-tools ca-certificates python3 python3-pip

# Python packages
COPY requirements.txt .

RUN pip3 install --upgrade pip setuptools && \
    pip3 install -r requirements.txt

# Fix the value of PYTHONHASHSEED
ENV SPARK_VERSION=3.3.2 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PYTHONHASHSEED=1 \
    PYTHONPATH=${PYTHONPATH}:/opt/spark-jobs \
    TZ=Asia/Taipei

# Download and uncompress Spark from the Apache archive
RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    mkdir -p /opt/spark && \
    tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 && \
    rm -f apache-spark.tgz


# Apache spark environment
FROM builder as apache-spark

WORKDIR /opt/spark

# Download MSSQL connectors whose version is 12.2.0.jre11
RUN wget --no-verbose -O mssql-jdbc-12.2.0.jre11.jar "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.2.0.jre11/mssql-jdbc-12.2.0.jre11.jar" && \
    cp mssql-jdbc-12.2.0.jre11.jar /opt/spark/jars && \
    rm -f mssql-jdbc-12.2.0.jre11.jar

# Download MySQL connectors whose version is 8.0.20
RUN wget --no-verbose -O mysql-connector-java-8.0.20.tar.gz "https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.20.tar.gz" && \
    tar zxvf mysql-connector-java-8.0.20.tar.gz && \
    cp mysql-connector-java-8.0.20/mysql-connector-java-8.0.20.jar /opt/spark/jars && \
    rm -f mysql-connector-java-8.0.20.tar.gz && \
    rm -rf mysql-connector-java-8.0.20

# Download ClickHouse connectors whose version is 0.3.2-patch11
RUN wget --no-verbose -O clickhouse-jdbc-0.3.2-patch11-all.jar "https://github.com/ClickHouse/clickhouse-java/releases/download/v0.3.2-patch11/clickhouse-jdbc-0.3.2-patch11-all.jar" && \
    cp clickhouse-jdbc-0.3.2-patch11-all.jar /opt/spark/jars && \
    rm -f clickhouse-jdbc-0.3.2-patch11-all.jar

ENV SPARK_MASTER_PORT=7077 \
    SPARK_MASTER_WEBUI_PORT=8080 \
    SPARK_LOG_DIR=/opt/spark/logs \
    SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
    SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
    SPARK_WORKER_WEBUI_PORT=8080 \
    SPARK_WORKER_PORT=7000 \
    SPARK_MASTER="spark://spark-master:7077" \
    SPARK_WORKLOAD="master"

EXPOSE 8080 7077 6066

RUN mkdir -p $SPARK_LOG_DIR && \
    touch $SPARK_MASTER_LOG && \
    touch $SPARK_WORKER_LOG && \
    ln -sf /dev/stdout $SPARK_MASTER_LOG && \
    ln -sf /dev/stdout $SPARK_WORKER_LOG

COPY start-spark.sh /

CMD ["/bin/bash", "/start-spark.sh"]