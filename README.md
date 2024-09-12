# ETL

This repository contains two jobs which are data sync and data lineage respectively, where 

+ data sync is implemented on standalone PySpark cluster
+ perform data lineage SQL scripts in ClickHouse using Python's package.


## Data Sync
### General

Create PySpark standalone cluster with Docker and docker-compose on Ubuntu.

The docker compose will create the following containers:

| container      | exposed ports  |
| -------------- | -------------- |
| spark-master   | 9090 7077      |
| spark-worker-a | 9091           |
| spark-worker-b | 9092           |
| spark-worker-c | 9093           |
| spark-worker-d | 9094           |

### Installation

Follow the steps to run PySpark standalone clusters.

+ Prerequisites
    + Docker
    + docker compose
+ ssh `VM's IP`
+ go to `/home/admin/ETL`
+ Build the image
    ```bash
    docker build -t cluster-apache-spark:3.3.2 . 
    ```
+ Run the service 
    ```bash
    docker compose -f docker-compose.yml up -d
    ```
+ Stop the service
    ```bash
    docker compose down
    ```

### PySpark UI

+ PySpark Master: http://<VM's IP>:9090
+ PySpark worker a: http://<VM's IP>:9091
+ PySpark worker b: http://<VM's IP>:9092
+ PySpark worker c: http://<VM's IP>:9093
+ PySpark worker d: http://<VM's IP>:9094

### Resource Allocation
Please note that the resource allocation below is for spark cluster deployed on production environment. For local usage, you can reduce the number to fit your machine.

+ Default cores allocation for each spark worker is 4.
+ Default RAM for each spark-worker is 16 GB.
+ Default RAM for spark executors is 16 GB.
+ Default RAM for spark driver is 16 GB.

### Run

+ WORKDIR: `/opt/spark`
+ Run the ETL including data sync and data lineage.
    ```bash
    docker exec spark-master python3 opt/spark-jobs/etl/main.py
    ```
+ Run the data sync for specific table, e.g., my_table.
    ```bash
    docker exec spark-master bin/spark-submit --master spark://spark-master:7077 --executor-memory 16G --executor-cores 4 opt/spark-jobs/datasync/main.py -s source_db -t target_db -tab my_table -pk id
    ```

+ Run the data sync for a query, i.e., re-sync the data of my_table given id range.
    ```bash
    docker exec spark-master bin/spark-submit --master spark://spark-master:7077 --executor-memory 16G --executor-cores 4 opt/spark-jobs/datasync/main.py -s source_db -t target_db -tab my_table -pk id -qry "SELECT * FROM my_table WHERE id BETWEEN 123 AND 456"
    ```

+ Note: Before running the file `ETL/src/etl/main.py` to sync the ODS data, please make sure the table you want to sync has been created in ClickHouse.