#!usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
from pyspark.sql import SparkSession

from config.data_sync_config import DATABASE_CONNECTOR
from src.datasync.toolkit.common_libs import AttributeDict


@pytest.fixture(scope="session")
def spark():
    app_name = "test_common_libs"
    spark_jars_packages = {
        "mssql":      f"{DATABASE_CONNECTOR.MSSQL.GROUPID}:{DATABASE_CONNECTOR.MSSQL.ARTIFACTID}:{DATABASE_CONNECTOR.MSSQL.VERSION}",
        "mysql":      f"{DATABASE_CONNECTOR.MYSQL.GROUPID}:{DATABASE_CONNECTOR.MYSQL.ARTIFACTID}:{DATABASE_CONNECTOR.MYSQL.VERSION}",
        "clickhouse": f"{DATABASE_CONNECTOR.CLICKHOUSE.GROUPID}:{DATABASE_CONNECTOR.CLICKHOUSE.ARTIFACTID}:{DATABASE_CONNECTOR.CLICKHOUSE.VERSION}",
    }
    spark_jars_packages = AttributeDict(spark_jars_packages)

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", f"{spark_jars_packages.mssql},{spark_jars_packages.mysql},{spark_jars_packages.clickhouse}")
        .getOrCreate()
    )

    return spark
