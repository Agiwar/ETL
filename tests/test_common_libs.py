#!usr/bin/env python
# -*- coding: utf-8 -*-
from config.data_sync_config import DATABASE_CONNECTOR, DATABASE_INFO, SLACK_INFO
from src.datasync.toolkit.common_libs import AttributeDict, SparkTool, SlackSession


def test_attribute_dict():
    attr_dict = AttributeDict({"i": 1, "j": 2, "k": 3})

    assert len(attr_dict) == 3
    assert attr_dict.i == 1
    assert "k" in attr_dict


def test_is_spark_dataframe(spark):
    data = [
        ("v11", "v21", "v31"),
        ("v12", "v22", "v32")
    ]
    column = ['col1', 'col2', 'col3']
    df = spark.createDataFrame(data, column)

    assert SparkTool.is_spark_dataframe(df) is True
    assert SparkTool.is_spark_dataframe(data) is False


def test_is_available_jdbc_driver():
    oracle_driver = "oracle.jdbc.driver.OracleDriver"
    mysql_driver = "com.mysql.cj.jdbc.Driver"

    assert SparkTool.is_available_jdbc_driver(oracle_driver) is False
    assert SparkTool.is_available_jdbc_driver(mysql_driver) is True


def test_extract_jdbc_dataframe(spark):
    spark_tool = SparkTool(spark)

    for data_source, db_info in DATABASE_INFO.items():
        driver = ""
        query = "SELECT 1 AS one"

        if "MSSQL" in data_source:
            driver = DATABASE_CONNECTOR.MSSQL.DRIVER
        elif "MYSQL" in data_source:
            driver = DATABASE_CONNECTOR.MYSQL.DRIVER
        else:
            driver = DATABASE_CONNECTOR.CLICKHOUSE.DRIVER

        properties = {
            "jdbc_driver": driver,
            "jdbc_url":    db_info.JDBC,
            "query":       query,
        }

        df = spark_tool.extract_jdbc_dataframe(**properties)

        assert spark_tool.is_spark_dataframe(df) is True
        assert df.count() == 1


def test_send_message():
    slack_info = {
        "username":    SLACK_INFO.USERNAME,
        "channel":     SLACK_INFO.CHANNEL,
        "webhook_url": SLACK_INFO.WEBHOOKURL,
    }

    slack = SlackSession(**slack_info)
    res = slack.send_message("Test for slack session successfully.")

    assert res.status_code == 200
