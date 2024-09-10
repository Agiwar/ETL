#!usr/bin/env python
# -*- coding: utf-8 -*-
from config.data_sync_config import DATABASE_CONNECTOR, DATABASE_INFO


def test_db_conn_config():
    assert len(DATABASE_CONNECTOR) == 3
    assert DATABASE_CONNECTOR.MSSQL.TYPE == "sqlserver"
    assert DATABASE_CONNECTOR.MSSQL.VERSION == "12.2.0.jre11"
    assert DATABASE_CONNECTOR.MYSQL.VERSION == "8.0.20"
    assert DATABASE_CONNECTOR.CLICKHOUSE.VERSION == "0.3.2-patch11"
    assert DATABASE_CONNECTOR.CLICKHOUSE.GROUPID == "com.clickhouse"
    assert DATABASE_CONNECTOR.CLICKHOUSE.DRIVER == "com.clickhouse.jdbc.ClickHouseDriver"


def test_db_prod_info_config():
    assert len(DATABASE_INFO) == 19
    assert len(DATABASE_INFO.MYSQL_COUPON) == 8
    assert len(DATABASE_INFO.JKOPAY_ODS) == 6
    assert "JDBC" in DATABASE_INFO.MYSQL_COUPON
