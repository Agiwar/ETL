#!usr/bin/env python
# -*- coding: utf-8 -*-
import configparser
import pathlib

from src.datasync.toolkit.common_libs import AttributeDict


__JDBC_URL = "jdbc:{db_type}://{HOST}:{PORT}/{DATABASE}?user={USER}&password={PASSWD}"
__MSSQL_JDBC_URL = "jdbc:{db_type}://{HOST}:{PORT};database={DATABASE};user={USER};password={PASSWD}"

__MSSQL_PROPERTY = "encrypt=true;trustServerCertificate=true;applicationIntent=ReadOnly"
__MYSQL_PROPERTY = "serverTimezone=GMT%2B8&useUnicode=true&amp&characterEncoding=utf-8&useSSL=false"


__config = configparser.ConfigParser(dict_type=AttributeDict)
__config.optionxform = str

__config_path = f"{pathlib.Path(__file__).parent}/config.ini"
__config.read(__config_path)


DATABASE_CONNECTOR = AttributeDict()
DATABASE_INFO = AttributeDict()
SLACK_INFO = AttributeDict()
CLICKHOUSE_INFO = AttributeDict()
MINIO_INFO = AttributeDict()

for item in __config.sections():
    item_info: AttributeDict = getattr(__config._sections, item)

    if item.endswith("CONNECTOR"):
        conn_type = item[:item.index("_CONNECTOR")]
        DATABASE_CONNECTOR.update({conn_type: item_info})

    elif item == "SLACK":
        SLACK_INFO.update(item_info)

    elif item == "CLICKHOUSE":
        CLICKHOUSE_INFO.update(item_info)

    elif item == "MINIO":
        MINIO_INFO.update(item_info)

    else:
        jdbc_url = ""

        if item.startswith("MSSQL"):
            db_type = __config._sections.MSSQL_CONNECTOR.TYPE
            jdbc_url = __MSSQL_JDBC_URL.format(db_type=db_type, **item_info)
            jdbc_url += f";{__MSSQL_PROPERTY}"

        elif item.startswith("MYSQL"):
            db_type = __config._sections.MYSQL_CONNECTOR.TYPE
            jdbc_url = __JDBC_URL.format(db_type=db_type, **item_info)
            jdbc_url += f"&{__MYSQL_PROPERTY}"

        else:
            db_type = __config._sections.CLICKHOUSE_CONNECTOR.TYPE
            jdbc_url = __JDBC_URL.format(db_type=db_type, **item_info)

        item_info.update({"JDBC": jdbc_url})
        DATABASE_INFO.update({item: item_info})
