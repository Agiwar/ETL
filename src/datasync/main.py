#!usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import time
from typing import Type

from clickhouse_driver import Client
from colorama import Fore, Style
from pyspark.sql import SparkSession

from config.data_sync_config import DATABASE_CONNECTOR, DATABASE_INFO, SLACK_INFO
from src.datasync.toolkit.common_libs import AttributeDict, SparkTool, SlackSession


APP_NAME = "data_sync"

SPARK_JARS_PACKAGES = AttributeDict({
    "mssql":      f"{DATABASE_CONNECTOR.MSSQL.GROUPID}:{DATABASE_CONNECTOR.MSSQL.ARTIFACTID}:{DATABASE_CONNECTOR.MSSQL.VERSION}",
    "mysql":      f"{DATABASE_CONNECTOR.MYSQL.GROUPID}:{DATABASE_CONNECTOR.MYSQL.ARTIFACTID}:{DATABASE_CONNECTOR.MYSQL.VERSION}",
    "mysql5":     f"{DATABASE_CONNECTOR.MYSQL5.GROUPID}:{DATABASE_CONNECTOR.MYSQL5.ARTIFACTID}:{DATABASE_CONNECTOR.MYSQL5.VERSION}",
    "clickhouse": f"{DATABASE_CONNECTOR.CLICKHOUSE.GROUPID}:{DATABASE_CONNECTOR.CLICKHOUSE.ARTIFACTID}:{DATABASE_CONNECTOR.CLICKHOUSE.VERSION}",
})

SLACK_PROPERTIES = {
    "username":    SLACK_INFO.USERNAME,
    "channel":     SLACK_INFO.CHANNEL,
    "webhook_url": SLACK_INFO.WEBHOOKURL,
}

CLICKHOUSE_INFO = {
    "host":     DATABASE_INFO.JKOPAY_ODS.HOST,
    "port":     "9000",  # This driver uses native protocol (port 9000).
    "user":     DATABASE_INFO.JKOPAY_ODS.USER,
    "password": DATABASE_INFO.JKOPAY_ODS.PASSWD,
}


spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .config("spark.jars.packages", f"{SPARK_JARS_PACKAGES.mssql},{SPARK_JARS_PACKAGES.mysql},{SPARK_JARS_PACKAGES.mysql5},{SPARK_JARS_PACKAGES.clickhouse}")
    .getOrCreate()
)

slack = SlackSession(**SLACK_PROPERTIES)
ch_client = Client(**CLICKHOUSE_INFO)


class DataSync:
    """DataSync lets you sync data between the different databases.

    Usage:
    """

    def __init__(
        self,
        source_db:    str,
        target_db:    str,
        table_name:   str,
        primary_key:  str,
        custom_query: str,
    ) -> None:
        self.source_db = source_db
        self.target_db = target_db
        self.table_name = table_name
        self.primary_key = primary_key
        self.custom_query = custom_query

        self.conn_info = self.__get_connection_info()

        self.spark = spark

    def __get_connection_info(self) -> AttributeDict:
        source_type = self.source_db.split("_")[0]

        source_conn_info = {
            "jdbc_driver": getattr(DATABASE_CONNECTOR.get(source_type), "DRIVER"),
            "jdbc_url":    getattr(DATABASE_INFO.get(self.source_db), "JDBC"),
        }

        target_conn_info = {
            "jdbc_driver": DATABASE_CONNECTOR.CLICKHOUSE.DRIVER,
            "jdbc_url":    getattr(DATABASE_INFO.get(self.target_db), "JDBC"),
        }

        conn_info = AttributeDict({
            "source": AttributeDict(source_conn_info),
            "target": AttributeDict(target_conn_info),
        })

        return conn_info

    def action(self) -> Type[None]:
        spark_tool = SparkTool(self.spark)

        if self.custom_query is not None:

            spark_tool.all_data_sync(
                source_jdbc_driver=self.conn_info.source.jdbc_driver,
                source_jdbc_url=self.conn_info.source.jdbc_url,
                target_jdbc_driver=self.conn_info.target.jdbc_driver,
                target_jdbc_url=self.conn_info.target.jdbc_url,
                table_name=self.table_name,
                query_condition=self.custom_query,
            )

            success_msg = f"Insert the specific data from `{self.source_db}.{self.table_name}` into `{self.target_db}.{self.table_name}` via `{self.custom_query}` done."
            print(f"{Fore.GREEN}{success_msg}{Style.RESET_ALL}")
            slack.send_message(success_msg)

            return

        big_data, _ = spark_tool.is_data_size_larger_than_100000(
            jdbc_driver=self.conn_info.source.jdbc_driver,
            jdbc_url=self.conn_info.source.jdbc_url,
            table_name=self.table_name,
            primary_key=self.primary_key,
        )

        if big_data:
            spark_tool.small_batch_data_sync(
                source_jdbc_driver=self.conn_info.source.jdbc_driver,
                source_jdbc_url=self.conn_info.source.jdbc_url,
                target_jdbc_driver=self.conn_info.target.jdbc_driver,
                target_jdbc_url=self.conn_info.target.jdbc_url,
                table_name=self.table_name,
                primark_key=self.primary_key,
            )

        else:
            spark_tool.all_data_sync(
                source_jdbc_driver=self.conn_info.source.jdbc_driver,
                source_jdbc_url=self.conn_info.source.jdbc_url,
                target_jdbc_driver=self.conn_info.target.jdbc_driver,
                target_jdbc_url=self.conn_info.target.jdbc_url,
                table_name=self.table_name,
            )

        success_msg = f"Sync table `{self.table_name}` from `{self.source_db}` to `{self.target_db}` done."
        print(f"{Fore.GREEN}{success_msg}{Style.RESET_ALL}")
        slack.send_message(success_msg)

        return


def get_data_sync_details() -> list:
    sources: list[str] = [db for db in DATABASE_INFO if "ODS" not in db]
    targets: list[str] = [db for db in DATABASE_INFO if "ODS" in db]

    data_sync_details = list()
    for source in sources:
        db = source.split("_")[1]

        if db.startswith("MPOS"):
            db = "MPOS"

        if db.startswith("JKOPAY"):
            db = "JKOPAY"

        target = [ods for ods in targets if db in ods][0]

        source_info: AttributeDict = getattr(DATABASE_INFO, source)
        target_info: AttributeDict = getattr(DATABASE_INFO, target)

        table_str: str = source_info.TABLE
        primary_key: str = source_info.PK
        clickhouse_ods: str = target_info.DATABASE

        data_sync_details.append(AttributeDict({
            "source": source,
            "target": target,
            "tables": table_str.split(","),
            "pk":     primary_key,
            "ods":    clickhouse_ods,
        }))

    return data_sync_details


def get_parser() -> argparse.ArgumentParser:
    usage = "python main.py -s [DB1] -t [DB2] -tab [table_name] -pk [primary_key] -qry [custom_query]"
    description = "Sync data from one database to another."

    parser = argparse.ArgumentParser(usage=usage, description=description)

    parser.add_argument(
        "-s",
        "--source",
        choices=[db for db in DATABASE_INFO if "MSSQL" in db or "MYSQL" in db],
        type=str,
        help="Specify which database you want to access.",
    )

    parser.add_argument(
        "-t",
        "--target",
        choices=[
            db for db in DATABASE_INFO if "MSSQL" not in db and "MYSQL" not in db],
        type=str,
        help="Specify where ODS layer in ClickHouse you want to load data.",
    )

    parser.add_argument(
        "-tab",
        "--table",
        type=str,
        help="Specify the table name you want to sync.",
    )

    parser.add_argument(
        "-pk",
        "--primary_key",
        type=str,
        help="Specify the primary key of table which is big.",
    )

    parser.add_argument(
        "-qry",
        "--custom_query",
        type=str,
        help="Provide the custom query to insert the specific data.",
    )

    return parser


def optimize_clickhouse_table(ods: str, table: str):
    sql = f"OPTIMIZE TABLE {ods}.{table} FINAL"
    ch_client.execute(sql)


def main(data_sync_properties: AttributeDict):
    data_sync = DataSync(**data_sync_properties)
    data_sync.action()


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    if args.source is None:
        data_sync_details = get_data_sync_details()
        for row in data_sync_details:
            source = row.source
            target = row.target
            tables = row.tables
            pk = row.pk
            ods = row.ods

            # MYSQL_DYNAMIC is incrementally sync not fully sync.
            if source in ("MYSQL_DYNAMIC", "MSSQL_JKOPAY3", "MYSQL_ONLINEPAY", "MYSQL_PAYPAY"):
                continue

            # Skip data sync procedure if there isn't any table needed to be synced.
            if len(tables) == 1 and tables[0] == "":
                continue

            for table in tables:
                data_sync_properties = AttributeDict({
                    "source_db":    source,
                    "target_db":    target,
                    "table_name":   table,
                    "primary_key":  pk,
                    "custom_query": None,
                })

                if f"{ods}.{table}" in ["JKOPay_ods.HashtagsStoreMap", "JKOPay_ods.BrandStoreMap"]:
                    ch_client.execute(f"TRUNCATE TABLE {ods}.{table}")
                    time.sleep(130)  # avoid deadlock table error
                    main(data_sync_properties)
                    continue

                main(data_sync_properties)
                optimize_clickhouse_table(ods=ods, table=table)

    else:
        data_sync_properties = AttributeDict({
            "source_db":    args.source,
            "target_db":    args.target,
            "table_name":   args.table,
            "primary_key":  args.primary_key,
            "custom_query": args.custom_query,
        })

        main(data_sync_properties)

        ods = getattr(DATABASE_INFO.get(args.target), "DATABASE")

        # Don't need to apply OPTIMIZE SQL on the incrementally sync table.
        if ods not in ("DynamicDB_ods", "JKOPay_ods"):
            optimize_clickhouse_table(ods=ods, table=args.table)

