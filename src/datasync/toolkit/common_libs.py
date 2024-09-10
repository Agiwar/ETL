#!usr/bin/env python
# -*- coding: utf-8 -*-
import json
import math
from typing import Optional, Tuple, Type
from urllib import parse

import requests
from colorama import Fore, Style
from pyspark.sql import DataFrame, SparkSession


class AttributeDict(dict):
    def __init__(self, *args, **kwargs) -> None:
        super(AttributeDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


class SparkTool:
    """SparkTool is used to initialize spark and implement custom methods with dataframe.

    Usage:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName().config().getOrCreate()
    spark_tool = SparkTool(spark)
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    @staticmethod
    def is_spark_dataframe(obj: object) -> bool:
        if not isinstance(obj, DataFrame):
            return False

        return True

    @staticmethod
    def is_available_jdbc_driver(driver: str) -> bool:
        from config.data_sync_config import DATABASE_CONNECTOR

        available_drivers = [db.DRIVER for db in DATABASE_CONNECTOR.values()]
        if driver not in available_drivers:
            return False

        return True

    @staticmethod
    def is_available_data_source(data_source: str) -> bool:
        from config.data_sync_config import DATABASE_INFO

        available_data_source = [db for db in DATABASE_INFO]
        if data_source not in available_data_source:
            return False

        return True

    @staticmethod
    def legalize_jdbc_url(jdbc_url: str) -> str:
        special_chars = ["@"]
        if any(char in jdbc_url for char in special_chars):
            parsed = parse.urlparse(jdbc_url)
            params = dict(parse.parse_qsl(parsed.query))

            for field, value in params.items():
                if any(char in value for char in special_chars):
                    params.update({field: parse.quote(value)})

            new_param = ["{}={}".format(k, v) for k, v in params.items()]
            new_query = "&".join(new_param)

            parsed = parsed._replace(query=new_query)
            jdbc_url = parse.urlunparse(parsed)

        return jdbc_url

    def extract_jdbc_dataframe(
        self,
        jdbc_driver: str,
        jdbc_url:    str,
        query:       str,
    ) -> DataFrame:

        if not self.is_available_jdbc_driver(jdbc_driver):
            error_msg = "No suitable driver."
            raise RuntimeError(f"{Fore.RED}{error_msg}{Style.RESET_ALL}")

        legalize_jdbc_url = self.legalize_jdbc_url(jdbc_url)

        properties = {
            "driver": jdbc_driver,
            "url":    legalize_jdbc_url,
            "query":  query,
        }
        df = self.spark.read.format("jdbc").options(**properties).load()

        return df

    def load_jdbc_dataframe(
        self,
        dataframe:   DataFrame,
        jdbc_driver: str,
        jdbc_url:    str,
        table_name:  str,
        saving_mode: str = "append",
    ) -> Type[None]:

        if not self.is_spark_dataframe(dataframe):
            error_msg = f"{dataframe} object is not a Spark DataFrame."
            raise TypeError(f"{Fore.RED}{error_msg}{Style.RESET_ALL}")

        if not self.is_available_jdbc_driver(jdbc_driver):
            error_msg = "No suitable driver."
            raise RuntimeError(f"{Fore.RED}{error_msg}{Style.RESET_ALL}")

        if saving_mode != "append":
            error_msg = f"Saving mode must be 'append', since table {table_name} has been created before."
            raise RuntimeError(f"{Fore.RED}{error_msg}{Style.RESET_ALL}")

        legalize_jdbc_url = self.legalize_jdbc_url(jdbc_url)

        (
            dataframe.write
            .mode(saving_mode)
            .option("driver", jdbc_driver)
            .jdbc(url=legalize_jdbc_url, table=table_name)
        )

        return

    def is_data_size_larger_than_100000(
        self,
        jdbc_driver: str,
        jdbc_url:    str,
        table_name:  str,
        primary_key: str,
    ) -> Tuple[bool, Optional[int]]:

        query = f"SELECT count(*) AS counts, max({primary_key}) AS max_id FROM {table_name}"
        df = self.extract_jdbc_dataframe(jdbc_driver, jdbc_url, query)

        values = [row for row in df.select("counts", "max_id").collect()][0]
        data_records: int = values.counts
        max_id: int = values.max_id

        if data_records < 100000:
            return False, None

        return True, max_id

    def all_data_sync(
        self,
        source_jdbc_driver: str,
        source_jdbc_url:    str,
        target_jdbc_driver: str,
        target_jdbc_url:    str,
        table_name:         str,
        query_condition:    str = None,
    ) -> Type[None]:

        query = query_condition if query_condition is not None else f"SELECT * FROM {table_name}"

        df = self.extract_jdbc_dataframe(
            jdbc_driver=source_jdbc_driver,
            jdbc_url=source_jdbc_url,
            query=query,
        )

        self.load_jdbc_dataframe(
            dataframe=df,
            jdbc_driver=target_jdbc_driver,
            jdbc_url=target_jdbc_url,
            table_name=table_name,
        )

        return

    def small_batch_data_sync(
        self,
        source_jdbc_driver: str,
        source_jdbc_url:    str,
        target_jdbc_driver: str,
        target_jdbc_url:    str,
        table_name:         str,
        primark_key:        str,
    ) -> Type[None]:

        big_table, max_id = self.is_data_size_larger_than_100000(
            jdbc_driver=source_jdbc_driver,
            jdbc_url=source_jdbc_url,
            table_name=table_name,
            primary_key=primark_key,
        )

        if not big_table:
            error_msg = f"Size of table {table_name} isn't larger than 100,000."
            raise RuntimeError(f"{Fore.RED}{error_msg}{Style.RESET_ALL}")

        multiple = 100000
        upper_bound = math.ceil(max_id / multiple)

        for i in range(0, upper_bound):

            condition_expr = f"{primark_key} > {i * multiple} AND {primark_key} <= {(i + 1) * multiple}"
            query = f"SELECT * FROM {table_name} WHERE {condition_expr}"

            self.all_data_sync(
                source_jdbc_driver=source_jdbc_driver,
                source_jdbc_url=source_jdbc_url,
                target_jdbc_driver=target_jdbc_driver,
                target_jdbc_url=target_jdbc_url,
                table_name=table_name,
                query_condition=query,
            )

        return


class SlackSession:
    """Initialize SlackSession by user's slack info and send message to your slack channel.

    Usage:
    slack = SlackSession(
        username=< your username >,
        channel=< your channel >,
        webhook_url=< your slack webhook_url >,
    )
    slack.send_message(< your message >)
    """

    def __init__(
        self,
        username:    str,
        channel:     str,
        webhook_url: str,
    ) -> None:
        if channel is None:
            error_msg = "Please assign a channel."
            raise RuntimeError(f"{Fore.RED}{error_msg}{Style.RESET_ALL}")

        if webhook_url is None:
            error_msg = "Please assign a valid token."
            raise RuntimeError(f"{Fore.RED}{error_msg}{Style.RESET_ALL}")

        self.username = username
        self.channel = channel
        self.webhook_url = webhook_url

    def send_message(self, message: str) -> requests.Response:
        payload = dict()

        payload.update({
            "username": self.username,
            "channel":  self.channel,
            "text":     message,
        })

        try:
            response = requests.post(self.webhook_url, json.dumps(payload))
            return response

        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
