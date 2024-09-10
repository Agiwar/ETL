import argparse
import configparser
import logging

import pandas as pd
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError

from config.data_sync_config import CLICKHOUSE_INFO


def read_offline_csv(filename, columns_str=None, columns_datetime=None):
    dict_dtypes, list_parse_dates = None, None

    if columns_str is not None:
        list_columns_need_to_be_str = columns_str.split(",")
        dict_dtypes = {x: str for x in list_columns_need_to_be_str}

    if columns_datetime is not None:
        list_parse_dates = columns_datetime.split(",")

    df = pd.read_csv(filepath_or_buffer=filename, dtype=dict_dtypes, parse_dates=list_parse_dates)

    return df


def clickhouse_connection(config):
    clickhouse_client = Client(**config)

    try:
        clickhouse_client.execute("SELECT 1")
        logging.info("ClickHouse connection successful.")
        return clickhouse_client

    except ClickHouseError as err:
        logging.error(f"Failed to connect: {err}.")
        return None


def upload_file_to_clickhouse(data, db_name, table_name):
    if data is None or data.empty:
        logging.error("Dataframe is empty or None. Skipping upload.")
        return

    clickhouse_client = clickhouse_connection(CLICKHOUSE_CONFIG)
    if clickhouse_client is None:
        logging.error("No ClickHouse client created.")
        return

    sql = f"INSERT INTO {db_name}.{table_name} VALUES"
    try:
        clickhouse_client.execute(query=sql, params=data.to_dict("records"))
        logging.info(f"Execute the query '{sql}' successfully.")
        return

    except ClickHouseError as err:
        logging.error(f"Failed to execute the query {sql}: {err}.")
        return


def get_parser():
    usage = (
        "python3 upload_data_to_clickhouse.py "
        "-f [FileName] "
        "-d [DBName] "
        "-t [TableName] "
        "-cStr [ColumnsNeedToBeStr] "
        "-cDate [ColumnsNeedToBeDate]"
    )
    description = "Upload offline data into ClickHouse."

    parser = argparse.ArgumentParser(usage=usage, description=description)

    parser.add_argument(
        "-f",
        "--file_name",
        type=str,
        required=True,
    )

    parser.add_argument(
        "-d",
        "--db_name",
        type=str,
        required=True,
    )

    parser.add_argument(
        "-t",
        "--table_name",
        type=str,
        required=True,
    )

    parser.add_argument(
        "-cStr",
        "--columns_str",
        type=str,
        required=False,
    )

    parser.add_argument(
        "-cDate",
        "--columns_datetime",
        type=str,
        required=False,
    )

    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()

    df = read_offline_csv(filename=args.file_name, columns_str=args.columns_str, columns_datetime=args.columns_datetime)
    upload_file_to_clickhouse(data=df, db_name=args.db_name, table_name=args.table_name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    CLICKHOUSE_CONFIG = {
        "host":     CLICKHOUSE_INFO.HOST,
        "port":     int(CLICKHOUSE_INFO.PORT),
        "user":     CLICKHOUSE_INFO.USER,
        "password": CLICKHOUSE_INFO.PASSWD,
    }

    main()
