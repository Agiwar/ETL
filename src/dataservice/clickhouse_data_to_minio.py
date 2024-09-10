import argparse
import logging
from datetime import datetime
from io import BytesIO
from typing import NoReturn, Optional

import pandas as pd
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError
from minio import Minio
from minio.error import S3Error

from config.data_sync_config import CLICKHOUSE_INFO, MINIO_INFO


def clickhouse_connection(config: dict) -> Optional[Client]:
    clickhouse_client = Client(**config)

    try:
        clickhouse_client.execute("SELECT 1")
        logging.info("ClickHouse connection successful.")
        return clickhouse_client

    except ClickHouseError as err:
        logging.error(f"Failed to connect: {err}.")
        return None


def minio_connection(config: dict) -> Optional[Minio]:
    minio_client = Minio(**config)

    try:
        minio_client.list_buckets()
        logging.info("MinIO connection successful.")
        return minio_client

    except S3Error as err:
        logging.error(f"MinIO not reachable: {err}.")
        return None


def read_clickhouse_as_dataframe(sql: str) -> Optional[pd.DataFrame]:
    clickhouse_client = clickhouse_connection(CLICKHOUSE_CONFIG)
    if clickhouse_client is None:
        logging.error("No ClickHouse client created.")
        return

    try:
        df = clickhouse_client.query_dataframe(query=sql)
        logging.info(f"Execute the query '{sql}' successfully.")
        return df

    except ClickHouseError as err:
        logging.error(f"Failed to execute the query {sql}: {err}.")
        return


def upload_file_to_minio(data: pd.DataFrame, service_name: str) -> NoReturn:
    if data is None or data.empty:
        logging.error("Dataframe is empty or None. Skipping upload.")
        return

    minio_client = minio_connection(MINIO_CONFIG)
    if minio_client is None:
        logging.error("No MinIO client created.")
        return

    csv_bytes = data.to_csv(index=False).encode("utf-8")
    csv_buffer = BytesIO(csv_bytes)

    datetime_str = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    try:
        minio_client.put_object(
            bucket_name=f"data-service",
            object_name=f"{service_name}/{service_name}-{datetime_str}.csv",
            data=csv_buffer,
            length=len(csv_bytes),
            content_type="application/csv",
        )
        logging.info("Upload file to MinIO successfully.")
        return

    except S3Error as err:
        logging.error(f"Failed to upload the CSV file to MinIO: {err}.")
        return


def get_parser() -> argparse.ArgumentParser:
    usage = "python3 clickhouse_data_to_minio.py -n [ServiceName] -q [QueryData]"
    description = "Put ClickHouse data into MinIO."

    parser = argparse.ArgumentParser(usage=usage, description=description)

    parser.add_argument(
        "-n",
        "--service_name",
        type=str,
        required=True,
    )

    parser.add_argument(
        "-q",
        "--query",
        type=str,
        required=True,
    )

    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()

    df = read_clickhouse_as_dataframe(sql=args.query)
    upload_file_to_minio(data=df, service_name=args.service_name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    CLICKHOUSE_CONFIG = {
        "host":     CLICKHOUSE_INFO.HOST,
        "port":     int(CLICKHOUSE_INFO.PORT),
        "user":     CLICKHOUSE_INFO.USER,
        "password": CLICKHOUSE_INFO.PASSWD,
    }

    MINIO_CONFIG = {
        "endpoint":   MINIO_INFO.ENDPOINT,
        "access_key": MINIO_INFO.ACCESS_KEY,
        "secret_key": MINIO_INFO.SECRET_KEY,
    }

    main()

