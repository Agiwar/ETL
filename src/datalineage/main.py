#!usr/bin/env python
# -*- coding: utf-8 -*-
from clickhouse_driver import Client

from config.data_sync_config import CLICKHOUSE_INFO, SLACK_INFO
from src.datasync.toolkit.common_libs import SlackSession


CLICKHOUSE_PROPERTIES = {
    "host":     CLICKHOUSE_INFO.HOST,
    "port":     int(CLICKHOUSE_INFO.PORT),
    "user":     CLICKHOUSE_INFO.USER,
    "password": CLICKHOUSE_INFO.PASSWD,
}

SLACK_PROPERTIES = {
    "username":    SLACK_INFO.USERNAME,
    "channel":     SLACK_INFO.CHANNEL,
    "webhook_url": SLACK_INFO.WEBHOOKURL,
}

ch_client = Client(**CLICKHOUSE_PROPERTIES)
slack = SlackSession(**SLACK_PROPERTIES)

DWD_LAYER_SQL = [
    "dwd_layer/dwd_tab1/insert.sql",
    "dwd_layer/dwd_tab2/insert.sql",
    "dwd_layer/dwd_tab3/insert.sql",
]

DWM_LAYER_SQL = [
    "dwm_layer/dwm_tab1/insert.sql",
    "dwm_layer/dwm_tab2/insert.sql",
    "dwm_layer/dwm_tab3/insert.sql",
    "dwm_layer/dwm_tab4/insert.sql",
]

DWS_LAYER_SQL = [
    "dws_layer/dws_tab1/insert.sql",
    "dws_layer/dws_tab2/insert.sql",
    "dws_layer/dws_tab3/insert.sql",
    "dws_layer/dws_tab4/insert.sql",
    "dws_layer/dws_tab5/insert.sql",
]

ADS_LAYER_SQL = [
    "ads_layer/ads_tab1/insert.sql",
    "ads_layer/ads_tab2/insert.sql",
]


def execute_sql_from_file(filename: str) -> None:
    with open(filename) as f:
        sql_commands = f.read().split(";")

        for command in sql_commands:
            ch_client.execute(command)

    return


def main():
    workdir = "opt/spark-jobs/datalineage/sql/"
    data_lineage = [DWD_LAYER_SQL, DWM_LAYER_SQL, DWS_LAYER_SQL, ADS_LAYER_SQL]

    for layer in data_lineage:
        for file in layer:
            execute_sql_from_file(filename=f"{workdir}{file}")

            table_name = file.split("/")[-2]

            success_msg = f"Execute SQL of `{table_name}` done."
            slack.send_message(success_msg)


if __name__ == "__main__":
    main()
