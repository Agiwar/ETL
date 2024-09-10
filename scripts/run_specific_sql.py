from clickhouse_driver import Client

from config.data_sync_config import CLICKHOUSE_INFO


CLICKHOUSE_PROPERTIES = {
    "host":     CLICKHOUSE_INFO.HOST,
    "port":     int(CLICKHOUSE_INFO.PORT),
    "user":     CLICKHOUSE_INFO.USER,
    "password": CLICKHOUSE_INFO.PASSWD,
}


sql = "SELECT 1 AS one;"


def main():
    ch_client = Client(**CLICKHOUSE_PROPERTIES)
    ch_client.execute(sql)

    return


if __name__ == "__main__":
    main()
