#!usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from config.data_sync_config import SLACK_INFO
from src.datasync.toolkit.common_libs import SlackSession
from src.etl.bin.run_etl import RunETL


slack_info = {
    "username":    SLACK_INFO.USERNAME,
    "channel":     SLACK_INFO.CHANNEL,
    "webhook_url": SLACK_INFO.WEBHOOKURL,
}
slack = SlackSession(**slack_info)


log_filename = "opt/spark-logs/etl.log"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S",
)


command = {
    "run_data_sync": (
        "bin/spark-submit "
        "--master spark://spark-master:7077 "
        "--executor-memory 16G "
        "--executor-cores 4 "
        f"opt/spark-jobs/datasync/main.py >> {log_filename} 2>&1"
    ),
    
    "run_data_lineage": f"python3 opt/spark-jobs/datalineage/main.py >> {log_filename} 2>&1",
}


def main():
    logging.info("ETL job started.")
    run_etl = RunETL()
    
    logging.info("Running data synchronization step.")
    run_etl.run_data_sync(cmd=command.get("run_data_sync"))
    
    logging.info("Running data lineage step.")
    run_etl.run_data_lineage(cmd=command.get("run_data_lineage"))

    logging.info("ETL job completed.")


if __name__ == "__main__":
    slack.send_message("ETL starts.")
    main()
    slack.send_message("ETL done.")
