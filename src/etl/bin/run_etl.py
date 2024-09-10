#!usr/bin/env python
# -*- coding: utf-8 -*-
import os


class RunETL:

    @staticmethod    
    def run_data_sync(cmd: str) -> None:
        os.system(cmd)
        return

    @staticmethod
    def run_data_lineage(cmd: str) -> None:
        os.system(cmd)
        return
