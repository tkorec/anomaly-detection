import pandas as pd
from pyspark.sql import SparkSession


from PatternFeed.DataLoaderPF import DataLoader


class Engine:

    def __init__() -> None:
        self.spark = SparkSession.builder.appName("time-series-monitoring").getOrCreate()
        self.dataloader = DataLoader(self.spark)

    def run_monitoring():

        for metric in metrics:

            