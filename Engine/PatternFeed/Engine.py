from pyspark.sql import SparkSession
import MetricsPF


class Engine:

    def __init__(self) -> None:
        self.spark = SparkSession.builder.appName("PatternFeedMonitoringEngine").getOrCreate()
        self.metrics = MetricsPF(self.spark)
        self.metrics_list = dir(self.metrics)
        self.all_methods_except_init = [attr for attr in self.metrics_methods if callable(getattr(MetricsPF, attr)) and attr != "__init__"]

        self.results = {}


    def run_monitoring(self):

        for metric in self.all_methods_except_init:
            method_to_call = getattr(self.metrics, metric)
            result = method_to_call()
            self.results.append(result)

        # Save Results to S3