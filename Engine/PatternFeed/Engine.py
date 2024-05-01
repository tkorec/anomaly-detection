from pyspark.sql import SparkSession
import MetricsPF
import DataLoaderPF


class Engine:

    def __init__(self) -> None:
        self.spark = SparkSession.builder.appName("PatternFeedMonitoringEngine").getOrCreate()

        self.data_loader = DataLoaderPF(self.spark)
        self.aggregated_data = self.data_loader.load_aggregated_data()
        self.cube_data = self.data_loader.load_cube_data()
        self.results_data = self.data_loader.load_results()

        self.metrics = MetricsPF(self.spark, self.aggregated_data, self.cube_data, self.results_data)
        self.metrics_list = dir(self.metrics)
        self.all_methods_except_init = [attr for attr in self.metrics_methods if callable(getattr(MetricsPF, attr)) and attr != "__init__"]

        self.results = {}


    def run_monitoring(self):

        for metric in self.all_methods_except_init:
            method_to_call = getattr(self.metrics, metric)
            result = method_to_call()
            self.results.append(result)

        # Save Results to S3