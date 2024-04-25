import DataLoading
import Functions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class MetricsFunctions:

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.data_loading = DataLoading(self.spark)
        self.data = self.data_loading.load_counting_metrics_results()
    
    def count_rows(self) -> str:
        count_rows = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_rows.filter(count_rows["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def count_rows_per_dataset(self) -> str:
        count_rows_per_dataset = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_rows_per_dataset.filter(count_rows_per_dataset["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def count_unique_panelists(self) -> str:
        count_unique_panelists = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_unique_panelists.filter(count_unique_panelists["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def count_unique_panelists_per_dataset(self) -> str:
        count_unique_panelists_per_dataset = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_unique_panelists_per_dataset.filter(count_unique_panelists_per_dataset["verdict"] != "OK").groupBy(F.desc("verdict"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def count_valid_clicks_ratio_per_dataset(self) -> str:
        count_valid_clicks_ratio_per_dataset = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_valid_clicks_ratio_per_dataset.filter(count_valid_clicks_ratio_per_dataset["verdict"] != "OK").groupBy(F.desc("verdict"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result
    
