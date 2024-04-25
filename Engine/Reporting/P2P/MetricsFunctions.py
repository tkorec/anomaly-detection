import DataLoading
import Functions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class MetricsFunctions:

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.data_loading = DataLoading(self.spark)
        self.data = self.data_loading.load_counting_metrics_results()

    def count_rows():
        count_rows = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_rows.filter(count_rows["verdict"] != "OK").orderBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def count_rows_per_domain(self):
        count_rows_per_domain = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_rows_per_domain.filter(count_rows_per_domain["verdict"] != "OK").orderBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def count_rows_per_dataset(self):
        count_rows_per_dataset = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_rows_per_dataset.filter(count_rows_per_dataset["verdict"] != "OK").orderBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data) 
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def count_rows_per_metadatalink_per_domain(self):
        count_rows_per_metadatalink_per_domain = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_rows_per_metadatalink_per_domain.filter(count_rows_per_metadatalink_per_domain["verdict"] != "OK").orderBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def count_rows_per_pidsource_per_domain(self):
        count_rows_per_pidsource_per_domain = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_rows_per_pidsource_per_domain.filter(count_rows_per_pidsource_per_domain["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def count_unique_panelists(self):
        count_unique_panelists = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_unique_panelists.filter(count_unique_panelists["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def count_unique_panelists_per_domain(self):
        count_unique_panelists_per_domain = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_unique_panelists_per_domain.filter(count_unique_panelists_per_domain["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def count_unique_panelists_per_dataset(self):
        count_unique_panelists_per_dataset = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_unique_panelists_per_dataset.filter(count_unique_panelists_per_dataset["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def count_behaviors_per_domain(self):
        count_behaviors_per_domain = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_behaviors_per_domain.filter(count_behaviors_per_domain["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.behavior_domain_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def count_behaviors_with_extracted_pids_per_domain(self):
        count_behaviors_with_extracted_pids_per_domain = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_behaviors_with_extracted_pids_per_domain.filter(count_behaviors_with_extracted_pids_per_domain["verdict"] != "OK") \
            .groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.behavior_domain_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_results = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_results

    def behavior_ratio_per_panelist_per_domain(self):
        behavior_ratio_per_panelist_per_domain = self.data.filter(self.data["name"] == func.__name__)
        nok_data = behavior_ratio_per_panelist_per_domain.filter(behavior_ratio_per_panelist_per_domain["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.behavior_domain_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_results = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_results

    def count_unique_pids_per_domain(self):
        count_unique_pids_per_domain = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_unique_pids_per_domain.filter(count_unique_pids_per_domain["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def count_deterministic_pids_per_domain(self):
        count_deterministic_pids_per_domain = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_deterministic_pids_per_domain.filter(count_deterministic_pids_per_domain["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def count_nondeterministic_pids_per_domain(self):
        count_nondeterministic_pids_per_domain = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_nondeterministic_pids_per_domain.filter(count_nondeterministic_pids_per_domain["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result