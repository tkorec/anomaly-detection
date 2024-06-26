import DataLoading
import Functions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class MetricsFunctions:

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.data_loading = DataLoading(self.spark)
        self.data = self.data_loading.load_counting_metrics_results()

    # Counting Metrics
    def count_rows(self) -> str:
        count_rows = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_rows.filter(count_rows["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def count_rows_per_domain(self) -> str:
        count_rows_per_domain = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_rows_per_domain.filter(count_rows_per_domain["verdict"] != "OK").groupBy(F.desc("difference"))
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
        nok_data = count_unique_panelists.filter(count_unique_panelists["verdict"] != "OK")
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def count_unique_panelists_per_domain(self) -> str:
        count_unique_panelists_per_domain = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_unique_panelists_per_domain.filter(count_unique_panelists_per_domain["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def count_unique_panelists_per_dataset(self) -> str:
        count_unique_panelists_per_dataset = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_unique_panelists_per_dataset.filter(count_unique_panelists_per_dataset["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result
    
    def count_behaviors_per_domain(self) -> str:
        count_behaviors_per_domain = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_behaviors_per_domain.filter(count_behaviors_per_domain["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_behavior_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result


    def count_behaviors_with_extracted_pids_per_domain(self) -> str:
        count_behaviors_with_extracted_pids_per_domain = self.data.filter(self.data["name"] == func.__name__)
        nok_data = count_behaviors_with_extracted_pids_per_domain.filter(count_behaviors_with_extracted_pids_per_domain["verdict"] != "OK") \
            .groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_behavior_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result


    def behavior_ratio_per_panelist(self) -> str:
        behavior_ratio_per_panelist = self.data.filter(self.data["name"] == func.__name__)
        nok_data = behavior_ratio_per_panelist.filter(behavior_ratio_per_panelist["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_behavior_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result
    
    # Time Series Metrics
    def number_of_events_per_domain(self):
        number_of_events_per_domain = self.data.filter(self.data["name"] == func.__name__)
        nok_data = number_of_events_per_domain.filter(number_of_events_per_domain["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_behavior_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result
    
    def number_of_events_per_domain_per_dataset(self):
        number_of_events_per_domain_per_dataset = self.data.filter(self.data["name"] == func.__name__)
        nok_data = number_of_events_per_domain_per_dataset.filter(number_of_events_per_domain_per_dataset["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_behavior_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def number_of_events_per_domain_per_patternId(self):
        number_of_events_per_domain_per_patternId = self.data.filter(self.data["name"] == func.__name__)
        nok_data = number_of_events_per_domain_per_patternId.filter(number_of_events_per_domain_per_patternId["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_patternid_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def bot_panelists_per_domain(self):
        bot_panelists_per_domain = self.data.filter(self.data["name"] == func.__name__)
        nok_data = bot_panelists_per_domain.filter(bot_panelists_per_domain["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def bot_panelists_per_domain_per_dataset(self):
        bot_panelists_per_domain_per_dataset = self.data.filter(self.data["name"] == func.__name__)
        nok_data = bot_panelists_per_domain_per_dataset.filter(bot_panelists_per_domain_per_dataset["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def bot_events_per_domain(self):
        bot_events_per_domain = self.data.filter(self.data["name"] == func.__name__)
        nok_data = bot_events_per_domain.filter(bot_events_per_domain["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def bot_events_per_domain_per_dataset(self):
        bot_events_per_domain_per_dataset = self.data.filter(self.data["name"] == func.__name__)
        nok_data = bot_events_per_domain_per_dataset.filter(bot_events_per_domain_per_dataset["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def duplicated_search_term_events(self):
        duplicated_search_term_events = self.data.filter(self.data["name"] == func.__name__)
        nok_data = duplicated_search_term_events.filter(duplicated_search_term_events["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def events_with_pids_per_domain(self):
        events_with_pids_per_domain = self.data.filter(self.data["name"] == func.__name__)
        nok_data = events_with_pids_per_domain.filter(events_with_pids_per_domain["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result

    def events_with_pids_per_domain_per_dataset():
        events_with_pids_per_domain_per_dataset = self.data.filter(self.data["name"] == func.__name__)
        nok_data = events_with_pids_per_domain_per_dataset.filter(events_with_pids_per_domain_per_dataset["verdict"] != "OK").groupBy(F.desc("difference"))
        metric_wrapper = Functions.metric_wrapper_func(func.__name__)
        results, legend = Functions.domain_dataset_results(nok_data)
        insert_position = metric_wrapper.find("</div>")
        monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
        return monitoring_result
