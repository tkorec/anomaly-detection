from pandas import DataFrame
import Model
import func

class Metrics:

    def __init__(self, spark: SparkSession, aggregated_data: DataFrame, cube_data: DataFrame, result_data: DataFrame) -> None:
        self.spark = spark
        self.aggregated_data = aggregated_data
        self.cube_data = cube_data
        self.result_data = result_data
        self.model_arima = Model()
        self.model = self.model_arima.model()
        self.results = []


    """
    Every function in this file is dedicated to one metric. The reason of creating one method for each metric, despite the code repeats itself,
    is primarily because of organization, readability, easier adding, deteting, or updating metrics, because if I refactored code and created
    shared functions for some metrics, there would be also a lot of exceptions because of metrics that measure different counts than events, e.g.,
    bot_panelists, events_by_products_not_null_pid, etc.

    Each function works the same way:
    1/ The metric's data is selected form dataset where the metric's data is situated (cube or aggregated data; some metrics appears in both, 
    so I took them from cube dataset since it is more comprehensive) by grouping granularity parameters of a metric and date and summing 
    the analyzed counts – the sum is required as we have metrics for "All" datasets or "All" domains and we need the overall count for
    all that datasets, domains, etc.
    2/ The list of unique combinations of metrics is created, e.g., number_of_events per Costco, per Walmart, ...
    3/ We iterate through the list of unique metrics, select only data related to the particular metric, i.e., particular combination of
    domain, dataset, etc., sort this dataset by date so we have a time series.
    4/ Parameters for ARIMA model are selected from result_data dataframe for the particular metric, e.g., particular combination of domain,
    dataset, etc.
    5/ Time series and parameters are used for calling model function in Model class that returns current day value, expected value for current
    day found by model, verdict of decision making process (is current value within the threshold of expected value, is current value missing?),
    and parameters of model.
    6/ The outputs of model function are used for creating result report for current day that is then stored to S3. Report is then build upon this
    report and the parameters for ARIMA model for the particular metric from this report are used the following day for again
    """
    
    def number_of_events_per_domain(self) -> None:
        number_of_events_per_domain = self.cube_data.groupby("date", "domain", "behavior").agg(spark_sum("events_by_product").alias("events_by_product"))
        unique_combinations = number_of_events_per_domain.select("domain", "behavior").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = number_of_events_per_domain.filter(
                (number_of_events_per_domain["domain"] == domain) &
                (number_of_events_per_domain["behavior"] == behavior)).select("date", "events_by_product")
            data = data.orderBy("date")
            parameters = self.result_data.filter(
                (self.result_data["name"] == func.__name__) &
                (self.result_data["domain"] == domain) &
                (self.result_data["behavior"] == behavior)
            ).select("parameters").distinct()
            current_day_value, expected, verdict, parameters = self.model(data, parameters)
            result_input = self.create_result_input(func.__name__, domain, dataset, behavior, current_day_value, expected, verdict, parameters)
            self.results.append(result_input)


    def number_of_events_per_domain_per_dataset(self) -> None:
        number_of_events_per_domain_per_dataset = self.cube_data.groupby("date", "domain", "dataset", "behavior") \
            .agg(spark_sum("events_by_product").alias("events_by_product"))
        unique_combinations = number_of_events_per_domain_per_dataset.select("domain", "dataset", "behavior").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = number_of_events_per_domain_per_dataset.filter(
                (number_of_events_per_domain_per_dataset["domain"] == domain) &
                (number_of_events_per_domain_per_dataset["dataset"] == dataset) &
                (number_of_events_per_domain_per_dataset["behavior"] == behavior)).select("date", "events_by_product")
            data = data.orderBy("date")
            parameters = self.result_data.filter(
                (self.result_data["name"] == func.__name__) &
                (self.result_data["domain"] == domain) &
                (self.result_data["dataset"] == dataset) &
                (self.result_data["behavior"] == behavior)
            ).select("parameters").distinct()
            current_day_value, expected, verdict, parameters = self.model(data, parameters)
            result_input = self.create_result_input(func.__name__, domain, dataset, behavior, current_day_value, expected, verdict, parameters)
            self.results.append(result_input)


    def number_of_events_per_domain_per_patternId(self) -> None:
        number_of_events_per_domain_per_patternId = self.aggregated_data.groupby("date", "domain", "patternId") \
            .agg(spark_sum("events_by_product").alias("events_by_product"))
        unique_combinations = number_of_events_per_domain_per_patternId.select("domain", "patternId").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            pattern_id = unique_combinations.collect()[i]["patternId"]
            data = number_of_events_per_domain_per_patternId.filter(
                (number_of_events_per_domain_per_patternId["domain"] == domain) &
                (number_of_events_per_domain_per_patternId["patternId"] == pattern_id)).select("date", "events_by_product")
            data = data.orderBy("date")
            parameters = self.result_data.filter(
                (self.result_data["name"] == func.__name__) &
                (self.result_data["domain"] == domain) &
                (self.result_data["patternId"] == pattern_id)
            ).select("parameters").distinct()
            current_day_value, expected, verdict, parameters = self.model(data, parameters)
            result_input = self.create_result_input(func.__name__, domain, dataset, behavior, current_day_value, expected, verdict, parameters)
            self.results.append(result_input)


    def bot_panelists_per_domain(self) -> None:
        bot_panelists_per_domain = self.cube_data.groupby("date", "domain").agg(spark_sum("bot_panelists").alias("bot_panelists"))
        unique_combinations = bot_panelists_per_domain.select("domain").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = bot_panelists_per_domain.filter(bot_panelists_per_domain["domain"] == domain).select("date", "bot_panelists")
            data = data.orderBy("date")
            parameters = self.result_data.filter(
                (self.result_data["name"] == func.__name__) &
                (self.result_data["domain"] == domain)
            ).select("parameters").distinct()
            current_day_value, expected, verdict, parameters = self.model(data, parameters)
            result_input = self.create_result_input(func.__name__, domain, dataset, behavior, current_day_value, expected, verdict, parameters)
            self.results.append(result_input)


    def bot_panelists_per_domain_per_dataset(self) -> None:
        bot_panelists_per_domain_per_dataset = self.cube_data.groupby("date", "domain", "dataset").agg(spark_sum("bot_panelists").alias("bot_panelists"))
        unique_combinations = bot_panelists_per_domain_per_dataset.select("domain", "dataset").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = bot_panelists_per_domain_per_dataset.filter(
                (bot_panelists_per_domain_per_dataset["domain"] == domain) &
                (bot_panelists_per_domain_per_dataset["dataset"] == dataset)).select("date", "bot_panelists")
            data = data.orderBy("date")
            parameters = self.result_data.filter(
                (self.result_data["name"] == func.__name__) &
                (self.result_data["domain"] == domain) &
                (self.result_data["dataset"] == dataset)
            ).select("parameters").distinct()
            current_day_value, expected, verdict, parameters = self.model(data, parameters)
            result_input = self.create_result_input(func.__name__, domain, dataset, behavior, current_day_value, expected, verdict, parameters)
            self.results.append(result_input)

    def bot_events_per_domain(self) -> None:
        bot_events_per_domain = self.cube_data.groupby("date", "domain").agg(spark_sum("bot_events_by_product").alias("bot_events_by_product"))
        unique_combinations = bot_events_per_domain.select("domain").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = bot_events_per_domain.filter(bot_events_per_domain["domain"] == domain).select("date", "bot_events_by_product")
            data = data.orderBy("date")
            parameters = self.result_data.filter(
                (self.result_data["name"] == func.__name__) &
                (self.result_data["domain"] == domain)
            ).select("parameters").distinct()
            current_day_value, expected, verdict, parameters = self.model(data, parameters)
            result_input = self.create_result_input(func.__name__, domain, dataset, behavior, current_day_value, expected, verdict, parameters)
            self.results.append(result_input)


    def bot_events_per_domain_per_dataset(self) -> None:
        bot_events_per_domain_per_dataset = self.cube_data.groupby("date", "domain", "dataset") \
            .agg(spark_sum("bot_events_by_product").alias("bot_events_by_product"))
        unique_combinations = bot_events_per_domain_per_dataset.select("domain", "dataset").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = bot_events_per_domain_per_dataset.filter(
                (bot_events_per_domain_per_dataset["domain"] == domain) &
                (bot_events_per_domain_per_dataset["dataset"] == dataset)).select("date", "bot_events_by_product")
            data = data.orderBy("date")
            parameters = self.result_data.filter(
                (self.result_data["name"] == func.__name__) &
                (self.result_data["domain"] == domain) &
                (self.result_data["dataset"] == dataset)
            ).select("parameters").distinct()
            current_day_value, expected, verdict, parameters = self.model(data, parameters)
            result_input = self.create_result_input(func.__name__, domain, dataset, behavior, current_day_value, expected, verdict, parameters)
            self.results.append(result_input)


    def events_by_product_notnull_pid_per_domain(self) -> None:
        events_by_product_notnull_pid_per_domain = self.cube_data.groupby("date", "domain") \
            .agg(spark_sum("events_by_product_notnull_PID").alias("events_by_product_notnull_PID"))
        unique_combinations = events_by_product_notnull_pid_per_domain.select("domain").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = events_by_product_notnull_pid_per_domain.filter(events_by_product_notnull_pid_per_domain["domain"] == domain) \
                .select("date", "events_by_product_notnull_PID")
            data = data.orderBy("date")
            parameters = self.result_data.filter(
                (self.result_data["name"] == func.__name__) &
                (self.result_data["domain"] == domain)
            ).select("parameters").distinct()
            current_day_value, expected, verdict, parameters = self.model(data, parameters)
            result_input = self.create_result_input(func.__name__, domain, dataset, behavior, current_day_value, expected, verdict, parameters)
            self.results.append(result_input)


    def events_by_product_notnull_deterministic_pid_per_domain(self) -> None:
        events_by_product_notnull_deterministic_pid_per_domain = self.cube_data.groupby("date", "domain") \
            .agg(spark_sum("events_by_product_notnull_deterministic_PID").alias("events_by_product_notnull_deterministic_PID"))
        unique_combinations = events_by_product_notnull_deterministic_pid_per_domain.select("domain").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = events_by_product_notnull_deterministic_pid_per_domain \
                .filter(events_by_product_notnull_deterministic_pid_per_domain["domain"] == domain) \
                .select("date", "events_by_product_notnull_deterministic_PID")
            data = data.orderBy("date")
            parameters = self.result_data.filter(
                (self.result_data["name"] == func.__name__) &
                (self.result_data["domain"] == domain)
            ).select("parameters").distinct()
            current_day_value, expected, verdict, parameters = self.model(data, parameters)
            result_input = self.create_result_input(func.__name__, domain, dataset, behavior, current_day_value, expected, verdict, parameters)
            self.results.append(result_input)


    def products_in_catalog_per_domain(self) -> None:
        products_in_catalog_per_domain = self.cube_data.groupby("date", "domain").agg(spark_sum("products_in_catalog").alias("products_in_catalog"))
        unique_combinations = products_in_catalog_per_domain.select("domain").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = products_in_catalog_per_domain.filter(products_in_catalog_per_domain["domain"] == domain).select("date", "products_in_catalog")
            data = data.orderBy("date")
            parameters = self.result_data.filter(
                (self.result_data["name"] == func.__name__) &
                (self.result_data["domain"] == domain)
            ).select("parameters").distinct()
            current_day_value, expected, verdict, parameters = self.model(data, parameters)
            result_input = self.create_result_input(func.__name__, domain, dataset, behavior, current_day_value, expected, verdict, parameters)
            self.results.append(result_input)


    def events_by_product_in_catalog_per_domain(self) -> None:
        events_by_product_in_catalog_per_domain = self.cube_data.groupby("date", "domain") \
            .agg(spark_sum("events_by_product_in_catalog").alias("events_by_product_in_catalog"))
        unique_combinations = events_by_product_in_catalog_per_domain.select("domain").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = events_by_product_in_catalog_per_domain.filter(events_by_product_in_catalog_per_domain["domain"] == domain) \
                .select("date", "events_by_product_in_catalog")
            data = data.orderBy("date")
            parameters = self.result_data.filter(
                (self.result_data["name"] == func.__name__) &
                (self.result_data["domain"] == domain)
            ).select("parameters").distinct()
            current_day_value, expected, verdict, parameters = self.model(data, parameters)
            result_input = self.create_result_input(func.__name__, domain, dataset, behavior, current_day_value, expected, verdict, parameters)
            self.results.append(result_input)


    def create_result_input(name, domain, dataset, behavior, current_day_value, expected, verdict, parameters):
        relative_difference = abs(current_day_value - expected) / (expected + 1e-6)
        z_score = (abs(current_day_value - expected)) / relative_difference.std()
        result_input = {
            "name": name,
            "value": current_day_value,
            "expected": expected,
            "relative difference": relative_difference,
            "verdict": verdict,
            "threshold": 2,
            "z_score": z_score,
            "behavior": behavior,
            "dataset": dataset,
            "domain": domain,
            "parameters": parameters
        }
        return result_input 