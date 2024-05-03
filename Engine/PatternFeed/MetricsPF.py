from pandas import DataFrame
import Model
import func

class MetricsPF:


    def __init__(self, spark: SparkSession, aggregated_data: DataFrame, cube_data: DataFrame, result_data: DataFrame) -> None:
        self.spark = spark
        self.aggregated_data = aggregated_data
        self.cube_data = cube_data
        self.result_data = result_data
        self.model_arima = Model()
        self.model = self.model_arima.model(self, data, attributes)
        self.results = []



    def number_of_events_per_domain(self) -> None:
        number_of_events_per_domain = self.aggregated_data.groupby("date", "domain", "dataset", "behavior").agg(spark_sum("events").alias("events"))
        unique_combinations = number_of_events_per_domain.select("domain", "dataset", "behavior").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = number_of_events_per_domain.filter(
                (number_of_events_per_domain["domain"] == domain) & 
                (number_of_events_per_domain["behavior"] == behavior)).select("date", "events") 
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
            
        


    def number_of_events_per_domain_per_dataset(self) -> None:
        number_of_events_per_domain_per_dataset = self.aggregated_data.groupby("date", "domain", "dataset", "behavior") \
            .agg(spark_sum("events").alias("events"))
        unique_combinations = number_of_events_per_domain_per_dataset.select("domain", "dataset", "behavior").distinct()
    
        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = number_of_events_per_domain_per_dataset.filter(
                (number_of_events_per_domain_per_dataset["domain"] == domain) &
                (number_of_events_per_domain_per_dataset["dataset"] == dataset) &
                (number_of_events_per_domain_per_dataset["behavior"] == behavior)).select("date", "events")
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
        number_of_events_per_domain_per_patternId = self.aggregated_data.groupby("date", "domain", "patternId").agg(spark_sum("events").alias("events"))
        unique_combinations = number_of_events_per_domain_per_patternId.select("domain", "dataset", "behavior", "patternId").distinct()
    
        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            patternId = unique_combinations.collect()[i]["patternId"]
            data = number_of_events_per_domain_per_patternId.filter(
                (number_of_events_per_domain_per_patternId["domain"] == domain) &
                (number_of_events_per_domain_per_patternId["patternId"] == patternId)).select("date", "events")
            data = data.orderBy("date")
            parameters = self.result_data.filter(
                (self.result_data["name"] == func.__name__) &
                (self.result_data["domain"] == domain) &
                (self.result_data["patternId"] == patternId)
            ).select("parameters").distinct()
            current_day_value, expected, verdict, parameters = self.model(data, parameters)
            result_input = self.create_result_input(func.__name__, domain, dataset, behavior, current_day_value, expected, verdict, parameters)
            self.results.append(result_input)
        


    def bot_panelists_per_domain(self) -> None:
        bot_panelists_per_domain = self.cube_data.groupby("date", "domain").agg(spark_sum("bot_panelists").alias("bot_panelists"))
        unique_combinations = bot_panelists_per_domain.select("domain", "dataset", "behavior").distinct()
    
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
        unique_combinations = bot_panelists_per_domain_per_dataset.select("domain", "dataset", "behavior").distinct()
    
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
        bot_events_per_domain = self.cube_data.groupby("date", "domain").agg(spark_sum("bot_events").alias("bot_events"))
        unique_combinations = bot_events_per_domain.select("domain").distinct()
    
        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = bot_events_per_domain.filter(bot_events_per_domain["domain"] == domain).select("date", "bot_events")
            data = data.orderBy("date")
            parameters = self.result_data.filter(
                (self.result_data["name"] == func.__name__) &
                (self.result_data["domain"] == domain) 
            ).select("parameters").distinct()
            current_day_value, expected, verdict, parameters = self.model(data, parameters)
            result_input = self.create_result_input(func.__name__, domain, dataset, behavior, current_day_value, expected, verdict, parameters)
            self.results.append(result_input)
        


    def bot_events_per_domain_per_dataset(self) -> DataFrame:
        bot_events_per_domain_per_dataset = self.cube_data.groupby("date", "domain", "dataset").agg(spark_sum("bot_events").alias("bot_events"))
        unique_combinations = bot_events_per_domain_per_dataset.select("domain", "dataset").distinct()
    
        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = bot_events_per_domain_per_dataset.filter(
                (bot_events_per_domain_per_dataset["domain"] == domain) &
                (bot_events_per_domain_per_dataset["dataset"] == dataset)).select("date", "bot_events")
            data = data.orderBy("date")
            parameters = self.result_data.filter(
                (self.result_data["name"] == func.__name__) &
                (self.result_data["domain"] == domain) &
                (self.result_data["dataset"] == dataset)
            ).select("parameters").distinct()
            current_day_value, expected, verdict, parameters = self.model(data, parameters)
            result_input = self.create_result_input(func.__name__, domain, dataset, behavior, current_day_value, expected, verdict, parameters)
            self.results.append(result_input)



    def duplicated_search_term_events(self) -> DataFrame:
        duplicated_search_term_events = self.cube_data.groupby("date", "domain") \
            .agg(spark_sum("duplicated_searchterm_events").alias("duplicated_searchterm_events"))
        unique_combinations = duplicated_search_term_events.select("domain").distinct()
    
        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = duplicated_search_term_events.filter(duplicated_search_term_events["domain"] == domain).select("date", "duplicated_searchterm_events")
            data = data.orderBy("date")
            parameters = self.result_data.filter(
                (self.result_data["name"] == func.__name__) &
                (self.result_data["domain"] == domain)
            ).select("parameters").distinct()
            current_day_value, expected, verdict, parameters = self.model(data, parameters)
            result_input = self.create_result_input(func.__name__, domain, dataset, behavior, current_day_value, expected, verdict, parameters)
            self.results.append(result_input)
        


    def events_with_pids_per_domain(self) -> DataFrame:
        events_with_pids_per_domain = self.cube_data.groupby("date", "domain").agg(spark_sum("events_with_PIDs").alias("events_with_PIDs"))
        unique_combinations = events_with_pids_per_domain.select("domain").distinct()
    
        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = events_with_pids_per_domain.filter(events_with_pids_per_domain["domain"] == domain).select("date", "events_with_PIDs")
            data = data.orderBy("date")
            parameters = self.result_data.filter(
                (self.result_data["name"] == func.__name__) &
                (self.result_data["domain"] == domain)
            ).select("parameters").distinct()
            current_day_value, expected, verdict, parameters = self.model(data, parameters)
            result_input = self.create_result_input(func.__name__, domain, dataset, behavior, current_day_value, expected, verdict, parameters)
            self.results.append(result_input)


    def events_with_pids_per_domain_per_dataset(self) -> DataFrame:
        events_with_pids_per_domain_per_dataset = self.cube_data.groupby("date", "domain", "dataset") \
            .agg(spark_sum("events_with_PIDs").alias("events_with_PIDs"))
        unique_combinations = events_with_pids_per_domain_per_dataset.select("domain", "dataset").distinct()
    
        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = events_with_pids_per_domain_per_dataset.filter(
                (events_with_pids_per_domain_per_dataset["domain"] == domain) &
                (events_with_pids_per_domain_per_dataset["dataset"] == dataset)).select("date", "events_with_PIDs")
            data = data.orderBy("date")
            parameters = self.result_data.filter(
                (self.result_data["name"] == func.__name__) &
                (self.result_data["domain"] == domain) &
                (self.result_data["dataset"] == dataset)
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
 



