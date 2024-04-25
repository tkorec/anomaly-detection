class Metrics:


    def __init__(self, aggregated_data: DataFrame, cube_data: DataFrame) -> None:
        self.aggregated_data = aggregated_data
        self.cube_data = cube_data
        self.model = Model(data: DataFrame)
        self.results = []



    def number_of_events_per_domain(self) -> DataFrame:
        number_of_events_per_domain = self.aggregated_data.groupby("date", "domain", "behavior").agg(spark_sum("events").alias("events"))
        unique_combinations = number_of_events_per_domain.select("domain", "behavior").distinct()

    
        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = number_of_events_per_domain.filter(
                (number_of_events_per_domain["domain"] == domain) & 
                (number_of_events_per_domain["behavior"] == behavior)).select("date", "events") 
            data = data.orderBy("date")

            forecast, verdict, p, d, q = self.model(data)

            result_input = {
                "date": "",
                "metric": "number-of-events-per-domain",
                "domain": domain, 
                "behavior": behavior, 
                "forecast": forecast, 
                "confidential_interval": confidential_interval,
                "current_day_value": current_day_value,
                "verdict": verdict,
                "attributes": [p, d, q]
                }
            
            self.results.append(result_input)
            
        


    def number_of_events_per_domain_per_dataset(self) -> DataFrame:
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
        


    def number_of_events_per_domain_per_patternId(self) -> DataFrame:
        number_of_events_per_domain_per_patternId = self.aggregated_data.groupby("date", "domain", "patternId").agg(spark_sum("events").alias("events"))
        unique_combinations = number_of_events_per_domain_per_patternId.select("domain", "patternId").distinct()
    
        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            patternId = unique_combinations.collect()[i]["patternId"]
            data = number_of_events_per_domain_per_patternId.filter(
                (number_of_events_per_domain_per_patternId["domain"] == domain) &
                (number_of_events_per_domain_per_patternId["patternId"] == patternId)).select("date", "events")
            data = data.orderBy("date")
        


    def bot_panelists_per_domain(self) -> DataFrame:
        bot_panelists_per_domain = self.cube_data.groupby("date", "domain").agg(spark_sum("bot_panelists").alias("bot_panelists"))
        unique_combinations = bot_panelists_per_domain.select("domain").distinct()
    
        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            data = bot_panelists_per_domain.filter(bot_panelists_per_domain["domain"] == domain).select("date", "bot_panelists")
            data = data.orderBy("date")
        
    
    
    def bot_panelists_per_domain_per_dataset(self) -> DataFrame:
        bot_panelists_per_domain_per_dataset = self.cube_data.groupby("date", "domain", "dataset").agg(spark_sum("bot_panelists").alias("bot_panelists"))
        unique_combinations = bot_panelists_per_domain_per_dataset.select("domain", "dataset").distinct()
    
        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            data = bot_panelists_per_domain_per_dataset.filter(
                (bot_panelists_per_domain_per_dataset["domain"] == domain) &
                (bot_panelists_per_domain_per_dataset["dataset"] == dataset)).select("date", "bot_panelists")
            data = data.orderBy("date")
        


    def bot_events_per_domain(self) -> DataFrame:
        bot_events_per_domain = self.cube_data.groupby("date", "domain").agg(spark_sum("bot_events").alias("bot_events"))
        unique_combinations = bot_events_per_domain.select("domain").distinct()
    
        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            data = bot_events_per_domain.filter(bot_events_per_domain["domain"] == domain).select("date", "bot_events")
            data = data.orderBy("date")
        


    def bot_events_per_domain_per_dataset(self) -> DataFrame:
        bot_events_per_domain_per_dataset = self.cube_data.groupby("date", "domain", "dataset").agg(spark_sum("bot_events").alias("bot_events"))
        unique_combinations = bot_events_per_domain_per_dataset.select("domain", "dataset").distinct()
    
        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            data = bot_events_per_domain_per_dataset.filter(
                (bot_events_per_domain_per_dataset["domain"] == domain) &
                (bot_events_per_domain_per_dataset["dataset"] == dataset)).select("date", "bot_events")
            data = data.orderBy("date")



    def duplicated_search_term_events(self) -> DataFrame:
        duplicated_search_term_events = self.cube_data.groupby("date", "domain") \
            .agg(spark_sum("duplicated_searchterm_events").alias("duplicated_searchterm_events"))
        unique_combinations = duplicated_search_term_events.select("domain").distinct()
    
        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            data = duplicated_search_term_events.filter(duplicated_search_term_events["domain"] == domain).select("date", "duplicated_searchterm_events")
            data = data.orderBy("date")
        


    def events_with_pids_per_domain(self) -> DataFrame:
        events_with_pids_per_domain = self.cube_data.groupby("date", "domain").agg(spark_sum("events_with_PIDs").alias("events_with_PIDs"))
        unique_combinations = events_with_pids_per_domain.select("domain").distinct()
    
        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            data = events_with_pids_per_domain.filter(events_with_pids_per_domain["domain"] == domain).select("date", "events_with_PIDs")
            data = data.orderBy("date")


    def events_with_pids_per_domain_per_dataset(self) -> DataFrame:
        events_with_pids_per_domain_per_dataset = self.cube_data.groupby("date", "domain", "dataset") \
            .agg(spark_sum("events_with_PIDs").alias("events_with_PIDs"))
        unique_combinations = events_with_pids_per_domain_per_dataset.select("domain", "dataset").distinct()
    
        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            data = events_with_pids_per_domain_per_dataset.filter(
                (events_with_pids_per_domain_per_dataset["domain"] == domain) &
                (events_with_pids_per_domain_per_dataset["dataset"] == dataset)).select("date", "events_with_PIDs")
            data = data.orderBy("date")
 


