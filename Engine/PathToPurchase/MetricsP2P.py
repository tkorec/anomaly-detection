

class Metrics:

    def __init__(self, aggregated_data: DataFrame, cube_data: DataFrame) -> None:
        self.aggregated_data = aggregated_data
        self.cube_data = cube_data
        self.model = Model(data: DataFrame)
        self.results = []


    def number_of_events_per_domain(self):
        number_of_events_per_domain = self.cube_data.groupby("date", "domain", "behavior").agg(spark_sum("events_by_product").alias("events_by_product"))
        unique_combinations = number_of_events_per_domain.select("domain", "behavior").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            behavior = unique_combinations.collect()[i]["behavior"]
            data = number_of_events_per_domain.filter(
                (number_of_events_per_domain["domain"] == domain) &
                (number_of_events_per_domain["behavior"] == behavior)).select("date", "events_by_product")
            data = data.orderBy("date")


    def number_of_events_per_domain_per_dataset(self):
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


    def number_of_events_per_domain_per_patternId(self):
        number_of_events_per_domain_per_patternId = self.aggregated_data.groupby("date", "domain", "patternId") \
            .agg(spark_sum("events_by_product").alias("events_by_product"))
        unique_combinations = number_of_events_per_domain_per_patternId.select("domain", "patternId").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            pattern_id = unique_combinations.collect()[i]["patternId"]
            data = number_of_events_per_domain_per_patternId.filter(
                (number_of_events_per_domain_per_patternId["domain"] == domain) &
                (number_of_events_per_domain_per_patternId["patternId"] == pattern_id)).select("date", "events_by_product")
            data = data.orderBy("date")


    def bot_panelists_per_domain(self):
        bot_panelists_per_domain = self.cube_data.groupby("date", "domain").agg(spark_sum("bot_panelists").alias("bot_panelists"))
        unique_combinations = bot_panelists_per_domain.select("domain").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            data = bot_panelists_per_domain.filter(bot_panelists_per_domain["domain"] == domain).select("date", "bot_panelists")
            data = data.orderBy("date")


    def bot_panelists_per_domain_per_dataset(self):
        bot_panelists_per_domain_per_dataset = self.cube_data.groupby("date", "domain", "dataset").agg(spark_sum("bot_panelists").alias("bot_panelists"))
        unique_combinations = bot_panelists_per_domain_per_dataset.select("domain", "dataset").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            data = bot_panelists_per_domain_per_dataset.filter(
                (bot_panelists_per_domain_per_dataset["domain"] == domain) &
                (bot_panelists_per_domain_per_dataset["dataset"] == dataset)).select("date", "bot_panelists")
            data = data.orderBy("date")

    def bot_events_per_domain(self):
        bot_events_per_domain = self.cube_data.groupby("date", "domain").agg(spark_sum("bot_events_by_product").alias("bot_events_by_product"))
        unique_combinations = bot_events_per_domain.select("domain").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            data = bot_events_per_domain.filter(bot_events_per_domain["domain"] == domain).select("date", "bot_events_by_product")
            data = data.orderBy("date")


    def bot_events_per_domain_per_dataset(self):
        bot_events_per_domain_per_dataset = self.cube_data.groupby("date", "domain", "dataset") \
            .agg(spark_sum("bot_events_by_product").alias("bot_events_by_product"))
        unique_combinations = bot_events_per_domain_per_dataset.select("domain", "dataset").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            dataset = unique_combinations.collect()[i]["dataset"]
            data = bot_events_per_domain_per_dataset.filter(
                (bot_events_per_domain_per_dataset["domain"] == domain) &
                (bot_events_per_domain_per_dataset["dataset"] == dataset)).select("date", "bot_events_by_product")
            data = data.orderBy("date")


    def events_by_product_notnull_pid_per_domain(self):
        events_by_product_notnull_pid_per_domain = self.cube_data.groupby("date", "domain") \
            .agg(spark_sum("events_by_product_notnull_PID").alias("events_by_product_notnull_PID"))
        unique_combinations = events_by_product_notnull_pid_per_domain.select("domain").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            data = events_by_product_notnull_pid_per_domain.filter(events_by_product_notnull_pid_per_domain["domain"] == domain) \
                .select("date", "events_by_product_notnull_PID")
            data = data.orderBy("date")


    def events_by_product_notnull_deterministic_pid_per_domain(self):
        events_by_product_notnull_deterministic_pid_per_domain = self.cube_data.groupby("date", "domain") \
            .agg(spark_sum("events_by_product_notnull_deterministic_PID").alias("events_by_product_notnull_deterministic_PID"))
        unique_combinations = events_by_product_notnull_deterministic_pid_per_domain.select("domain").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            data = events_by_product_notnull_deterministic_pid_per_domain \
                .filter(events_by_product_notnull_deterministic_pid_per_domain["domain"] == domain) \
                .select("date", "events_by_product_notnull_deterministic_PID")
            data = data.orderBy("date")


    def products_in_catalog_per_domain(self):
        products_in_catalog_per_domain = self.cube_data.groupby("date", "domain").agg(spark_sum("products_in_catalog").alias("products_in_catalog"))
        unique_combinations = products_in_catalog_per_domain.select("domain").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            data = products_in_catalog_per_domain.filter(products_in_catalog_per_domain["domain"] == domain).select("date", "products_in_catalog")
            data = data.orderBy("date")


    def events_by_product_in_catalog_per_domain(self):
        events_by_product_in_catalog_per_domain = self.cube_data.groupby("date", "domain") \
            .agg(spark_sum("events_by_product_in_catalog").alias("events_by_product_in_catalog"))
        unique_combinations = events_by_product_in_catalog_per_domain.select("domain").distinct()

        for i in range(0, len(unique_combinations)):
            domain = unique_combinations.collect()[i]["domain"]
            data = events_by_product_in_catalog_per_domain.filter(events_by_product_in_catalog_per_domain["domain"] == domain) \
                .select("date", "events_by_product_in_catalog")
            data = data.orderBy("date")