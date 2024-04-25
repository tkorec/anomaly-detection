import boto3
import json
import re
from pandas import DataFrame

class DataLoader:

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def load_counting_metrics_results(self) -> DataFrame:
        s3_client = boto3.client("s3")
        bucket_name = "asc-clickstream-emr-output"
        prefix = "monitoring/p2p/dataset_version=dataset576_V11/year=2024/month=4/day=15"
        objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        domain_object = objects.get("Contents", [])
        key = domain_object[0]["Key"]
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        content = response["Body"].read().decode("utf-8")
        content = content.replace("\n", "")
        content = re.sub(r"\s+", "", content)
        content = json.loads(content)
        domain_content = content.get("domains")

        keys_at_position = list(domain_content.keys())
        keys_at_position = sorted(keys_at_position)

        data = []
        for key in keys_at_position:
            domain_response = domain_content.get(key, [])
            domain_response = [{
                "domain": key,
                **dmn_rsp
            } for dmn_rsp in domain_response]
            data.extend(x for x in domain_response)
    
        data = self.spark.createDataFrame(data)
        data = data.filter(F.col("dataset") != "other")

        return data


    def load_time_series_metrics_results(self) -> DataFrame: