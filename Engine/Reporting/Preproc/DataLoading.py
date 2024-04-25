import boto3
import json
from pandas import DataFrame

class DataLoader:

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        
    
    def load_counting_metrics_results(self) -> DataFrame:

        s3_client = boto3.client("s3")
        bucket_name = "asc-clickstream-emr-output"
        prefix = "monitoring/preproc/dataset_version=dataset576_V6_1/year=2024/month=4/day=15"
        objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        domain_object = objects.get("Contents", [])[1]
        key = domain_object["Key"]
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        content = response["Body"].read().decode("utf-8")
        json_form = json.loads(content)
        domain_json = json_form.get("domains")
        keys_at_position = list(domain_json.keys())

        data = []
        for key in keys_at_position:
            domain_response = domain_json.get(key, [])
            domain_response = [{
                "domain": key,
                **dmn_rsp
        } for dmn_rsp in domain_response]
        data.extend(x for x in domain_response)  
       
        data = self.spark.createDataFrame(data)
        data = data.filter(F.col("dataset") != "other")

        return data
    

    def load_time_series_metrics_results(self) -> DataFrame:


