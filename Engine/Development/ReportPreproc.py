import boto3
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.appName("PreprocessReport").getOrCreate()


s3_client = boto3.client("s3")
bucket_name = "asc-clickstream-emr-output"
prefix = "monitoring/preproc/dataset_version=dataset576_V6_1/year=2024/month=4/day=15"
objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

domain_object = objects.get("Contents", [])[1]
key = domain_object["Key"]
response = s3_client.get_object(Bucket=bucket_name, Key=key)
content = response["Body"].read().decode("utf-8")
json = json.loads(content)
domain_json = json.get("domains")
keys_at_position = list(domain_json.keys())

data = []
for key in keys_at_position:
    domain_response = domain_json.get(key, [])
    domain_response = [{
        "domain": key,
        **dmn_rsp
    } for dmn_rsp in domain_response]
    data.extend(x for x in domain_response)  
    
    
data = spark.createDataFrame(data)
data = data.filter(F.col("dataset") != "other")
        

def domain_dataset_results(nok_data):
    results = ""
    for i in range(0, nok_data.count()):
        results += f"""
            <table class="values" style="width: 100%; margin-top: 5px; background-color: #FFE3E3; border: 1.5px solid #FBC0C0;">
                <tr>
                    <td style="color: #FF8282; font-size: 12px; padding: 10px 0 10px 10px;">{nok_data.collect()[i]["domain"]} - {nok_data.collect()[i]["dataset"]}</td>
                    <td style="color: #FF8282; font-size: 12px; padding: 10px 0; text-align: center;">
                        value: {nok_data.collect()[i]["value"]};   
                        expected: {nok_data.collect()[i]["expected"]};  
                        difference: {nok_data.collect()[i]["difference"]}; 
                        relative difference: {nok_data.collect()[i]["relativedifference"]};
                        z-score: 2.67: {nok_data.collect()[i]["z_score"]}
                    </td>
                    <td style="color: #FF8282; font-size: 12px; padding: 10px 10px 10px 0; text-align: end;">{nok_data.collect()[i]["verdict"]}</td>
                </tr>
            </table>"""
    legend = """
        <table class="legend" style="width: 100%; margin-top: 5px;">
            <tr>
                <td style="font-size: 12px; padding: 0 0 7px 0;">Domain - dataset</td>
                <td style="font-size: 12px; padding: 0 0 7px 0; text-align: end;">Problem Type [NOK, MISSING, ERROR]</td>
            </tr>
        </table>"""
    return results, legend

def metric_wrapper_func(metric_name):
    metric_wrapper = f"""
        <div class="metric-wrapper" style="margin-top: 50px; width: 95%">
            <p class="metric-name" style="font-weight: 600; font-size: 14px;">{metric_name}</p>
        </div>"""
    return metric_wrapper


def count_rows():
    count_rows = self.data.filter(self.data["name"] == func.__name__)
    nok_data = count_rows.filter(count_rows["verdict"] != "OK").groupBy(F.desc("difference"))
    metric_wrapper = metric_wrapper_func(func.__name__)
    results, legend = domain_dataset_results(nok_data)
    insert_position = metric_wrapper.find("</div>")
    monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
    return monitoring_result

def count_rows_per_dataset():
    count_rows_per_dataset = self.data.filter(self.data["name"] == func.__name__)
    nok_data = count_rows_per_dataset.filter(count_rows_per_dataset["verdict"] != "OK").groupBy(F.desc("difference"))
    metric_wrapper = metric_wrapper_func(func.__name__)
    results, legend = domain_dataset_results(nok_data)
    insert_position = metric_wrapper.find("</div>")
    monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
    return monitoring_result

def count_unique_panelists():
    count_unique_panelists = self.data.filter(self.data["name"] == func.__name__)
    nok_data = count_unique_panelists.filter(count_unique_panelists["verdict"] != "OK").groupBy(F.desc("difference"))
    metric_wrapper = metric_wrapper_func(func.__name__)
    results, legend = domain_dataset_results(nok_data)
    insert_position = metric_wrapper.find("</div>")
    monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
    return monitoring_result

def count_unique_panelists_per_dataset():
    count_unique_panelists_per_dataset = self.data.filter(self.data["name"] == func.__name__)
    nok_data = count_unique_panelists_per_dataset.filter(count_unique_panelists_per_dataset["verdict"] != "OK").groupBy(F.desc("verdict"))
    metric_wrapper = metric_wrapper_func(func.__name__)
    results, legend = domain_dataset_results(nok_data)
    insert_position = metric_wrapper.find("</div>")
    monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
    return monitoring_result

def count_valid_clicks_ratio_per_dataset():
    count_valid_clicks_ratio_per_dataset = self.data.filter(self.data["name"] == func.__name__)
    nok_data = count_valid_clicks_ratio_per_dataset.filter(count_valid_clicks_ratio_per_dataset["verdict"] != "OK").groupBy(F.desc("verdict"))
    metric_wrapper = metric_wrapper_func(func.__name__)
    results, legend = domain_dataset_results(nok_data)
    insert_position = metric_wrapper.find("</div>")
    monitoring_result = metric_wrapper[:insert_position] + legend + results + metric_wrapper[insert_position:]
    return monitoring_result



