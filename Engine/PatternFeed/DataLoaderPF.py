from typing import Any, Dict, List, Union

import pandas as pd
from pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

import os
import glob
import boto3
import ast
import json


class DataLoader:

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
    


    """
    Code iterates through all S3 locations of the specified bucket and prefix and downloads
    files only from the ones, where 
    """
    def load_aggregated_data():

        s3_client = boto3.client("s3")
        bucket_name = "asc-clickstream-emr-output"
        prefix = "trendlines/behavior-platform/agg/data_version=dataset576_V11/"
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)


        agg_data = []
        for obj in response.get("Contents", []):
        
            folder_name = obj["Key"]
            date_string = "-".join([
                folder_name.split("/")[-4].split("=")[1],
                folder_name.split("/")[-3].split("=")[1],
                folder_name.split("/")[-2].split("=")[1]
            ])
            date_object = datetime.strptime(date_string, "%Y-%m-%d")
            timestamp = datetime.timestamp(date_object)
            milliseconds = int(timestamp * 1000)
        
            if milliseconds >= 1709251200000:
                response2 = s3_client.get_object(Bucket=bucket_name, Key=folder_name)
                content = response2["Body"].read().decode("utf-8")
                domain = folder_name.split("/")[-5].split("=")[1]
                json_strings = content.strip().split("\n")
                json_objects = [json.loads(json_str) for json_str in json_strings]
                json_objects = [{"domain": domain, **json_obj} for json_obj in json_objects]   
                agg_data.extend(json_objects)
            
        return agg_data
    

    def load_cubed_data():

        s3_client = boto3.client("s3")
        bucket_name = "asc-clickstream-emr-output"
        prefix = "trendlines/behavior-platform/cube/data_version=dataset576_V11/"
        response = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)

        cube_data = []
        for obj in response.get("Contents", []):
    
            folder_name = obj["Key"]
            date_string = "-".join([
                folder_name.split("/")[-4].split("=")[1],
                folder_name.split("/")[-3].split("=")[1],
                folder_name.split("/")[-2].split("=")[1]
        ])
        date_object = datetime.strptime(date_string, "%Y-%m-%d")
        timestamp = datetime.timestamp(date_object)
        milliseconds = int(timestamp * 1000)
    
        if milliseconds >= 1709251200000:
            response2 = s3_client.get_object(Bucket=bucket_name, Key=folder_name)
            content = response2["Body"].read().decode("utf-8")
            domain = folder_name.split("/")[-5].split("=")[1]
            json_strings = content.strip().split("\n")
            json_objects = [json.loads(json_str) for json_str in json_strings]
            json_objects = [{"domain": domain, **json_obj} for json_obj in json_objects]
            cube_data.extend(json_objects)

        return cube_data


    """
    Function loads only the latest day file
    """
    def load_parameters():

    


