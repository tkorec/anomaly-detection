import boto3

class DataLoader():

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark


    def load_aggregated_data():
        # Initialize S3 client
        s3_client = boto3.client("s3")
    
        bucket_name = "asc-clickstream-emr-output"
        prefix = "trendlines/p2p/agg/data_version=dataset576_V11/"
    
        # List objects in the specified bucket and prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
        # Extract folder names
        data = []
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
        
            if milliseconds >= 1710028800000:
                data_response = s3_client.get_object(Bucket=bucket_name,
                                            Key=folder_name)
                content = data_response["Body"].read().decode("utf-8")
                domain = folder_name.split("/")[-5].split("=")[1]
                json_strings = content.strip().split("\n")
                json_objects = [json.loads(json_str) for json_str in json_strings]
                json_objects = [{
                    "domain": domain,
                    **json_obj
                } for json_obj in json_objects]
                data.extend(json_objects)
            
        return data
    


    def load_cubed_data():
        # Initialize S3 client
        s3_client = boto3.client("s3")
    
        bucket_name = "asc-clickstream-emr-output"
        prefix = "trendlines/p2p/cube/data_version=dataset576_V11/"
    
        # List objects in the specified bucket and prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
        # Extract folder names
        data = []
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
        
            if milliseconds >= 1710028800000:
                data_response = s3_client.get_object(Bucket=bucket_name,
                                            Key=folder_name)
                content = data_response["Body"].read().decode("utf-8")
                domain = folder_name.split("/")[-5].split("=")[1]
                json_strings = content.strip().split("\n")
                json_objects = [json.loads(json_str) for json_str in json_strings]
                json_objects = [{
                    "domain": domain,
                    **json_obj
                } for json_obj in json_objects]
                data.extend(json_objects)
            
        return data