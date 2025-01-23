import json
import boto3
import pandas as pd
import os
from io import StringIO

s3 = boto3.client("s3")
 
def lambda_handler(event, context):
    target_bucket = event["target_bucket"]
    directory = event["directory"]
    output_file = event["output_file"]
    key_filename = directory + "/" + output_file
 
    try:
        url = event["url"]
        df = pd.read_json(url)
        df_normalized = pd.json_normalize(df[event["normalize_by"]])

        num_columns = df_normalized.shape[1]
        num_rows = df_normalized.shape[0]
        nan_columns = df_normalized.columns[df_normalized.isna().any()].tolist()

        response = {
            "num_columns": str(num_columns),
            "num_rows": str(num_rows),
            "nan_columns": str(nan_columns)
        }

        s3.put_object(Bucket=target_bucket, Key=key_filename, Body=json.dumps(response))

        return {
            'statusCode': 200,
            'body': json.dumps(response)
        }
 
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(e)})
        }