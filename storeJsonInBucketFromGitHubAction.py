import json
import boto3
import pandas as pd

s3 = boto3.client("s3")

def lambda_handler(event, context):
    #origin_bucket = event["origin_bucket"]
    origin_bucket = "xideralricardocinepos"
    target_bucket = "xideralricardo"
    directory = "data_revision"
    output_file = "synthetic_data_info.json"
    key_filename = directory + "/" + output_file
    
    json_files_from_origin_bucket = list()
    json_dicts_from_origin_bucket = list()

    try:
        objects_in_origin_bucket = s3.list_objects_v2(Bucket=origin_bucket)

        if "Contents" in objects_in_origin_bucket:
            for obj in objects_in_origin_bucket["Contents"]:
                if obj["Key"].endswith(".json"):
                    json_files_from_origin_bucket.append(obj["Key"])

        for json_file in json_files_from_origin_bucket:
            json_obj = s3.get_object(Bucket=origin_bucket, Key=json_file)
            file_content = json_obj["Body"].read().decode("utf-8")
            data = json.loads(file_content)
            df = pd.DataFrame(data)

            num_rows= df.shape[0]
            num_columns = df.shape[1]
            null_columns = df.columns[df.isnull().any()].tolist()

            json_dict = {
                "json_file": json_file,
                "num_rows": num_rows,
                "num_columns": num_columns,
                "null_columns": null_columns
            }

            json_dicts_from_origin_bucket.append(json_dict)

        s3.put_object(Bucket=target_bucket, Key=key_filename, Body=json.dumps(json_dicts_from_origin_bucket))

        return {
            'statusCode': 200,
            'body': {
                "json_dicts": json_dicts_from_origin_bucket
            }
        }
    
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(e)})
        }