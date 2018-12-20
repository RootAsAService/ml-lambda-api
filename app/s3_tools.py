import boto3
import botocore
import json

BUCKET_NAME = 'my-bucket' # replace with your bucket name
KEY = 'my_image_in_s3.jpg' # replace with your object key

s3 = boto3.resource('s3')


def read_stac_item(bucket_name, item_key):
    s3 = boto3.resource('s3')

    try:
        content_object = s3.Object(bucket_name, item_key)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            json_content = {}
        else:
            raise

    return json_content
