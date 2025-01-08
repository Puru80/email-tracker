import boto3, botocore
import requests


def lambda_handler(event, context):
    print(f"boto3 version: {boto3.__version__}")
    print(f"botocore version: {botocore.__version__}")

    try:
        response = requests.get(api_url)
        print(f"API response status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error hitting API: {e}")
