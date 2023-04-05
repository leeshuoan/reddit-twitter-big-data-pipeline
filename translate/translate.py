import json
import boto3
from deep_translator import GoogleTranslator
from typing import Dict, List

DATA_LOCATION = {
    "bucket": "is459-ukraine-war-data",
    "twitter": {
        "database_name": "twitter-crawler-database",
        "table_name": "project",
        "file_path": "project/russia war/twitter/"
    },
    "reddit_posts": {
        "database_name": "reddit-crawler-database",
        "table_name": "postsreddit",
        "file_path": "project/russia war/reddit/"
    },
    "reddit_comments": {
        "database_name": "reddit-crawler-database",
        "table_name": "postsreddit",
        "file_path": "project/russia war/reddit/"
    }
}


def fetch_data_catalog(source: str) -> List[Dict[str, str]]:
    glue = boto3.client('glue')
    if source == "twitter":
        catalog_table = glue.get_table(
            DatabaseName=DATA_LOCATION["twitter"]["database_name"], Name=DATA_LOCATION["twitter"]["table_name"])
    elif source == "reddit_posts":
        catalog_table = glue.get_table(
            DatabaseName=DATA_LOCATION["reddit_posts"]["database_name"], Name=DATA_LOCATION["reddit_posts"]["table_name"])
    elif source == "reddit_comments":
        catalog_table = glue.get_table(
            DatabaseName=DATA_LOCATION["reddit_comments"]["database_name"], Name=DATA_LOCATION["reddit_comments"]["table_name"])
    data_columns = catalog_table["Table"]["StorageDescriptor"]["Columns"]
    return data_columns


def translate_tweet(filename: str):
    s3 = boto3.client('s3')
    bucket = DATA_LOCATION["bucket"]
    file_path = DATA_LOCATION["twitter"]["file_path"]

    data_columns = fetch_data_catalog("twitter")
    content_obj = {}
    for index, column in enumerate(data_columns):
        column_key = column['Name']
        content_obj[column_key] = None

    translated_content = []
    s3_response_obj = s3.get_object(Bucket=bucket, Key=file_path+filename)
    json_content = json.loads(s3_response_obj['Body'].read())
    translator = GoogleTranslator(source='auto', target='english')
    for obj in json_content:
        for key, value in obj.items():
            if key == "content":
                content_obj[key] = translator.translate(value)
            else:
                content_obj[key] = value
        translated_content.append(content_obj)
    return {
        'statusCode': 200,
        'result': json.dumps(translated_content)
    }


def translate_reddit(filename: str, source: str):
    s3 = boto3.client('s3')
    bucket = DATA_LOCATION["bucket"]
    if source == "posts":
        file_path = DATA_LOCATION["reddit_posts"]["file_path"]
        data_columns = fetch_data_catalog("reddit_posts")
    elif source == "comments":
        file_path = DATA_LOCATION["reddit_comments"]["file_path"]
        data_columns = fetch_data_catalog("reddit_comments")

    content_obj = {}
    for index, column in enumerate(data_columns):
        column_key = column['Name']
        content_obj[column_key] = None

    translated_content = []
    s3_response_obj = s3.get_object(Bucket=bucket, Key=file_path+filename)
    json_content = json.loads(s3_response_obj['Body'].read())
    translator = GoogleTranslator(source='auto', target='english')
    for obj in json_content:
        for key, value in obj.items():
            if key == "content" or key == "title":
                content_obj[key] = translator.translate(value)
            else:
                content_obj[key] = value
        translated_content.append(content_obj)
    return {
        'statusCode': 200,
        'result': json.dumps(translated_content)
    }


print(translate_tweet("2023-03-16 08:32:00.json"))
