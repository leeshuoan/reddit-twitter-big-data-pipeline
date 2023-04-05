import json
import urllib.parse
import boto3
import snscrape.modules.twitter as sntwitter
from datetime import datetime, timedelta

print('Loading function')

s3 = boto3.client('s3')
# gluejobname = "twitter-comprehend-glue-job-v2"

def lambda_handler(event, context):
    # Set the start and end dates for the search (in UTC timezone)
    start_date = datetime.utcnow()
    time_stamp = start_date.replace(second=0, microsecond=0)
    end_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    crawl_day = datetime.utcnow().strftime("%d-%m-%Y") # dd-mm-yyyy
    bucket="tf-is459-ukraine-war-data"
    source="twitter"
    obj = s3.get_object(Bucket=bucket, Key='topics.txt')
    topics = obj['Body'].read().decode("utf-8").split("\n")

    try:
        for query in topics:
            tweets = []
            key=f"project/{source}/topic={query}/dataload={crawl_day}/{time_stamp}.json"
            for _, tweet in enumerate(sntwitter.TwitterSearchScraper(f"{query} since:{start_date.date()} until:{end_date.date()}").get_items()):
                if tweet.date.time() < (start_date - timedelta(minutes=15)).time():
                    break
                tweets.append({
                    'id': tweet.id,
                    'date': tweet.date.strftime('%Y-%m-%d %H:%M:%S'),
                    'content': tweet.rawContent,
                    "username": tweet.user.username,
                    "followersCount": tweet.user.followersCount,
                    "mentionedUsers": ",".join([user.username for user in tweet.mentionedUsers]) if tweet.mentionedUsers else None,
                    "retweetCount": tweet.retweetCount,
                    "replyCount": tweet.replyCount,
                    "inReplyToUser": tweet.inReplyToUser.username if tweet.inReplyToUser else None,
                    "timeStamp": str(time_stamp)
                })
            tweet_json = json.dumps(tweets)
            response = s3.put_object(Body=tweet_json, Bucket=bucket, Key=key)
        return response
    except Exception as e:
        print(e)
