import json
import boto3
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import praw

load_dotenv()

client_id = os.environ.get("REDDIT_CLIENT_ID")
client_secret = os.environ.get("REDDIT_CLIENT_SECRET")
password = os.environ.get("REDDIT_PASSWORD")
username = os.environ.get("REDDIT_USERNAME")
user_agent = os.environ.get("REDDIT_USER_AGENT")

print('Loading function')

s3 = boto3.client('s3')

reddit = praw.Reddit(
    client_id=client_id,
    client_secret=client_secret,
    password=password,
    username=username,
    user_agent=user_agent
)

def lambda_handler(event, context):
    # Set the start and end dates for the search (in UTC timezone)
    start_date = datetime.utcnow() - timedelta(days=3, minutes=15)
    time_stamp = datetime.utcnow().replace(second=0, microsecond=0)
    end_date = start_date + timedelta(minutes=15)
    bucket="is459-ukraine-war-data"
    obj = s3.get_object(Bucket=bucket, Key='topics.txt')
    topics = obj['Body'].read().decode("utf-8").split("\n")

    try:
        for query in topics:
            posts = []
            comments = []
            posts_key=f"project/{query}/reddit/{time_stamp}_posts.json"
            comments_key=f"project/{query}/reddit/{time_stamp}_comments.json"
            for post in reddit.subreddit("all").search(query=query , sort="new", time_filter="week"):
                if datetime.fromtimestamp(post.created_utc) < start_date or datetime.fromtimestamp(post.created_utc) > end_date:
                    continue
                posts.append({
                    'id':str(post.id),
                    'date': str(datetime.fromtimestamp(post.created_utc)),
                    'title':str(post.title),
                    'content':str(post.selftext),
                    'username':str(post.author),
                    'commentCount':int(post.num_comments),
                    'score':int(post.score),
                    'subreddit':str(post.subreddit)
                })
                if post.num_comments > 0:
                    submission = reddit.submission(id=post.id)
                    submission.comments.replace_more(limit=None)
                    for comment in submission.comments.list():
                        if str(comment.author) == "AutoModerator":
                            continue
                        comments.append({
                            'id': str(comment.id),
                            'date': str(datetime.fromtimestamp(comment.created_utc)),
                            'content': str(comment.body),
                            'username': str(comment.author.name),
                            'score': int(comment.score),
                            'post_id': str(post.id),
                            'parent_id': str(comment.parent_id),
                        })
                        
            posts_json = json.dumps(posts)
            comments_json = json.dumps(comments)
            response1 = s3.put_object(Body=posts_json, Bucket=bucket, Key=posts_key)
            response2 = s3.put_object(Body=comments_json, Bucket=bucket, Key=comments_key)
        return response1, response2
    except Exception as e:
        print(e)
