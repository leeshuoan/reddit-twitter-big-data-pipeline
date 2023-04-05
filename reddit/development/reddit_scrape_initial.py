import json
from datetime import datetime, timedelta
import praw
query = 'ukraine war'

reddit = praw.Reddit(
    client_id="m9BYSa5sn4PPK6cz91s4ZA",
    client_secret="IexB6l0s3CdUYjWj4PCl-GSCbtdI_A",
    password="P@ssword123",
    username="apple-tree3",
    user_agent="IS459-BigData/1.0.0"
)

posts = []
time_stamp = datetime.utcnow().replace(second=0, microsecond=0)
start_date = datetime.utcnow() - timedelta(minutes=15)

for post in reddit.subreddit("all").search(query=query , sort="new", time_filter="day", limit=1000):
    try:
        if datetime.fromtimestamp(post.created_utc) < start_date:
            break
        posts.append({
            'id':str(post.id),
            'date': str(datetime.fromtimestamp(post.created_utc)),
            'title':str(post.title),
            'content':str(post.selftext),
            'username':str(post.author),
            'subreddit':str(post.subreddit)
        })
    except Exception as e:
        print("Error: " + str(post.id))
        print(e)
        continue
            
with open (f"reddit/{time_stamp}_posts.json", "w") as f:
    json.dump(posts, f, ensure_ascii=False)

print(len(posts))
