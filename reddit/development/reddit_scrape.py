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
comments = []
start_date = datetime.utcnow() - timedelta(days=3, minutes=15)
time_stamp = datetime.utcnow().replace(second=0, microsecond=0)
end_date = start_date + timedelta(minutes=15)

for post in reddit.subreddit("all").search(query=query , sort="new", time_filter="week"):
    try:
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
    except Exception as e:
        print("Error: " + str(post.id))
        print(e)
        continue
            
with open (f"reddit/reddit_posts_dump.json", "w") as f:
    json.dump(posts, f, ensure_ascii=False)
with open (f"reddit/reddit_comments_dump.json", "w") as f:
    json.dump(comments, f, ensure_ascii=False)

print(len(posts))
print(len(comments))