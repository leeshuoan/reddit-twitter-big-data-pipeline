import json
from datetime import datetime, timedelta
import praw
import os, fnmatch
query = 'ukraine war'

reddit = praw.Reddit(
    client_id="m9BYSa5sn4PPK6cz91s4ZA",
    client_secret="IexB6l0s3CdUYjWj4PCl-GSCbtdI_A",
    password="P@ssword123",
    username="apple-tree3",
    user_agent="IS459-BigData/1.0.0"
)

updated_posts = []
comments = []
post_date = datetime.utcnow() - timedelta(days=3)
# post_date = datetime.utcnow() - timedelta(minutes=15)
time_stamp = datetime.utcnow().replace(second=0, microsecond=0)

# post_files = fnmatch.filter(os.listdir('reddit/'), str(post_date)[:10]+'*_posts.json')
post_files = ['2023-03-27 07-56-00.json']

for file in post_files:
    with open('reddit/'+file) as f:
        posts = json.load(f)

    for post in posts:
        try:
            updated_post = reddit.submission(id=post['id'])
            updated_posts.append({
                'id':str(updated_post.id),
                'date': str(datetime.fromtimestamp(updated_post.created_utc)),
                'title':str(updated_post.title),
                'content':str(updated_post.selftext),
                'username':str(updated_post.author),
                'subreddit':str(updated_post.subreddit),
                'commentCount': int(updated_post.num_comments),
                'score': int(updated_post.score),
            })
            if updated_post.num_comments > 0:
                submission = reddit.submission(id=updated_post['id'])
                submission.comments.replace_more(limit=None)
                for comment in submission.comments.list():
                    if str(comment.author) == "AutoModerator":
                        continue
                    if comment.author == None:
                        continue
                    comments.append({
                        'id': str(comment.id),
                        'date': str(datetime.fromtimestamp(comment.created_utc)),
                        'content': str(comment.body),
                        'username': str(comment.author.name),
                        'score': int(comment.score),
                        'post_id': str(updated_post.id),
                        'parent_id': str(comment.parent_id),
                    })
        except Exception as e:
            # print("Error: " + str(updated_post.id) + ", " + str(comment.id))
            print(e)
            continue
            
with open (f"reddit/{time_stamp}_posts_aggregated.json", "w") as f:
    json.dump(updated_posts, f, ensure_ascii=False)
with open (f"reddit/{time_stamp}_comments.json", "w") as f:
    json.dump(comments, f, ensure_ascii=False)

print(len(updated_posts))
print(len(comments))