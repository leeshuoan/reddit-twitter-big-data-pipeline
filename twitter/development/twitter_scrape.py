import json
import snscrape.modules.twitter as sntwitter
from datetime import datetime, timedelta

query = "ukraine war"
# Set the start and end dates for the search (in UTC timezone)
start_date = datetime.utcnow()
time_stamp = start_date.replace(second=0, microsecond=0)
end_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
tweets = []
for i, tweet in enumerate(sntwitter.TwitterSearchScraper(f"{query} since:{start_date.date()} until:{end_date.date()}").get_items()):
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
    
with open (f"twitter/twitter_dump.json", "w") as f:
    json.dump(tweets, f)


# print(tweets)
print(len(tweets))

