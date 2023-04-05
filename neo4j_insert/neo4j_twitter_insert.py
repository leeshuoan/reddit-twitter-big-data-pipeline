from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import json
import os

def create_and_return_relationship(tx, tweet_username, mentioned_username, content, date, id, followersCount, retweetCount, replyCount, inReplyToUser):
        query = (
            "MERGE (u1:User { username: $tweet_username, content: $content, date: $date, id: $id, followersCount: $followersCount, retweetCount: $retweetCount, replyCount: $replyCount }) "
            "MERGE (u2:User { username: $mentioned_username }) "
            "MERGE (u3:User { username: $inReplyToUser }) "
            "MERGE (u1)-[:mentioned]->(u2) "
            "MERGE (u1)-[:inReplyToUser]->(u3) "
            "RETURN u1, u2, u3"
        )
        result = tx.run(query, tweet_username=tweet_username, mentioned_username=mentioned_username, content=content, date=date, id=id, followersCount=followersCount, retweetCount=retweetCount, replyCount=replyCount, inReplyToUser=inReplyToUser)
        try:
            return [{"u1": row["u1"]["username"], "u2": row["u2"]["username"], "u3": row["u3"]["username"]} for row in result]
        except ServiceUnavailable as exception:
            print("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

def create_with_no_mentioned_user(tx, tweet_username, content, date, id, followersCount, retweetCount, replyCount, inReplyToUser):
        query = (
            "MERGE (u1:User { username: $tweet_username, content: $content, date: $date, id: $id, followersCount: $followersCount, retweetCount: $retweetCount, replyCount: $replyCount }) "
            "MERGE (u2:User { username: $inReplyToUser }) "
            "MERGE (u1)-[:inReplyToUser]->(u2) "
            "RETURN u1, u2"
        )
        result = tx.run(query, tweet_username=tweet_username, content=content, date=date, id=id, followersCount=followersCount, retweetCount=retweetCount, replyCount=replyCount, inReplyToUser=inReplyToUser)
        try:
            return [{"u1": row["u1"]["username"], "u2": row["u2"]["username"]} for row in result]
        except ServiceUnavailable as exception:
            print("{query} raised an error: \n {exception}".format(query=query, exception=exception))

def create_with_no_reply_to_user(tx, tweet_username, mentioned_username, content, date, id, followersCount, retweetCount, replyCount):
        query = (
            "MERGE (u1:User { username: $tweet_username, content: $content, date: $date, id: $id, followersCount: $followersCount, retweetCount: $retweetCount, replyCount: $replyCount }) "
            "MERGE (u2:User { username: $mentioned_username }) "
            "MERGE (u1)-[:mentioned]->(u2) "
            "RETURN u1, u2"
        )
        result = tx.run(query, tweet_username=tweet_username, mentioned_username=mentioned_username, content=content, date=date, id=id, followersCount=followersCount, retweetCount=retweetCount, replyCount=replyCount)
        try:
            return [{"u1": row["u1"]["username"], "u2": row["u2"]["username"]} for row in result]
        except ServiceUnavailable as exception:
            print("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

def create_and_return(tx, tweet_username, content, date, id, followersCount, retweetCount, replyCount):
    query = (
        "MERGE (u:User { username: $tweet_username, content: $content, date: $date, id: $id, followersCount: $followersCount, retweetCount: $retweetCount, replyCount: $replyCount }) "
        "RETURN u"
    )
    result = tx.run(query, tweet_username=tweet_username, content=content, date=date, id=id, followersCount=followersCount, retweetCount=retweetCount, replyCount=replyCount)
    try:
        return [{"u": row["u"]["username"]} for row in result]
    except ServiceUnavailable as exception:
        print("{query} raised an error: \n {exception}".format(query=query, exception=exception))

def create_orchestrator(uri, user, password, tweets):
    data_base_connection = GraphDatabase.driver(uri = uri, auth=(user, password))
    with data_base_connection.session(database="neo4j") as session:
        # Write transactions allow the driver to handle retries and transient errors
        for tweet in tweets:
            if tweet['mentionedUsers'] and tweet['inReplyToUser']:
                for mentioned_user in tweet['mentionedUsers'].split(','):
                    result = session.execute_write(create_and_return_relationship, tweet['username'], mentioned_user, tweet['content'], tweet['date'], tweet['id'], tweet['followersCount'], tweet['retweetCount'], tweet['replyCount'], tweet['inReplyToUser'])
                    for row in result:
                        print(f"Created friendship between: {row['u1']}, {row['u2']}, {row['u3']}")
            elif tweet['mentionedUsers']:
                for mentioned_user in tweet['mentionedUsers'].split(','):
                    result = session.execute_write(create_with_no_reply_to_user, tweet['username'], mentioned_user, tweet['content'], tweet['date'], tweet['id'], tweet['followersCount'], tweet['retweetCount'], tweet['replyCount'])
                    for row in result:
                        print(f"Created friendship between: {row['u1']}, {row['u2']}")
            elif tweet['inReplyToUser']:
                result = session.execute_write(create_with_no_mentioned_user, tweet['username'], tweet['content'], tweet['date'], tweet['id'], tweet['followersCount'], tweet['retweetCount'], tweet['replyCount'], tweet['inReplyToUser'])
                for row in result:
                    print(f"Created friendship between: {row['u1']}, {row['u2']}")
            else:
                result = session.execute_write(create_and_return, tweet['username'], tweet['content'], tweet['date'], tweet['id'], tweet['followersCount'], tweet['retweetCount'], tweet['replyCount'])
                for row in result:
                    print(f"Created user: {row['u']}")

def delete_all(tx):
    query = (
        "MATCH (n) DETACH DELETE n "
        "RETURN n"
    )
    result = tx.run(query)
    try:
        return [{"n": row["n"]["username"]} for row in result]
    # Capture any errors along with the query and data for traceability
    except ServiceUnavailable as exception:
        print("{query} raised an error: \n {exception}".format(query=query, exception=exception))

def delete_database(uri, user, password):
    data_base_connection = GraphDatabase.driver(uri = uri, auth=(user, password))
    with data_base_connection.session(database="neo4j") as session:
        # Write transactions allow the driver to handle retries and transient errors
        result = session.execute_write(delete_all)
        for row in result:
            print(f"Deleted: {row['n']}")

if __name__ == "__main__":
    with open('twitter_dump.json', 'r') as outfile:
        tweets = json.load(outfile)
    uri = os.environ.get("NEO4J_URI")
    user = os.environ.get("NEO4J_USER")
    password = os.environ.get("NEO4J_PASSWORD")
    delete_database(uri, user, password)
    create_orchestrator(uri, user, password, tweets)
