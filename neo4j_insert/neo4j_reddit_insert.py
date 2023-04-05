from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from dotenv import load_dotenv
import json
import os


def create_post_relationships(tx, id, date, title, content, username, commentCount, score, subreddit):
    query = (
        "MERGE (p1:Post { id: $id, date: $date, title: $title, content: $content, username: $username, commentCount: $commentCount, score: $score, subreddit: $subreddit })"
        "MERGE (r1:Subreddit { name: $subreddit })"
        "MERGE (u1:User { username: $username })"
        "MERGE (p1)-[:POSTED_IN]->(r1)"
        "MERGE (p1)-[:POSTED_BY]->(u1)"
        "RETURN p1, r1, u1"
    )
    result = tx.run(query, id=id, date=date, title=title, content=content, username=username, commentCount=commentCount, score=score, subreddit=subreddit)
    try:
        return [{"p1": row["p1"]["id"], "r1": row["r1"]["name"], "u1": row["u1"]["username"]} for row in result]
    except ServiceUnavailable as exception:
        print("{query} raised an error: \n {exception}".format(
            query=query, exception=exception))
        raise
    

def create_comment_relationships(tx, id, date, content, username, score, post_id):
    query = (
        "MERGE (p1:Post { id: $postId })"
        "MERGE (u1:User { username: $username })"
        "MERGE (c1:Comment { id: $id, date: $date, content: $content, username: $username, score: $score, postId: $postId })"
        "MERGE (c1)-[:COMMENTED_ON]->(p1)"
        "MERGE (c1)-[:COMMENTED_BY]->(u1)"
        "RETURN c1, u1"
    )
    result = tx.run(query, id=id, date=date, content=content, username=username, score=score, postId=post_id)
    try:
        return [{"c1": row["c1"]["id"], "u1": row["u1"]["username"]} for row in result]
    except ServiceUnavailable as exception:
        print("{query} raised an error: \n {exception}".format(
            query=query, exception=exception))
        raise


def create_orchestrator(uri, user, password, posts, commetns):
    data_base_connection = GraphDatabase.driver(uri=uri, auth=(user, password))
    with data_base_connection.session(database="neo4j") as session:
        # Write transactions allow the driver to handle retries and transient errors
        for post in posts:
            result = session.execute_write(create_post_relationships, post["id"], post["date"], post["title"], post["content"], post["username"], post["commentCount"], post["score"], post["subreddit"])
            print(f"Created: {result}")
        for comment in comments:
            result = session.execute_write(create_comment_relationships, comment["id"], comment["date"], comment["content"], comment["username"], comment["score"], comment["post_id"])
            print(f"Created: {result}")


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
        print("{query} raised an error: \n {exception}".format(
            query=query, exception=exception))


def delete_database(uri, user, password):
    data_base_connection = GraphDatabase.driver(uri=uri, auth=(user, password))
    with data_base_connection.session(database="neo4j") as session:
        # Write transactions allow the driver to handle retries and transient errors
        result = session.execute_write(delete_all)
        for row in result:
            print(f"Deleted: {row['n']}")


if __name__ == "__main__":
    load_dotenv()
    with open('reddit_posts_dump.json', 'r') as outfile:
        posts = json.load(outfile)
    with open('reddit_comments_dump.json', 'r') as outfile:
        comments = json.load(outfile)
    uri = os.environ.get("NEO4J_URI")
    user = os.environ.get("NEO4J_USER")
    password = os.environ.get("NEO4J_PASSWORD")
    delete_database(uri, user, password)
    create_orchestrator(uri, user, password, posts, comments)
