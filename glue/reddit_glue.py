#########################################
# IMPORT LIBRARIES AND SET VARIABLES
#########################################

# Import Python modules
import sys
import requests
from datetime import datetime
from datetime import timedelta
import numpy as np

# Import pyspark modules
from pyspark.context import SparkContext
import pyspark.sql.functions as f
from pyspark.sql import Row

# Import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# Import boto3 modules
import boto3

# Import neo4j modules
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

# Import translate modules
from deep_translator import GoogleTranslator
translator = GoogleTranslator(source='auto', target='english')

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [
                          'JOB_NAME', 'NEO_URI', 'NEO_USER', 'NEO_PASSWORD', 'CLAIMBUSTER_API_KEY'])

# Initialize contexts and session
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# parameters
glue_db = "project-database"
glue_post_tbl = "reddit_posts"
glue_comment_tbl = "reddit_comments"
bucket = "tf-is459-ukraine-war-data"
s3_write_path = f"s3://{bucket}"
uri = args['NEO_URI']
user = args['NEO_USER']
password = args['NEO_PASSWORD']
api_key_claimbuster = args['CLAIMBUSTER_API_KEY']
api_endpoint_claimbuster = "https://idir.uta.edu/claimbuster/api/v2/score/text/"

s3 = boto3.client('s3')
obj = s3.get_object(Bucket=bucket, Key='topics.txt')
topics = obj['Body'].read().decode("utf-8").split("\n")
crawl_date = (datetime.utcnow()-timedelta(days=1)).strftime("%d-%m-%Y")

print("Start")
#########################################
# NEO4j Functions
#########################################

# post["username"], post["commentCount"], post["score"], post["subreddit"],  post['topic'], post['Positive'],  post['Negative'],  post['Neutral'],  post['Mixed'],  post['claimScore']


def create_post_relationships(tx, id, date, title, content, username, commentCount, score, subreddit, topic, positive, negative, neutral, mixed, claimScore):
    query = (
        "MERGE (p1:Post_Reddit { id: $id, date: $date, title: $title, content: $content, username: $username, commentCount: $commentCount, score: $score, subreddit: $subreddit, \
            topic: $topic, positive: $positive, negative: $negative, neutral: $neutral, mixed: $mixed, claimScore: $claimScore })"
        "MERGE (r1:Subreddit_Reddit { name: $subreddit })"
        "MERGE (u1:User_Reddit { username: $username })"
        "MERGE (p1)-[:POSTED_IN]->(r1)"
        "MERGE (p1)-[:POSTED_BY]->(u1)"
        "RETURN p1, r1, u1"
    )
    result = tx.run(query, id=id, date=date, title=title, content=content,
                    username=username, commentCount=commentCount, score=score, subreddit=subreddit, 
                    topic=topic, positive=positive, negative=negative, neutral=neutral, mixed=mixed, 
                    claimScore=claimScore)
    try:
        return [{"p1": row["p1"]["id"], "r1": row["r1"]["name"], "u1": row["u1"]["username"]} for row in result]
    except ServiceUnavailable as exception:
        print("{query} raised an error: \n {exception}".format(
            query=query, exception=exception))
        raise


def create_comment_relationships(tx, id, date, content, username, score, post_id, topic, positive, negative, neutral, mixed, claimScore ):
    query = (
        "MATCH (p1:Post_Reddit { id: $postId }) "
        "MERGE (c1:Comment_Reddit { id: $id, date: $date, content: $content, username: $username, score: $score, postId: $postId, topic: $topic, positive: $positive, negative: $negative, neutral: $neutral, mixed: $mixed, claimScore: $claimScore })-[:COMMENTED_ON]->(p1) "
        "WITH c1 "
        "MERGE (c1)-[:COMMENTED_BY]->(u1:User_Reddit { username: $username }) "
        "RETURN c1, u1"
    )
    result = tx.run(query, id=id, date=date, content=content,
                    username=username, score=score, postId=post_id,
                    topic=topic, positive=positive, negative=negative, neutral=neutral, mixed=mixed, 
                    claimScore=claimScore)
    try:
        return [{"c1": row["c1"]["id"], "u1": row["u1"]["username"]} for row in result]
    except ServiceUnavailable as exception:
        print("{query} raised an error: \n {exception}".format(
            query=query, exception=exception))
        raise


def create_orchestrator(uri, user, password, posts, comments):
    print("Creating Graph Driver")
    data_base_connection = GraphDatabase.driver(uri=uri, auth=(user, password))
    print("Establishing connection")
    with data_base_connection.session(database="neo4j") as session:
        # Write transactions allow the driver to handle retries and transient errors
        for post in posts:
            result = session.execute_write(create_post_relationships, post["id"], post["date"], post["title"], post["content"],
                                           post["username"], post["commentCount"], post["score"], post["subreddit"],  post['topic'], post['Positive'],  post['Negative'],  post['Neutral'],  post['Mixed'],  post['claimScore'])
            print(f"Created: {result}")
        for comment in comments:
            result = session.execute_write(
                create_comment_relationships, comment["id"], comment["date"], comment["content"], comment["username"], comment["score"], comment["post_id"],  post['topic'], post['Positive'],  post['Negative'],  post['Neutral'],  post['Mixed'],  post['claimScore'])
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

#########################################
# AWS Comprehend Function (Sentiment Analysis)
#########################################


def get_sentiment(text_list):
    # Initialize an empty list for storing sentiment results
    sentiments = []
    # Initialize an Amazon Comprehend client object with your region name (replace with your own region)
    comprehend = boto3.client(
        service_name='comprehend', region_name='us-east-1')
    # Split the text list into batches of 25 documents each (the maximum number of documents per request for Amazon Comprehend)
    batches = [text_list[i:i+25] for i in range(0, len(text_list), 25)]
    # batches = [[x[:4500] for x in text_list[i:i+25] if x] for i in range(0,len(text_list),25)]
    # Iterate over each batch and call the Amazon Comprehend API to analyze sentiment
    for i, batch in enumerate(batches):
        response = comprehend.batch_detect_sentiment(
            TextList=batch, LanguageCode='en')
        # Extract the sentiment scores from the response and append them to the sentiments list as Row objects
        for item in response['ResultList']:
            index = i*len(batch) + item['Index']
            score = item['SentimentScore']
            sentiments.append({
                "index": index,
                "Positive": score["Positive"],
                "Negative": score["Negative"],
                "Neutral": score["Neutral"],
                "Mixed": score["Mixed"]
            })
    # Return the sentiments list sorted by index
    return sorted(sentiments, key=lambda x: x["index"])

#########################################
# Claimbuster Function to call api
#########################################


def invoke_claimbuster_api(input_claim):
    try:
        api_response = requests.get(
            url=api_endpoint_claimbuster+input_claim, headers={"x-api-key": api_key_claimbuster})
        data = api_response.json()
        if data["results"]:
            return data["results"][0]["score"]
        return 0
    except:
        return 0


for query in topics:
    #########################################
    # EXTRACT (READ DATA)
    #########################################
    print(query)
    dynamic_post_frame_read = glue_context.create_dynamic_frame.from_catalog(
        database=glue_db,
        table_name=glue_post_tbl,
        push_down_predicate=f"topic == '{query}' and dataload == '{crawl_date}'"
    )
    dynamic_comment_frame_read = glue_context.create_dynamic_frame.from_catalog(
        database=glue_db,
        table_name=glue_comment_tbl,
        push_down_predicate=f"topic == '{query}' and dataload == '{crawl_date}'"
    )

    # Convert dynamic frame to data frame to use standard pyspark functions
    data_post_frame = dynamic_post_frame_read.toDF().toPandas()
    data_comment_frame = dynamic_comment_frame_read.toDF().toPandas()

    # Extract out posts and comments of that timestamp
    print(data_post_frame)
    data_post_frame = data_post_frame
    data_comment_frame = data_comment_frame

    # ETL to remove empty content from posts & comments
    data_post_frame.to_csv("s3://wklee-is459/write/reddit_raw_posts.csv")
    data_comment_frame.to_csv("s3://wklee-is459/write/reddit_raw_comments.csv")
    data_post_frame = data_post_frame.replace("", np.nan)
    data_comment_frame = data_comment_frame.replace("", np.nan)
    data_post_frame.dropna(inplace=True)
    data_comment_frame.dropna(inplace=True)

    # Apply translate and replace the content in place
    data_post_frame["content"] = data_post_frame.content.apply(
        translator.translate)
    data_comment_frame["content"] = data_comment_frame.content.apply(
        translator.translate)
    print("Translated")
    # ETL to remove empty content due to failed translations from posts & comments
    data_post_frame["content"] = data_post_frame["content"].replace("", np.nan)
    data_post_frame["content"] = data_post_frame["content"].replace("[deleted]", np.nan)
    data_post_frame["username"] = data_post_frame["username"].replace("None", np.nan)
    data_post_frame["username"] = data_post_frame["username"].replace("", np.nan)

    data_comment_frame["content"] = data_comment_frame["content"].replace("", np.nan)
    data_comment_frame["content"] = data_comment_frame["content"].replace("[deleted]", np.nan)
    data_comment_frame["username"] = data_comment_frame["username"].replace("None", np.nan)
    data_comment_frame["username"] = data_comment_frame["username"].replace("", np.nan)

    data_post_frame.dropna(subset=["content"], inplace=True)
    data_post_frame.dropna(subset=["username"], inplace=True)
    data_comment_frame.dropna(subset=["content"], inplace=True)
    data_comment_frame.dropna(subset=["username"], inplace=True)
    print("Replaced")

    print("Getting sentiments")
    # Apply AWS Comprehend and add sentiment score as a new column in the dataframe
    post_sentiments = get_sentiment(data_post_frame.content.to_list())
    print(f"Sentiments: {post_sentiments}")
    for key in post_sentiments[0].keys():
        data_post_frame[key] = [x[key] for x in post_sentiments]
    comment_sentiments = get_sentiment(data_comment_frame.content.to_list())
    for key in comment_sentiments[0].keys():
        data_comment_frame[key] = [x[key] for x in comment_sentiments]

    print("Writing final")
    # Call claimbuster to get claim score and add it as a new column in the dataframe
    data_post_frame["claimScore"] = data_post_frame.content.apply(
        invoke_claimbuster_api)
    data_comment_frame["claimScore"] = data_comment_frame.content.apply(
        invoke_claimbuster_api)
    data_post_frame.to_csv("s3://wklee-is459/write/reddit_final_posts.csv")
    data_comment_frame.to_csv(
        "s3://wklee-is459/write/reddit_final_comments.csv")

    #########################################
    # LOAD (WRITE DATA)
    #########################################
    print("Ochestrating")
    create_orchestrator(uri, user, password, data_post_frame.to_dict(
        'records'), data_comment_frame.to_dict('records'))

job.commit()
