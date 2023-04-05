#########################################
# IMPORT LIBRARIES AND SET VARIABLES
#########################################

# Import Python modules
import sys

from datetime import datetime, timedelta
import requests

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
                          'JOB_NAME',
                          'NEO_URI',
                          'NEO_USER',
                          'NEO_PASSWORD',
                          'CLAIMBUSTER_API_KEY'])

# Initialize contexts and session
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Parameters
glue_db = "project-database"
glue_tbl = "twitter"  # data catalog table
crawl_day = (datetime.utcnow()  - timedelta(days=1)).strftime("%d-%m-%Y")  # dd-mm-yyyy
folder = f"project/twitter/topic=ukraine war/dataload={crawl_day}/"
bucket = "tf-is459-ukraine-war-data"
uri = args['NEO_URI']
user = args['NEO_USER']
password = args['NEO_PASSWORD']
api_key_claimbuster = args['CLAIMBUSTER_API_KEY']
api_endpoint_claimbuster = "https://idir.uta.edu/claimbuster/api/v2/score/text/"

s3 = boto3.client('s3')
obj = s3.get_object(Bucket=bucket, Key='topics.txt')
topics = obj['Body'].read().decode("utf-8").split("\n")

# Get latest file name which is also the timestamp
files = s3.list_objects_v2(Bucket=bucket, Prefix=folder)
latest_file = max(files['Contents'], key=lambda x: x['LastModified'])
time_stamp = latest_file['Key'].replace(folder, "").rstrip(".json")


#########################################
# NEO4j Functions
#########################################
def insert_neo4j_with_cypher(tx, tweet, topic):
    query = (
        "MERGE (t:Tweet {id: $id}) "
        "SET "
        "t.date = $tweetDate,"
        "t.timeStamp = datetime(REPLACE($timeStamp, ' ', 'T')),"
        "t.inReplyToUser = $inReplyToUser,"
        "t.replyCount = toInteger($replyCount),"
        "t.followersCount = toInteger($followersCount),"
        "t.content = $content,"
        "t.retweetCount = toInteger($retweetCount),"
        "t.username = $username,"
        "t.positive = toFloat($positive),"
        "t.negative = toFloat($negative),"
        "t.neutral = toFloat($neutral),"
        "t.mixed = toFloat($mixed),"
        "t.topic = $topic,"
        "t.claimScore = toFloat($claimScore) "
        "FOREACH (mentionedUser IN CASE WHEN $mentionedUsers IS NULL THEN [] ELSE SPLIT($mentionedUsers, ',') END | "
        "MERGE (u:User_Twitter {username: mentionedUser}) "
        "MERGE (t)-[:MENTIONS]->(u))"
    )
    tx.run(query, id=tweet["id"], tweetDate=tweet["date"], timeStamp=tweet["timeStamp"], inReplyToUser=tweet["inReplyToUser"], replyCount=tweet["replyCount"], followersCount=tweet["followersCount"], content=tweet["content"],
           retweetCount=tweet["retweetCount"], username=tweet["username"], positive=tweet["Positive"], negative=tweet["Negative"], neutral=tweet["Neutral"], mixed=tweet["Mixed"], mentionedUsers=tweet["mentionedUsers"], claimScore=tweet["claimScore"], topic=topic)


def create_orchestrator(uri, user, password, tweets, topic):
    data_base_connection = GraphDatabase.driver(uri=uri, auth=(user, password))
    with data_base_connection.session(database="neo4j") as session:
        # Write transactions allow the driver to handle retries and transient errors
        for tweet in tweets:
            session.execute_write(insert_neo4j_with_cypher, tweet, topic)


#########################################
# AWS Comprehend Function to get sentiment score
#########################################
def get_sentiment(text_list):
    # Initialize an empty list for storing sentiment results
    sentiments = []
    # Initialize an Amazon Comprehend client object with your region name (replace with your own region)
    comprehend = boto3.client(
        service_name='comprehend', region_name='us-east-1')
    # Split the text list into batches of 25 documents each (the maximum number of documents per request for Amazon Comprehend)
    batches = [text_list[i:i+25] for i in range(0, len(text_list), 25)]
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
    api_response = requests.get(
        url=api_endpoint_claimbuster+input_claim, headers={"x-api-key": api_key_claimbuster})
    data = api_response.json()
    if data["results"]:
        return data["results"][0]["score"]
    return 0


for query in topics:
    #########################################
    # EXTRACT (READ DATA)
    #########################################
    dynamic_frame_read = glue_context.create_dynamic_frame.from_catalog(
        database=glue_db,
        table_name=glue_tbl,
        push_down_predicate=f"topic=='{query}' and dataload == '{crawl_day}'"
    )

    # Convert dynamic frame to data frame to use standard pyspark functions
    data_frame = dynamic_frame_read.toDF().toPandas()

    # Extract out tweets of that timestamp
    # data_frame = data_frame[data_frame['timeStamp'] == time_stamp]
    data_frame = data_frame

    #########################################
    # TRANSFORM (MODIFY DATA)
    #########################################
    # Apply translate and replace the content in place
    data_frame["content"] = data_frame.content.apply(translator.translate)

    # Apply AWS Comprehend and add sentiment score as a new column in the dataframe
    sentiments = get_sentiment(data_frame.content.to_list())
    for key in sentiments[0].keys():
        data_frame[key] = [x[key] for x in sentiments]

    # Call claimbuster to get claim score and add it as a new column in the dataframe
    data_frame["claimScore"] = data_frame.content.apply(invoke_claimbuster_api)

    #########################################
    # LOAD (WRITE DATA)
    #########################################
    # Insert into neo4j
    create_orchestrator(uri, user, password,
                        data_frame.to_dict('records'), query)

job.commit()
