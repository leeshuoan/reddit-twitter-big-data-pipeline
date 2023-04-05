import requests
from dotenv import load_dotenv
import os

load_dotenv()

api_key = os.environ.get("CLAIM_BUSTER_API_KEY") 
input_claim = "Russia is losing the Ukraine war"

# Define the endpoint (url) with the claim formatted as part of it, api-key (api-key is sent as an extra header)
api_endpoint = f"https://idir.uta.edu/claimbuster/api/v2/score/text/{input_claim}"
# api_endpoint = f"https://idir.uta.edu/claimbuster/api/v2/query/knowledge_bases/{input_claim}"
# api_endpoint = f"https://idir.uta.edu/claimbuster/api/v2/query/fact_matcher/{input_claim}"
request_headers = {"x-api-key": api_key}

# Send the GET request to the API and store the api response
api_response = requests.get(url=api_endpoint, headers=request_headers)

# Print out the JSON payload the API sent back
print(api_response.json())