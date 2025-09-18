import boto3
import requests
from requests_aws4auth import AWS4Auth
import base64
import urllib.parse
import json

region = 'us-east-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
host = 'https://search-kclite-public-alh7skio4hadvkur54ycdv2ily.us-east-1.es.amazonaws.com'

index = 'mygoogle'
url = host + '/' + index + '/_search'

def get_from_Search(query):
    headers = { "Content-Type": "application/json" }
    r = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(query))
    response = {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": '*'
        },
        "isBase64Encoded": False
    }
    response['body'] = r.text
    body=r.text
    response_json=json.dumps(body)
    return body

def lambda_handler(event, context):
    try:
        print("Event is", event)
        response = {
            "statusCode": 200, 
            "statusDescription": "200 OK", 
            "isBase64Encoded": False,
            "headers": { "Content-Type": "application/json" }
        }
        
        # Handle both base64 encoded and plain JSON bodies
        if event.get('isBase64Encoded', False):
            # Base64 encoded body (old API Gateway format)
            encBodyData = event['body']
            bodyData = base64.b64decode(encBodyData)
            encFormData = bodyData.decode('utf-8')
            formDict = urllib.parse.parse_qs(encFormData)
            term = formDict.get('searchTerm')
            search_term = term[0] if term else ""
        else:
            # Plain JSON body (API Gateway v2.0 format)
            body_str = event['body']
            body_json = json.loads(body_str)
            
            # Handle different query formats
            if 'query' in body_json:
                if isinstance(body_json['query'], str):
                    search_term = body_json['query']
                elif isinstance(body_json['query'], dict):
                    # Handle complex queries like {"match_all": {}}
                    if 'match_all' in body_json['query']:
                        search_term = "*"  # Search all
                    else:
                        search_term = str(body_json['query'])
                else:
                    search_term = str(body_json['query'])
            elif 'searchTerm' in body_json:
                search_term = body_json['searchTerm']
            else:
                search_term = "*"  # Default to search all
        
        print("Search term:", search_term)
        
        # Build the query
        if search_term == "*":
            query = {
                "size": 25,
                "query": {
                    "match_all": {}
                },
                "fields": ["Title", "Author", "Date", "Summary"]
            }
        else:
            query = {
                "size": 25,
                "query": {
                    "multi_match": {
                        "query": search_term,
                        "fields": ["Title", "Author", "Body"]
                    }
                },
                "fields": ["Title", "Author", "Date", "Summary"]
            }
        
        print("Sending query to Opensearch")
        search_response = get_from_Search(query)
        response_json = json.loads(search_response)
        print("Response JSON is ", json.dumps(response_json))
        
        # Extract results
        if response_json.get("hits", {}).get("hits"):
            hits = response_json["hits"]["hits"]
            final_response = {
                "statusCode": 200,
                "body": json.dumps({
                    "status": True,
                    "results": hits,
                    "total": response_json["hits"]["total"]["value"] if "total" in response_json["hits"] else len(hits)
                })
            }
        else:
            final_response = {
                "statusCode": 200,
                "body": json.dumps({
                    "status": True,
                    "results": [],
                    "message": "No results found"
                })
            }
        
        return final_response
        
    except Exception as e:
        print("Exception is", str(e))
        respData = {
            'status': False,
            'message': str(e)
        }
        error_response = {
            'statusCode': 500,
            'body': json.dumps(respData)
        }
        return error_response
