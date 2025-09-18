import boto3
import re
import requests
import math
from requests_aws4auth import AWS4Auth

region = 'us-east-1' # e.g. us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
print("Credentials:", credentials)
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
print("Credentials access key:", credentials.access_key)
print("Credentials secret key:", credentials.secret_key)

host = 'https://search-kclite-public-alh7skio4hadvkur54ycdv2ily.us-east-1.es.amazonaws.com'

index = 'mygoogle'
datatype = '_doc'
#url = host + '/' + index + '/' + datatype

headers = { "Content-Type": "application/json" }

s3 = boto3.client('s3')
author = ''
date = ''
def listToString(s):
    str1 = ""
    for ele in s:
        str1 += bytes.decode(ele)
    return str1
    
# Lambda execution starts here
def handler(event, context):
    for record in event['Records']:

        # Get the bucket name and key for the new file
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # Get, read, and split the file into lines
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        lines = body.splitlines()
        
        cust_id= key
        url = host + '/' + index + '/' + datatype + '/' + cust_id
        
        # Decode and process title
        title = lines[0].decode('utf-8') if lines[0] else "Untitled"
        print("Key:", key)
        
        # Handle author with proper decoding and fallback
        author_raw = lines[1].decode('utf-8') if len(lines) > 1 else "Unknown"
        author = "Unknown" if author_raw == "None" else author_raw
        
        # Handle date with proper decoding and fallback
        date_raw = lines[2].decode('utf-8') if len(lines) > 2 else "2023-01-01"
        date = "2023-01-01" if date_raw == "None" else date_raw
        
        #print("Lines is ", body.split())
        final_body=lines[3:]
        size= len(final_body)
        end_index = math.floor(size/10)
        print("Size: ", size)
        print("End index: ",end_index)
        summary=final_body[1:2]
        print('The binary pdf file type is', type(final_body))
        print("Title:" , title)
        print("Type of title", type(title))
        print("Author:" , author)
        print("Date:", date)
        #print("Body:", final_body)
        print("Summary",summary)
        print("Type of final_body:", type(final_body))
        print("Type of body in string: ",type(listToString(final_body)))
        
        # Create document with properly formatted fields
        document = { 
            "Title": title,
            "Author": author, 
            "Date": date, 
            "Body": listToString(final_body),
            "Summary": listToString(summary) if summary else ""
        }
        #print("Document:",document)
        r = requests.post(url, auth=awsauth, json=document, headers=headers)
        print("Response:", r.text)