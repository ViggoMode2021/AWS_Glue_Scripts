import pandas as pd
import json
import boto3
import csv
import requests

def lambda_handler(event, context):
    key = 'run-AmazonS3_node1668214528527-1-part-r-00000'
    bucket = 'pii-detector-test-vig'
    s3_resource = boto3.resource('s3')
    s3_object = s3_resource.Object(bucket, key)
    
    data = s3_object.get()['Body'].read().decode('utf-8').splitlines()
    
    lines = csv.reader(data)
    headers = next(lines)
    for line in lines:
        #print complete line
        line = str(line)
        client = boto3.client('translate')
        response = client.translate_text(
        Text=line,
        SourceLanguageCode='en',
        TargetLanguageCode='es',
    )
        write_data = response.get('TranslatedText')
        write_data_list = list([write_data])
        for item in write_data_list:
            with open('/tmp/cleandata.csv','w') as file:
                 wr = csv.writer(file)
                 wr.writerows(write_data_list)
        key_2 = 'cleandata.csv'
        
        s3 = boto3.client('s3')
        s3.upload_file(
                    Bucket='pii-detector-test-vig',
                    Filename='/tmp/cleandata.csv',
                    Key='/tmp/cleandata.csv'
                )
