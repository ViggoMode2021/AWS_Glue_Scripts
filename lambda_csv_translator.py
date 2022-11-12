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
    print(data)
    headers = next(lines)
    for line in lines:
        #print complete line
        line_2 = str(line[0])
        client = boto3.client('translate')
        translation = client.translate_text(
        Text=str(data),
        SourceLanguageCode='en',
        TargetLanguageCode='es',
    )
        tosend=(translation.get('TranslatedText'))
        f = open('/tmp/cleandata.csv','w')
        f.write(tosend) #Give your csv text here.
        ## Python will convert \n to os.linesep
        f.close()
        s3 = boto3.client('s3')
        s3.upload_file(
                        Bucket='pii-detector-test-vig',
                        Filename='/tmp/cleandata.csv',
                        Key='/tmp/cleandata.csv'
                    )
