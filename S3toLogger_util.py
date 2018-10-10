# This is Lambda function written in Python to read files from S3 and load them to logstash client for Kiabana display
# This is reusable funtion 

import boto3
import botocore
import json
import csv
import requests
import sys
import pprint
import requests
import os

# Main function 

def handler(event, context):

    # initialize all s3 items
    s3 = boto3.resource('s3')
    s3_client = boto3.client('s3')
    s3_bucket = os.environ['S3_BUCKET']
    print("S3 B Name : "+s3_bucket)
    bucket = s3.Bucket(s3_bucket)
   

    # Init logstash client
    endpoint = "http://logstash-xxx.com:8089"
   
  
    # Load S3 and retrieve the object with particular name 
    try:
        s3.meta.client.head_bucket(Bucket='Bucket_name')
        print("S3 bucket connected")
        for object in bucket.objects.filter(Prefix='Logdata'):

            # check whether or not the last 4 chars are '.csv' to eliminate bad files/dirs
            filetype = object.key[-4:]
            if filetype == ".csv":
                data_list = []
                print("S3 File Name "+object.key[9:])
                	
                # download the file to a temp .csv, and parse it's contents
                s3.Bucket(object.bucket_name).download_file(object.key, '/tmp/data.csv')
                with open('/tmp/data.csv', 'rb') as csvfile:

                    rowsreader = csv.reader(csvfile, delimiter=',', quotechar='|')
                    count = 0
                    for row in rowsreader:
                        if count != 0:
                            data = LoggerData()
                            data.duplicateCount = row[0]
                            data.totalDuplicate = row[1]
                            data.cashedCount = row[2]
                            data.totalCashed = row[3]
                            data.notFoundCount = row[4]
                            data.totalNotFound = row[5]
                            data.underCount = row[6]
                            data.totalUnder = row[7]
                            data.mismatchCount = row[8]
                            data.totalMismatch = row[9]
                            data.overCount = row[10]
                            data.totalOver = row[11]
                            data_list.append(data)
                        count = count + 1
                        #pprint.pprint(row[11])

                # make each item a JSON object and write to log
                for data in data_list:
                    jsondata = create_json(data.duplicateCount, data.totalDuplicate, data.cashedCount, data.totalCashed, data.notFoundCount, data.totalNotFound, data.underCount, data.totalUnder, data.mismatchCount, data.totalMismatch, data.overCount, data.totalOver)
                    # sending post request and saving response as response object
                    logstash_api_call(jsondata, endpoint)
                    archive_s3file(s3_bucket)

    except botocore.exceptions.ClientError as e:
        print(format(e.response['Error']['Code']))


# class for each data object
class LoggerData:
    
    def __init__(self):
        self.duplicateCount = 0
        self.totalDuplicate = 0.0
        self.cashedCount = 0
        self.totalCashed = 0.0
        self.notFoundCount = 0
        self.totalNotFound = 0.0
        self.underCount = 0
        self.totalUnder = 0.0
        self.mismatchCount = 0
        self.totalMismatch = 0.0
        self.overCount = 0
        self.totalOver = 0.0

# creates JSON packet for write to LogStash
def create_json(duplicateCount, totalDuplicate, cashedCount, totalCashed, notFoundCount, totalNotFound, underCount, totalUnder, mismatchCount, totalMismatch, overCount, totalOver):
    
    pprint.pprint("Preparing JSON output")
    measurement = "LoggerData"
    json_body = [
        {
            "measurement": measurement,
            "tags": {
                "Chanel": "Logger Data",

            },
            "fields": {
                "duplicateCount": int(duplicateCount),
                "totalDuplicate": float(totalDuplicate),
                "cashedCount": int(cashedCount),
                "totalCashed": float(totalCashed),
                "notFoundCount": int(notFoundCount),
                "totalNotFound": float(totalNotFound),
                "underCount": int(underCount),
                "totalUnder": float(totalUnder),
                "mismatchCount": int(mismatchCount),
                "totalMismatch": float(totalMismatch),
                "overCount": int(overCount),
                "totalOver": float(totalOver)
            }
        }
    ]

    return json_body

# Define function for api call
def logstash_api_call(jsondata, endpoint):
    apidata = {'api_option':'paste',
               'api_paste_code':jsondata,
               'api_paste_format':'python'}
    r = requests.post(url = endpoint, data = apidata)
    # extracting response text
    pastebin_url = r.text
    print("Data writen to Logstash was :")
    pprint.pprint(jsondata)
    print("The pastebin URL is:%s"%pastebin_url)
    return "complete"

# Copy and delete all objects in the incoming directory
def archive_s3file(s3_bucket):
    
    try:
        s3.meta.client.head_bucket(Bucket=s3_bucket)
        print("S3 bucket for Archieve")
        for object in bucket.objects.filter(Prefix='Logdata'):

        # check whether or not the last 4 chars are '.csv' to eliminate bad files/dirs
        keyname = object.key[-4:]
        if keyname == ".csv":
            print("looking at: "+ object.key[9:])
            #copy key to 'archive', delete original key in 'incoming'
            new_key_name = "archived/" + object.key[9:]
            print("Copying file to new archive object...")
            s3_client.copy_object(
                Bucket=object.bucket_name,
                Key=new_key_name,
                CopySource={
                    'Bucket': object.bucket_name,
                    'Key': object.key
                },
                ServerSideEncryption='AES256'
            )

            print("Deleting original object...")
            original = bucket.Object(object.key)
            original.delete()

    except botocore.exceptions.ClientError as e:
        print(format(e.response['Error']['Code']))

    return "Complete"
