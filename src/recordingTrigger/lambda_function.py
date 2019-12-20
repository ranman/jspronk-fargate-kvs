import json
import os

import boto3

QUEUE_URL = os.getenv("QUEUE_URL")

def lambda_handler(event, context):
    sqs = boto3.resource('sqs').Queue(QUEUE_URL)
    payload = {
        'streamARN': event['Details']['ContactData']['MediaStreams']['Customer']['Audio']['StreamARN'],
        'startFragmentNum': event['Details']['ContactData']['MediaStreams']['Customer']['Audio']['StartFragmentNumber'],
        'connectContactId': event['Details']['ContactData']['ContactId'],
        'saveCallRecording': event['Details']['ContactData']['Attributes']['saveCallRecording'],
    }
    sqs.send_message(MessageBody=json.dumps(payload))
    return {
        'lambdaSuccess': 'Success'
    }
