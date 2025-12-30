import json
import os
import boto3
import urllib.parse

sqs = boto3.client('sqs')
QUEUE_URL = os.environ.get('QUEUE_URL')

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event)}")
    
    for record in event.get('Records', []):
        try:
            s3_data = record['s3']
            bucket_name = s3_data['bucket']['name']
            key_raw = s3_data['object']['key']
            key = urllib.parse.unquote_plus(key_raw)
            etag = s3_data['object'].get('eTag', '')

            # Validate file extension
            if not key.lower().endswith(('.jpg', '.jpeg', '.png')):
                print(f"Skipping non-image file: {key}")
                continue
            
            # Prepare message body
            message_body = {
                "bucket": bucket_name,
                "key": key,
                "etag": etag
            }
            
            # Send to SQS
            response = sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(message_body)
            )
            print(f"Sent message to SQS: {response['MessageId']} for {key}")

        except Exception as e:
            print(f"Error processing record: {e}")
            # Depending on requirements, we might want to raise to retry or capture in DLQ
            # For this simple lab, logging is sufficient.
            continue
            
    return {"statusCode": 200, "body": json.dumps("Processing complete")}
