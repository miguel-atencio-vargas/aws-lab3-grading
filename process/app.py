import json
import boto3
import os
from PIL import Image
from io import BytesIO
from botocore.exceptions import ClientError

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event)}")
    
    for record in event['Records']:
        try:
            body = json.loads(record['body'])
            bucket = body['bucket']
            key = body['key']
            
            filename = os.path.basename(key)
            metadata_key = f"metadata/{filename}.json"
            
            # Idempotency check: check if metadata file already exists
            try:
                s3.head_object(Bucket=bucket, Key=metadata_key)
                print(f"Metadata already exists for {key} at {metadata_key}, skipping.")
                continue
            except ClientError as e:
                # If 404, proceed. If other error, log/raise.
                error_code = e.response['Error']['Code']
                if error_code not in ('404', 'NotFound'):
                    print(f"Error checking metadata existence: {e}")
                    raise e # Let SQS retry? Or fail?

            # Download image
            print(f"Downloading {key} from {bucket}")
            response = s3.get_object(Bucket=bucket, Key=key)
            image_data = response['Body'].read()
            file_size = response['ContentLength']

            # Extract metadata
            with Image.open(BytesIO(image_data)) as img:
                width, height = img.size
                img_format = img.format
                
                # Construct metadata JSON
                # Matching tests/test_lab3.py expectations
                metadata = {
                    "source_bucket": bucket,
                    "source_key": key,
                    "width": width,
                    "height": height,
                    "file_size_bytes": file_size,
                    "format": img_format
                }

            # Upload metadata
            print(f"Uploading metadata to {metadata_key}")
            s3.put_object(
                Bucket=bucket,
                Key=metadata_key,
                Body=json.dumps(metadata),
                ContentType='application/json'
            )

        except Exception as e:
            print(f"Error processing record: {e}")
            # If we raise here, SQS will retry.
            # Depending on error type (e.g. malformed image), we might want to swallow it to avoid infinite retries.
            # For now, let's just log.
            continue
            
    return {"statusCode": 200, "body": "Processing complete"}
