# AWS IMAGE RESIZE LAMBDA FUNCTION
import os
import boto3
from PIL import Image
from io import BytesIO
import json


s3 = boto3.client('s3')
sns = boto3.client('sns')


source_bucket_name = 'sjd-non-resized'
destination_bucket_name = 'sjd-resized'
sns_topic = 'arn:aws:sns:ap-south-1:587237375878:image-resize'

def lambda_handler(event, context):
    print("Received event: " + str(event))
    print("Received context: " + str(context))

    if 'Records' in event:
        for record in event['Records']:
            handle_s3_record(record)
    else:
        handle_s3_record(event)

    


def handle_s3_record(record):
    if 's3' in record and 'bucket' in record['s3'] and 'name' in record['s3']['bucket'] and 'object' in record['s3'] and 'key' in record['s3']['object']:
        source_bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        # Download the image from S3
        obj = s3.get_object(Bucket=source_bucket, Key=key)
        content_type = obj['ContentType']
        image_data = obj['Body'].read()

        # Resize the image
        resized_image = resize_image(image_data)

        #upload resized image to S3 destination bucket
        destination_key = f"resized/{key}"
        s3.put_object(Bucket=destination_bucket_name, Key=destination_key, Body=resized_image, ContentType=content_type)


        # Send SNS notification
        sns.publish(TopicArn=sns_topic, Message=f"Image {key} was resized successfully", Subject="Image Resized")

        print(f"Image {key} was resized successfully")
    else:
        print("Invalid S3 event record")

def resize_image(image_data, quality=70):
    image = Image.open(BytesIO(image_data))
    image_format = image.format if image.format else 'JPEG'  # default to JPEG if format is not available

    buffer = BytesIO()
    image.save(buffer, format=image_format, quality=quality)

    buffer.seek(0)
    return buffer.getvalue()
