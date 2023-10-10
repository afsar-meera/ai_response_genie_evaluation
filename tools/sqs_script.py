import json
import boto3
from config import AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, SQS_URL


def send_to_sqs(MESSAGE, SQS_URL=SQS_URL, AWS_DEFAULT_REGION=AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY):
    sqs_client = boto3.client("sqs", region_name=AWS_DEFAULT_REGION,
                   aws_access_key_id=AWS_ACCESS_KEY_ID,
                   aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    message = MESSAGE
    response = sqs_client.send_message(
        QueueUrl= SQS_URL,
        MessageBody=json.dumps(message)
    )
    return response


def receive_from_sqs(SQS_URL, AWS_DEFAULT_REGION=AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY):
    sqs_client = boto3.client("sqs", region_name=AWS_DEFAULT_REGION,
                   aws_access_key_id=AWS_ACCESS_KEY_ID,
                   aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    response = sqs_client.receive_message(
        QueueUrl=SQS_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10,
    )

    print(f"Number of messages received: {len(response.get('Messages', []))}")

    for message in response.get("Messages", []):
        message_body = message["Body"]
        print(f"Message body: {json.loads(message_body)}")
        print(f"Receipt Handle: {message['ReceiptHandle']}")

if __name__ == "__main__":
    receive_from_sqs()
    send_to_sqs()