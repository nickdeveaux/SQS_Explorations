import boto3
import ast

def parse_s3_object(bucket_name, object_name):
    s3 = boto3.resource('s3')
    destination = '/tmp/' + object_name
    s3.meta.client.download_file(bucket_name, object_name, destination)
    with open(destination, 'r') as dest:
        return dest.readlines()



# Get the service resource
sqs = boto3.resource('sqs')

# Get the queue
s3_queue = sqs.get_queue_by_name(QueueName='sentinel_input')
worker_queue = sqs.get_queue_by_name(QueueName='worker_input')

# Always be polling
while True:

    # Process messages by printing out body and optional author name
    for message in s3_queue.receive_messages():
        body = ast.literal_eval(message.body)
        if body != None and 'Records' in body:
            record = body['Records'][0]
            if 's3' in record:
                object_name = record['s3']['object']['key']
                bucket = record['s3']['bucket']
                lines = parse_s3_object(bucket['name'], object_name)
                for line in lines:
                    worker_queue.send_message(MessageBody='worker_input', MessageAttributes={
                            'Url': {
                                        'StringValue': line,
                                        'DataType': 'String'},
                            'Source': {
                                        'StringValue': object_name,
                                        'DataType': 'String'},
                            })
                # Let the queue know that the message is processed
                message.delete()
sleep(1)
