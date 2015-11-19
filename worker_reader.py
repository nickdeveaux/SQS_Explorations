import boto3

# Get the service resource
sqs = boto3.resource('sqs')

# Get the queue
worker_queue = sqs.get_queue_by_name(QueueName='worker_input')

for message in worker_queue.receive_messages(MessageAttributeNames=['Url']):
    # TODO replace printing with worker code
    if message.message_attributes is not None:
        url = message.message_attributes.get('Url').get('StringValue')
        print 'Worker Message received: {0}: {1}'.format(message.body, url)


# Get the service resource.
dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('Worker_Output')

print(table.creation_date_time)

table.put_item(
   Item={
        'username': 'janedoe',
        'first_name': 'Jane',
        'last_name': 'Doe',
        'age': 25,
        'url': 'http://facebook.com',
        'account_type': 'standard_user',
    }
)

response = table.get_item(
    Key={
    	'url': 'http://facebook.com'
    }
)
item = response['Item']
print(item)

"""table.delete_item(
    Key={
    	'url': 'http://facebook.com'
    }
)"""

