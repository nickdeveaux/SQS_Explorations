import boto3

def write_to_db(url):
    # Get the service resource.
    dynamodb = boto3.resource('dynamodb')
    
    table = dynamodb.Table('Worker_Output')
    
    table.put_item(
       Item={
            'username': 'janedoe',
            'first_name': 'Jane',
            'last_name': 'Doe',
            'age': 25,
            'url': url,
            'account_type': 'standard_user',
        }
    )
    
    response = table.get_item(
        Key={
            'url': url
        }
    )
    item = response['Item']
    print(item)

def main():
    
    # Get the service resource
    sqs = boto3.resource('sqs')
    
    # Get the queue
    worker_queue = sqs.get_queue_by_name(QueueName='worker_input')
    
    for message in worker_queue.receive_messages(MessageAttributeNames=['Url']):
        # TODO replace this with worker code
        if message.message_attributes is not None:
            url = message.message_attributes.get('Url').get('StringValue')
            print 'Worker Message received: {0}: {1}'.format(message.body, url)
            write_to_db(url)

if __name__ == '__main__':
    main()

