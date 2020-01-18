import boto3

client = boto3.client('sns')


def create_topic(topic_name):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.create_topic
    client.create_topic(
        Name='string',
        Attributes={
            'string': 'string'
        },
        Tags=[
            {
                'Key': 'string',
                'Value': 'string'
            },
        ]
    )
    :param topic_name:
    :return:
    """
    return client.create_topic(
        Name=topic_name
    )


def subscribe_email_to_topic(topic_name, email_address):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.subscribe
    response = client.subscribe(
        TopicArn='string',
        Protocol='string',
        Endpoint='string',
        Attributes={
            'string': 'string'
        },
        ReturnSubscriptionArn=True|False
    )
    :param email_address: user email_id, which needs to be subscribed
    :param topic_name: topic to subscribe to
    :return:
    """
    response = client.subscribe(
        TopicArn=topic_name,
        Protocol='email',
        Endpoint=email_address,
        ReturnSubscriptionArn=True
    )

    return response['SubscriptionArn']


def publish_to_topic(topic_arn, message, subject):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
    response = client.publish(
        TopicArn='string',
        TargetArn='string',
        PhoneNumber='string',
        Message='string',
        Subject='string',
        MessageStructure='string',
        MessageAttributes={
            'string': {
                'DataType': 'string',
                'StringValue': 'string',
                'BinaryValue': b'bytes'
            }
        }
    )
    :return:
    """
    client.publish(
        TopicArn=topic_arn,
        Message=message,
        Subject=subject
    )
