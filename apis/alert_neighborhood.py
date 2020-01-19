from utils import SNSCommonUtils


def alert_custom(event, context):
    record = event['Records'][0]
    if record['eventName'] == 'INSERT':
        name = record['dynamodb']['NewImage']['NAME']['S']
        SNSCommonUtils.publish_to_topic(
            topic_arn='<topic_arn>',
            message='A new member in you neighborhood. Say Hi to ' + name,
            subject='Say Hi to your new neighbor - ' + name
        )


def alert(event, context):
    SNSCommonUtils.publish_to_topic(
        topic_arn='<topic_arn>',
        message='A new member in you neighborhood. Say Hi',
        subject='Say Hi to your new neighbor'
    )

