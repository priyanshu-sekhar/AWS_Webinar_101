from utils import SNSCommonUtils


def alert(event, context):
    SNSCommonUtils.publish_to_topic(
        topic_arn='arn:aws:sns:us-west-2:553658736773:neighborhood_1',
        message='A new Member in you neighborhood. Say Hi',
        subject='Say Hi to your new neighbor'
    )
