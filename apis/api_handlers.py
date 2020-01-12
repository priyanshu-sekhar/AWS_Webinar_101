from utils import DDBCommonUtils, DateTimeUtils, SNSCommonUtils


def insert_handler(event, context):
    if event['item'] == 'neighborhood':
        add_neighborhood(event['neighborhood'])
    elif event['item'] == 'person':
        add_person(event['person'])


def add_person(person):
    person['creation_day'] = DateTimeUtils.get_epoch_in_millis_for_day(0)

    DDBCommonUtils.insert_record(
        table_name='user',
        item_data=person
    )


def add_neighborhood(neighborhood):
    resp = SNSCommonUtils.create_topic('neighborhood_' + neighborhood['id'])
    neighborhood['subscription_arn'] = resp['TopicArn']

    DDBCommonUtils.insert_record(
        table_name='neighborhood',
        item_data=neighborhood
    )


