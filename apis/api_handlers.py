from utils import Constants, DDBCommonUtils, DateTimeUtils, SNSCommonUtils
from utils.Constants import NEIGHBORHOOD_EVENT_TYPE, USER_EVENT_TYPE


def insert_handler(event, context):
    if event['item'] == NEIGHBORHOOD_EVENT_TYPE:
        add_neighborhood(event[NEIGHBORHOOD_EVENT_TYPE])
    elif event['item'] == USER_EVENT_TYPE:
        add_person(event[USER_EVENT_TYPE])


def add_person(person):
    person[Constants.USER_CREATION_DAY_ATTR] = DateTimeUtils.get_epoch_in_millis_for_day(0)
    person[Constants.USER_STATUS_ATTR] = Constants.NEARBY_STATUS_TO_SUBSCRIBE

    DDBCommonUtils.insert_record(
        table_name=Constants.USER_TABLE_NAME,
        item_data=person
    )


def add_neighborhood(neighborhood):
    resp = SNSCommonUtils.create_topic('neighborhood_' + neighborhood[Constants.NEIGHBORHOOD_ID_ATTR])
    neighborhood[Constants.NEIGHBORHOOD_SUBSCRIPTION_ARN_ATTR] = resp['TopicArn']

    DDBCommonUtils.insert_record(
        table_name=Constants.NEIGHBORHOOD_TABLE_NAME,
        item_data=neighborhood
    )


