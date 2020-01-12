from utils import CloudwatchCommonUtils, DDBCommonUtils, S3CommonUtils, Commons, SNSCommonUtils, CSVUtils
from utils.Constants import NEIGHBORHOOD_TABLE_NAME, SUBSCRIPTION_KEY_NAME, TO_SUBSCRIBE_OBJ_NAME

DATA_BUCKET_ARG = 'data_bucket'
INVOKE_SUBSCRIPTION_JOB_ARGS = [DATA_BUCKET_ARG]


def subscribe_user_to_neighborhood(user):
    """
    We need to find which locality the user lies in. There are excellent geospatial db-approaches/elasticsearch to solve this,
    but owing to limited scope of webinar, limiting the approach to simply get all neighborhood details and do sequential verification
    Step 1: Scan neighborhood ddb
    Step 2: For each item, check if user coordinates fall under a radius of 1000m
    :param user: the user to be subscribed
    :return:
    """

    neighborhoods = DDBCommonUtils.scan(NEIGHBORHOOD_TABLE_NAME)
    for each in neighborhoods:
        distance = Commons.get_distance_between_pair_of_coords(
            coord1={
                'lat': each['lat'],
                'lng': each['lng']
            },
            coord2={
                'lat': user['lat'],
                'lng': user['lng']
            }
        )
        print('distance of user from neighborhood', distance)

        if distance < 10000:
            SNSCommonUtils.subscribe_email_to_topic(
                topic_name=each['subscription_arn'],
                email_address=user['email_id']
            )

            CloudwatchCommonUtils.put_subscription_metric()


def invoke_subscription():
    """
    Step 1: Fetch 'to_subscribe' user data from S3
    Step 2: For each, subscribe the user to his respective neighborhood
    :return:
    """

    args = Commons.get_job_arguments(INVOKE_SUBSCRIPTION_JOB_ARGS)
    data_bucket = args.get(DATA_BUCKET_ARG)

    user_content = S3CommonUtils.read_s3_file(
        bucket_name=data_bucket,
        file_key=S3CommonUtils.get_object_key_for_date(
            folder_key=SUBSCRIPTION_KEY_NAME,
            file_name=TO_SUBSCRIBE_OBJ_NAME
        )
    )

    for user in CSVUtils.create_item_list_from_csv(user_content):
        subscribe_user_to_neighborhood(user)


invoke_subscription()
