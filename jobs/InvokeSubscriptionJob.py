from utils import CloudwatchCommonUtils, DDBCommonUtils, S3CommonUtils, Commons, SNSCommonUtils, CSVUtils, Constants
from utils.Constants import NEIGHBORHOOD_TABLE_NAME, USER_TABLE_NAME, SUBSCRIPTION_KEY_NAME, TO_SUBSCRIBE_OBJ_NAME

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
                'lat': each[Constants.NEIGHBORHOOD_LAT_ATTR],
                'lng': each[Constants.NEIGHBORHOOD_LNG_ATTR]
            },
            coord2={
                'lat': user[Constants.USER_LAT_ATTR],
                'lng': user[Constants.USER_LNG_ATTR]
            }
        )
        print('distance of user from neighborhood', distance)

        if distance < 10000:
            SNSCommonUtils.subscribe_email_to_topic(
                topic_name=each[Constants.NEIGHBORHOOD_SUBSCRIPTION_ARN_ATTR],
                email_address=user[Constants.USER_EMAIL_ATTR]
            )

            DDBCommonUtils.update_record(
                table_name=USER_TABLE_NAME,
                update_expression=Constants.UPDATE_STATUS_ATTR_EXPRESSION,
                expression_attr_values={
                    ':status': Constants.NEARBY_STATUS_SUBSCRIBED,
                    ':neighborhood_id': user[Constants.NEIGHBORHOOD_TABLE_PK]
                },
                pk_name=Constants.USER_TABLE_PK,
                pk_value=each[Constants.USER_TABLE_PK]
            )

            CloudwatchCommonUtils.put_metric_data(
                metric=Constants.ALERT_NEIGHBORHOOD_METRIC,
                dimensions=[{
                    'Name': 'neighborhood_' + each[Constants.NEIGHBORHOOD_TABLE_PK]
                }]
            )


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

        CloudwatchCommonUtils.put_metric_data(
            metric=Constants.SUBSCRIBED_METRIC_NAME
        )


invoke_subscription()
