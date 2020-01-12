import logging

from utils import DDBCommonUtils, S3CommonUtils, Commons, DateTimeUtils, CSVUtils
from utils.Constants import USER_TABLE_NAME, SUBSCRIPTION_KEY_NAME, TO_SUBSCRIBE_OBJ_NAME

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

DATE_OFFSET_ARG = 'date_offset'
DATA_BUCKET_ARG = 'data_bucket'
USER_DATA_EXTRACTION_JOB_ARGS = [DATE_OFFSET_ARG, DATA_BUCKET_ARG]


def extract_user_data():
    """
    This script extracts data from dynamo DB index based on date of extraction passed and saves it in specified data bucket
    """
    args = Commons.get_job_arguments(USER_DATA_EXTRACTION_JOB_ARGS)
    date_offset = int(args.get(DATE_OFFSET_ARG, 1))
    epoch_day = DateTimeUtils.get_epoch_in_millis_for_day(date_offset)
    data_bucket = args.get(DATA_BUCKET_ARG)

    try:
        # project the attributes that you need to extract
        data_projection = "email_id,lat,lng"
        extracted_data = DDBCommonUtils.query_with_args(
            table_name=USER_TABLE_NAME,
            partition_key_name='creation_day',
            partition_key_value=epoch_day,
            projection=data_projection
        )

        file_body = CSVUtils.create_csv_from_item_list(headers=data_projection, items=extracted_data)

        S3CommonUtils.write_to_file(
            bucket_name=data_bucket,
            file_key=S3CommonUtils.get_object_key_for_date(
                folder_key=SUBSCRIPTION_KEY_NAME,
                file_name=TO_SUBSCRIBE_OBJ_NAME
            ),
            content=file_body
        )

    except Exception as e:
        logger.error('Exception occurred while extracting data %s', e.message)
        raise e


extract_user_data()