import boto3
import logging
import time
from botocore.errorfactory import ClientError
from collections import defaultdict
from utils import DateTimeUtils
from utils.Constants import PATH_SEPARATOR, SMALL_YEAR_TO_DATE_SLASH_FORMAT

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def get_s3_resource(role_arn=None):
    """
    Returns S3 resource with the correct access token and key depending on the role arn
    :param role_arn: Role under which to run the S3 commands
    :return: required S3 resource
    """

    if role_arn:
        client = boto3.client('sts')
        sts_response = client.assume_role(RoleArn=role_arn, RoleSessionName='AssumeExternalRole',
                                          DurationSeconds=900)
        s3 = boto3.resource(service_name='s3', aws_access_key_id=sts_response['Credentials']['AccessKeyId'],
                            aws_secret_access_key=sts_response['Credentials']['SecretAccessKey'],
                            aws_session_token=sts_response['Credentials']['SessionToken'])
    else:
        s3 = boto3.resource('s3')
    return s3


def read_s3_file(bucket_name, file_key, role_arn=None):
    """
    reads data from file_key within bucket_name
    :param bucket_name: encapsulating s3 bucket
    :param file_key: key of the file to read content from
    :param role_arn: Role under which to run the S3 commands
    :return:
    """

    if not bucket_name or not file_key:
        raise Exception('Bucket name or file key is empty while trying to read file', bucket_name, file_key)

    try:
        logger.info('Reading file %s in bucket %s', file_key, bucket_name)
        s3_resource = get_s3_resource(role_arn)
        file_obj = s3_resource.Object(bucket_name, file_key)
        content = file_obj.get()['Body'].read().decode('utf-8')
        return content
    except Exception as e:
        logger.error('Exception occurred while reading file %s in bucket %s %s', file_key, bucket_name, e.message)
        raise e


def write_to_file(bucket_name, file_key, content, file_name=None, role_arn=None):
    """
    If file_name specified, then copies content of file to the path s3://<bucket_name>/<file_key>
    Else writes content to an object with key file_key in bucket specified by bucket_name
    :param bucket_name: bucket where file content will be uploaded
    :param file_key: name of the file to be created with bucket relative path prefix
    :param content: content to be copied into file
    :param file_name: name of the local file to extract content from
    :param role_arn: Role under which to run the S3 commands
    :return:
    """

    if not bucket_name or not file_key:
        raise Exception('Bucket name or file key is empty while trying to write file', bucket_name, file_key)

    logger.info('Writing file %s in bucket %s', file_key, bucket_name)
    try:
        s3 = get_s3_resource(role_arn)
        if not file_name:
            s3.Object(bucket_name, file_key).put(Body=content)
        else:
            with open(file_name, 'rb') as data:
                s3.Object(bucket_name, file_key).put(Body=data)
        logger.info('Write successful')
    except Exception as e:
        logger.error('Exception occurred while writing to S3 object %s in bucket %s %s', file_key, bucket_name,
                     e.message)
        raise e


def get_object_uri_for_past_date(bucket_name, folder_key, offset_from_curr_date=0):
    """
    Gets the s3 path for the folder_key with past date prefixed
    :param bucket_name: encapsulating s3 bucket
    :param folder_key: folder key for generating path
    :param offset_from_curr_date: offset from current date
    :return:  Returns absolute object uri with (current date - offset) prefixed before folder_key
    """

    if not bucket_name:
        raise Exception('Bucket name is empty while generating object uri')

    return 's3://' + bucket_name + PATH_SEPARATOR + \
           get_object_key_for_past_date(
               folder_key=folder_key,
               offset_from_curr_date=offset_from_curr_date
           )


def get_object_key_for_past_date(folder_key, file_name=None, offset_from_curr_date=0):
    """
    Gets object key for file_name with past date prefixed
    :param folder_key: folder key for generating the path
    :param file_name: file name for generating the path, if file name is blank then returns till folder level
    :param offset_from_curr_date: offset from current date
    :return: Returns object key (without bucket) with (current date - offset) in the path
    """

    if not folder_key:
        raise Exception('folder name is empty while trying to generate key')

    formatted_date = DateTimeUtils.fetch_past_date_string(offset_from_curr_date=offset_from_curr_date,
                                                          str_format=SMALL_YEAR_TO_DATE_SLASH_FORMAT)
    if file_name:
        return folder_key + formatted_date + PATH_SEPARATOR + file_name
    else:
        return folder_key + formatted_date + PATH_SEPARATOR


def get_object_key_for_date(folder_key, file_name=None):
    """
    Gets object key for folder_key and filename optionally with date prefixed
    :param folder_key: folder key for generating the path
    :param file_name: file name for generating the path, if file name is blank then returns till folder level
    :return: Returns object key (without bucket) with date in the path
    """
    if not folder_key:
        raise Exception('date or folder name is empty while trying to generate key')

    key = folder_key + PATH_SEPARATOR
    if file_name:
        key += file_name

    return key