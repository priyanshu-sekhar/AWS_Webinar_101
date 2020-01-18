import boto3
import decimal
import json
import logging
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def get_ddb_resource(role_arn=None):
    """

    :param role_arn: role arn for which to get ddb resource
    :return: ddb resource under correct role
    """
    if role_arn:
        client = boto3.client('sts')
        sts_response = client.assume_role(RoleArn=role_arn, RoleSessionName='AssumeExternalRole',
                                          DurationSeconds=900)
        dynamo_db = boto3.resource(service_name='dynamodb',
                                   aws_access_key_id=sts_response['Credentials']['AccessKeyId'],
                                   aws_secret_access_key=sts_response['Credentials']['SecretAccessKey'],
                                   aws_session_token=sts_response['Credentials']['SessionToken'])
    else:
        dynamo_db = boto3.resource(service_name='dynamodb')
    return dynamo_db


def add_item_to_list_after_conversion(set_of_items, item):
    """

    :param set_of_items: list to add to
    :param item: item to be added after converting from Dynamo DB type
    """
    set_of_items.append(json.loads(json.dumps(item, cls=DecimalEncoder)))


def scan(table_name):
    """
    basic dynamodb scan operation
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.scan
    :return:
    """
    set_of_items = []

    try:
        ddb_resource = get_ddb_resource()
        table = ddb_resource.Table(table_name)

        response = table.scan()

        for item in response['Items']:
            add_item_to_list_after_conversion(set_of_items, item)

        while 'LastEvaluatedKey' in response:
            response = table.scan(
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            for item in response['Items']:
                add_item_to_list_after_conversion(set_of_items=set_of_items, item=item)

        logger.info('Query successful and all data read')

    except Exception as e:
        logger.error('Exception raised while querying %s', e.message)
        raise e

    return set_of_items


def query_with_args(table_name, partition_key_name, partition_key_value, index_name=None, projection=None, role_arn=None):
    """

    :param table_name: Name of the table to run query on
    :param index_name: Name of the index to run query on
    :param partition_key_name: partition key to query
    :param partition_key_value: partition key value to query
    :param projection: projection of columns to return
    :param role_arn: role arn under which to run the query
    :return: an array of items, where each item is a dictionary representing the row in a DDB table
    """
    logger.info('Querying table %s with index %s for key %s having value %s with role %s', table_name, index_name,
                partition_key_name, partition_key_value, role_arn)
    set_of_items = []

    try:
        ddb_resource = get_ddb_resource(role_arn=role_arn)
        table = ddb_resource.Table(table_name)

        if index_name:
            if projection:
                response = table.query(
                    IndexName=index_name,
                    KeyConditionExpression=Key(partition_key_name).eq(partition_key_value),
                    ProjectionExpression=projection
                )
            else:
                response = table.query(
                    IndexName=index_name,
                    KeyConditionExpression=Key(partition_key_name).eq(partition_key_value)
                )
        else:
            response = table.query(
                KeyConditionExpression=Key(partition_key_name).eq(partition_key_value)
            )

        for item in response['Items']:
            add_item_to_list_after_conversion(set_of_items, item)

        while 'LastEvaluatedKey' in response:
            response = table.query(
                IndexName=index_name,
                KeyConditionExpression=Key(partition_key_name).eq(partition_key_value),
                ProjectionExpression=projection,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            for item in response['Items']:
                add_item_to_list_after_conversion(set_of_items=set_of_items, item=item)

        logger.info('Query successful and all data read')

    except Exception as e:
        logger.error('Exception raised while querying %s', e.message)
        raise e

    return set_of_items


def insert_record(table_name, item_data, role_arn=None):
    """
    Response_Syntax = table.put_item(
        Item={
            'string': 'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{}
        },
        Expected={
            'string': {
                'Value': 'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{},
                'Exists': True|False,
                'ComparisonOperator': 'EQ'|'NE'|'IN'|'LE'|'LT'|'GE'|'GT'|'BETWEEN'|'NOT_NULL'|'NULL'|'CONTAINS'|'NOT_CONTAINS'|'BEGINS_WITH',
                'AttributeValueList': [
                    'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{},
                ]
            }
        },
        ReturnValues='NONE'|'ALL_OLD'|'UPDATED_OLD'|'ALL_NEW'|'UPDATED_NEW',
        ReturnConsumedCapacity='INDEXES'|'TOTAL'|'NONE',
        ReturnItemCollectionMetrics='SIZE'|'NONE',
        ConditionalOperator='AND'|'OR',
        ConditionExpression=Attr('myattribute').eq('myvalue'),
        ExpressionAttributeNames={
            'string': 'string'
        },
        ExpressionAttributeValues={
            'string': 'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{}
        }
    )
    :param table_name: the ddb table to insert record into
    :param item_data: the item to be inserted
    :param role_arn: role arn under which to run the query
    :return:
    """
    try:
        ddb_resource = get_ddb_resource(role_arn)
        table = ddb_resource.Table(table_name)

        table.put_item(
            Item=item_data
        )
    except Exception as e:
        logger.error('Exception raised while updating %s', e.message)
        raise e


def update_record(table_name, update_expression, expression_attr_values, pk_name, pk_value, sort_key_name=None, sort_key_value=None, role_arn=None):
    """
    Request Syntax: table.update_item(
        Key={
            'year': year,
            'title': title
        },
        UpdateExpression="set info.rating = :r, info.plot=:p, info.actors=:a",
        ExpressionAttributeValues={
            ':r': decimal.Decimal(5.5),
            ':p': "Everything happens all at once.",
            ':a': ["Larry", "Moe", "Curly"]
        },
        ReturnValues="UPDATED_NEW"
    )
    :return:
    """

    try:
        ddb_resource = get_ddb_resource(role_arn)
        table = ddb_resource.Table(table_name)

        key = dict()
        key[pk_name] = pk_value
        if sort_key_name:
            key[sort_key_name] = sort_key_value

        table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attr_values
        )
    except Exception as e:
        logger.error('Exception raised while updating %s', e.message)
        raise e


def batch_write(table_name, data, data_schema):
    """
    the request schema should be based on the following schema as per docs
    https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html#API_BatchWriteItem_RequestSyntax

    dynamodb.batch_write_item(
    RequestItems={
        'string': [
            {
                'PutRequest': {
                    'Item': {
                        'string': 'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{}
                    }
                },
                'DeleteRequest': {
                    'Key': {
                        'string': 'string'|123|Binary(b'bytes')|True|None|set(['string'])|set([123])|set([Binary(b'bytes')])|[]|{}
                    }
                }
            },
        ]
    },

    :param table_name: DDB table to create request for
    :param data: data to be batch uploaded
    :param data_schema: schema of the data to be uploaded

    Sample data -
    ['SG1', 'SG2', '625']
    with data_schema -
    ['id1', 'id2', 'status']

    currently supporting only String
    TODO add support for multiple data formats and DeleteRequest as well
    :return:
    """

    items = []
    for each in data:
        record = dict()

        for i, val in enumerate(each):
            if val:
                record[data_schema[i].upper()] = {
                    "S": val
                }

        items.append({
            'PutRequest': {
                'Item': record
            }
        })

    logger.info('batch items to writel', items)
    client = boto3.client('dynamodb')

    client.batch_write_item(
        RequestItems=dict({table_name: items}),
        ReturnConsumedCapacity='TOTAL',
        ReturnItemCollectionMetrics='SIZE'
    )


