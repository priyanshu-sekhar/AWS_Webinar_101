import boto3


# Create CloudWatch client
cloudwatch = boto3.client('cloudwatch')


def put_metric_data(metric, dimensions=None):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudwatch.html#CloudWatch.Client.put_metric_data
    response = client.put_metric_data(
        Namespace='string',
        MetricData=[
            {
                'MetricName': 'string',
                'Dimensions': [
                    {
                        'Name': 'string',
                        'Value': 'string'
                    },
                ],
                'Timestamp': datetime(2015, 1, 1),
                'Value': 123.0,
                'StatisticValues': {
                    'SampleCount': 123.0,
                    'Sum': 123.0,
                    'Minimum': 123.0,
                    'Maximum': 123.0
                },
                'Values': [
                    123.0,
                ],
                'Counts': [
                    123.0,
                ],
                'Unit': 'Seconds'|'Microseconds'|'Milliseconds'|'Bytes'|'Kilobytes'|'Megabytes'|'Gigabytes'|'Terabytes'|'Bits'|'Kilobits'|'Megabits'|'Gigabits'|'Terabits'|'Percent'|'Count'|'Bytes/Second'|'Kilobytes/Second'|'Megabytes/Second'|'Gigabytes/Second'|'Terabytes/Second'|'Bits/Second'|'Kilobits/Second'|'Megabits/Second'|'Gigabits/Second'|'Terabits/Second'|'Count/Second'|'None',
                'StorageResolution': 123
            },
        ]
    )
    :return:
    """
    metric_data = {
        'MetricName': metric,
        'Value': 1.0
    }
    if dimensions:
        metric_data['Dimensions'] = dimensions

    cloudwatch.put_metric_data(
        MetricData=[metric_data],
        Namespace='NEARBY_USER'
    )
