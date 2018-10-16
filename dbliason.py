from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import datetime
import decimal
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError


def datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    return None
    # raise TypeError("Unknown type")


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class DatabaseLiason(object):

    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb')
        self.dynamodb_client = boto3.client('dynamodb')


    def create_jobs_table(self):
        table = self.dynamodb.create_table(
            TableName='Jobs',
            KeySchema=[
                {
                    'AttributeName': 'job_id',
                    'KeyType': 'HASH'  #Partition key
                },
                {
                    'AttributeName': 'job_status',
                    'KeyType': 'RANGE'  #Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'job_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'job_status',
                    'AttributeType': 'S'
                },

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        table.meta.client.get_waiter('table_exists').wait(TableName='Jobs')
        print("Table status:", table.table_status)


    def create_tasks_table(self):
        table = self.dynamodb.create_table(
            TableName='Tasks',
            KeySchema=[
                {
                    'AttributeName': 'job_id',
                    'KeyType': 'HASH'  #Partition key
                },
                {
                    'AttributeName': 'task_id',
                    'KeyType': 'RANGE'  #Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'job_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'task_id',
                    'AttributeType': 'S'
                },

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )

        table.meta.client.get_waiter('table_exists').wait(TableName='Tasks')
        print("Table status:", table.table_status)


    def add_item(self, item, tbl):
        table = self.dynamodb.Table(tbl)
        response = table.put_item(
            Item=item
        )

        print(json.dumps(response, indent=4, cls=DecimalEncoder, default=datetime_handler))


    def get_job_tasks(self, job_id, tbl):
        table = self.dynamodb.Table(tbl)
        response = table.scan(
            FilterExpression=Attr('job_id').eq(job_id)
        )
        items = response['Items']
        print(json.dumps(items, indent=4, cls=DecimalEncoder, default=datetime_handler))
        return items


    def get_item(self, key, tbl):
        table = self.dynamodb.Table(tbl)
        try:
            response = table.get_item(
                Key=key
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            item = response['Item']
            print(json.dumps(item, indent=4, cls=DecimalEncoder, default=datetime_handler))


    def update_item(self, key, expression, values, tbl):
        table = self.dynamodb.Table(tbl)

        response = table.update_item(
            Key=key,
            UpdateExpression=expression,
            ExpressionAttributeValues=values,
            ReturnValues='ALL_NEW'
        )


        print(json.dumps(response, indent=4, cls=DecimalEncoder, default=datetime_handler))



    def delete_item(self, key, tbl):
        table = self.dynamodb.Table(tbl)
        response = table.delete_item(
            Key=key
        )

        print(json.dumps(response, indent=4, cls=DecimalEncoder, default=datetime_handler))


    def table_exists(self, table_name):
        try:
            response = self.dynamodb_client.describe_table(TableName=table_name)
            print(json.dumps(response, indent=4, cls=DecimalEncoder, default=datetime_handler))
            return True
        except self.dynamodb_client.exceptions.ResourceNotFoundException:
            return False
