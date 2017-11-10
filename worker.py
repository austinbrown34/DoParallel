import requests
import boto3
import os
import json


class Worker(object):

    def __init__(self, payload):
        self.payload = payload
        self.endpoint = payload['endpoint']
        self.params = payload['params']
        self.total = payload['total']
        self.id = payload['id']
        self.job_id = payload['job_id']
        self.callback = payload['callback']


    def do_task(self):
        print('worker: do task')
        print(self.endpoint)
        print(self.params)
        response = requests.post(
            self.endpoint,
            json=json.dumps(self.params)
        )
        self.report_task(response.json())
        self.check_job_status()

        return {"status": "Success", "msg": "Task Executed"}


    def report_task(self, response):
        print('report task response:')
        print(response)
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=self.job_id)
        queue.send_message(MessageBody=json.dumps(response), MessageAttributes={
            'Status': {
                'StringValue': 'Complete',
                'DataType': 'String'
            },
            'Task_id': {
                'StringValue': str(self.id),
                'DataType': 'String'
            }
        })
        print('sent message to queue')


    def check_job_status(self):
        print('check queue')
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=self.job_id)
        completed_tasks = int(queue.attributes.get('ApproximateNumberOfMessages'))
        if completed_tasks == self.total:
            self.notify_manager()


    def notify_manager(self):
        print('notify manager')
        url = os.environ.get('FINISH_JOB_URL', None)
        if url is None:
            return {"status": "Error", "msg": "FINISH_JOB_URL not set."}
        data = {
            'job_id': self.job_id,
            'callback': self.callback
        }
        requests.post(
            url,
            json=data,
        )
