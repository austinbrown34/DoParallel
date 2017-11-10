import requests
import boto3
import os


class Worker(object):

    def __init__(self, payload):
        self.payload = payload
        self.endpoint = payload['endpoint']
        self.params = payload['params']
        self.total = payload['total']
        self.id = payload['id']
        self.job_id = payload['job_id']


    def do_task(self):
        print('worker: do task')
        response = requests.post(
            self.endpoint,
            json=self.params
        )
        self.report_task(response)
        self.check_job_status()

        return {"status": "Success", "msg": "Task Executed"}


    def report_task(self, response):
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=self.job_id)
        queue.send_message(MessageBody=response, MessageAttributes={
            'Info': {
                'task_id': str(self.id),
                'status': 'Complete'
            }
        })


    def check_job_status(self):
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=self.job_id)
        completed_tasks = int(queue.attributes.get('ApproximateNumberOfMessages'))
        if completed_tasks == self.total:
            self.notify_manager()


    def notify_manager(self):
        url = os.environ.get('FINISH_JOB_URL', None)
        if url is None:
            return {"status": "Error", "msg": "FINISH_JOB_URL not set."}
        data = {
            'job_id': self.job_id
        }
        requests.post(
            url,
            json=data,
        )
