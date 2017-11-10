import requests
import os
import uuid
import boto3


class Manager(object):

    def __init__(self, payload):
        self.payload = payload
        self.callback = self.payload['callback']


    def do_job(self):
        print('manager: do job!')
        self.tasks = self.payload['tasks']
        self.job_id = str(uuid.uuid4())
        sqs = boto3.resource('sqs')
        sqs.create_queue(QueueName=self.job_id)
        url = os.environ.get('DO_TASK_URL', None)

        if url is None:
            return {"status": "Error", "msg": "DO_TASK_URL not set."}

        for i, task in enumerate(self.tasks):
            print('task:')
            print(task)
            data = {
                'endpoint': task['endpoint'],
                'params': task['params'],
                'total': len(self.tasks),
                'id': i,
                'job_id': self.job_id
            }
            requests.post(
                url,
                json=data,
            )
        return {"status": "Success", "msg": "Job Executed"}


    def finish_job(self):
        self.job_id = self.payload['job_id']
        result = self.collect_work()
        return {"status": "Success", "msg": "Job Finished", "result": result}


    def collect_work(self):
        """ Use self.job_id to get all task reports in SQS and compile
            final results
        """
        results = {}
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=self.job_id)
        for message in queue.receive_messages():
            task_id = message.message_attributes.get('Info').get('task_id')
            status = message.message_attributes.get('Info').get('status')
            result = message.body
            results[task_id] = {
                'status': status,
                'result': result
            }
            message.delete()

        return results
