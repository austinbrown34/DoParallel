import requests
import os
import json
from dbliason import DatabaseLiason


class Worker(object):

    def __init__(self, payload):
        self.payload = payload
        self.endpoint = payload['endpoint']
        self.params = payload['params']
        self.total = payload['total']
        self.id = payload['id']
        self.job_id = payload['job_id']
        self.task_id = payload['task_id']
        self.job_status = payload['job_status']
        self.callback = payload['callback']
        self.dbliason = DatabaseLiason()


    def submit_work(self):
        self.report_task(self.payload['result'])
        self.notify_manager()


    def do_task(self):
        self.payload['submit_work_url'] = os.environ.get('SUBMIT_WORK_URL', None)
        requests.post(
            self.endpoint,
            json=self.payload
        )

        return {"status": "Success", "msg": "Task Executed"}


    def add_job_to_db(self, job):
        self.dbliason.add_item(job, 'Tasks')


    def update_job_task(self, response):
        key = {
            'job_id': self.job_id,
            'task_id': self.task_id
        }
        expression = "set task_status = :s, task_result = :r"

        values = {
            ':s': "Complete",
            ':r': response

        }

        self.dbliason.update_item(key, expression, values, 'Tasks')



    def report_task(self, response):
        self.update_job_task(response)



    def notify_manager(self):
        url = os.environ.get('FINISH_JOB_URL', None)
        if url is None:
            return {"status": "Error", "msg": "FINISH_JOB_URL not set."}
        data = {
            'job_id': self.job_id,
            'callback': self.callback,
            'total': self.total
        }
        requests.post(
            url,
            json=data,
        )
