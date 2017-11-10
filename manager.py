import requests
import os
import uuid


class Manager(object):

    def __init__(self, payload):
        self.payload = payload
        self.callback = self.payload['callback']


    def do_job(self):
        self.tasks = self.payload['tasks']
        self.job_id = str(uuid.uuid4())
        url = os.environ.get('DO_TASK_URL', None)
        completed_bucket = os.environ.get('COMPLETED_BUCKET', None)
        if url is None:
            return {"status": "Error", "msg": "DO_TASK_URL not set."}
        if completed_bucket is None:
            return {"status": "Error", "msg": "COMPLETED_BUCKET not set."}
        for i, task in enumerate(self.tasks):
            data = {
                'task': task['task'],
                'endpoint': task['endpoint'],
                'total': len(self.tasks),
                'completed_bucket': completed_bucket,
                'id': i,
                'job_id': self.job_id
            }
            requests.post(
                url,
                data=data,
            )
        return {"status": "Success", "msg": "Job Executed"}


    def finish_job(self):
        self.job_id = self.payload['job_id']
        result = self.collect_work()
        return {"status": "Success", "msg": "Job Finished", "result": result}


    def collect_work(self):
        """ Use self.job_id to get all task reports in S3 and compile
            final results
        """
        result = {}
        return result
