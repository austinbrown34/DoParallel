import requests
import boto3
import os


class Worker(object):

    def __init__(self, payload):
        self.payload = payload
        self.endpoint = payload['endpoint']
        self.task = payload['task']
        self.total = payload['total']
        self.completed_bucket = payload['completed_bucket']
        self.id = payload['id']
        self.job_id = payload['job_id']


    def do_task(self):
        response = requests.post(
            self.endpoint,
            data=self.task
        )
        self.report_task(response)
        self.check_job_status()

        return {"status": "Success", "msg": "Task Executed"}


    def report_task(self, response):
        session = boto3.Session()
        s3 = session.resource('s3')
        os.mkdir(self.job_id)
        path = os.path.join(
            self.job_id,
            '{}.txt'.format(id)
        )
        report = file(path, 'w')
        report.write(response)
        report.close()
        s3.meta.client.upload_file(
            path, self.completed_bucket, path
        )

    def check_job_status(self):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.completed_bucket)
        if len(bucket.objects.all()) == self.total:
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
            data=data,
        )
