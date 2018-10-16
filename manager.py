import requests
import grequests
import os
import uuid
from dbliason import DatabaseLiason


class Manager(object):

    def __init__(self, payload):
        self.payload = payload
        self.callback = self.payload['callback']
        self.dbliason = DatabaseLiason()
        self.setup_jobs_table()
        self.setup_tasks_table()


    def setup_jobs_table(self):
        if not self.dbliason.table_exists('Jobs'):
            self.dbliason.create_jobs_table()


    def setup_tasks_table(self):
        self.dbliason = DatabaseLiason()
        if not self.dbliason.table_exists('Tasks'):
            self.dbliason.create_tasks_table()


    def add_job_to_db(self, job):
        self.dbliason.add_item(job, 'Jobs')


    def add_tasks_to_db(self, tasks):
        for task in tasks:
            self.dbliason.add_item(tasks[task], 'Tasks')


    def do_job(self):
        self.tasks = self.payload['tasks']
        self.job_id = str(uuid.uuid4())
        url = os.environ.get('DO_TASK_URL', None)
        job_tasks = {}
        if url is None:
            return {"status": "Error", "msg": "DO_TASK_URL not set."}

        for i, task in enumerate(self.tasks):

            job_tasks['task{}'.format(i)] = {
                'task_id': 'task{}'.format(i),
                'job_id': self.job_id,
                'endpoint': task['endpoint'],
                'params': task['params'],
                'task_status': 'Pending'
            }
        job = {
            'job_id': self.job_id,
            'job_status': 'Pending',

        }

        self.add_job_to_db(job)
        self.add_tasks_to_db(job_tasks)
        # headers = {'X-Amz-Invocation-Type': 'Event'}
        data_list = []
        for i, task in enumerate(self.tasks):
            data = {
                'endpoint': task['endpoint'],
                'params': task['params'],
                'total': len(self.tasks),
                'id': i,
                'job_id': self.job_id,
                'callback': self.callback,
                'job_status': 'Pending',
                'task_id': 'task{}'.format(i),
            }
            data_list.append(data)
            # print('Starting post')
            # requests.post(
            #     url,
            #     json=data,
            #
            # )
            # print('Done Posting')

        rs = (grequests.post(url, json=d) for d in data_list)
        grequests.map(rs)


        return {"status": "Success", "msg": "Job Executed"}


    def update_job_status(self, status):
        key = {
            'job_id': self.job_id,
            'job_status': 'Pending'
        }
        self.dbliason.delete_item(key, 'Jobs')
        self.dbliason.add_item(
            {
                'job_id': self.job_id,
                'job_status': 'Complete'
            },
            'Jobs'
        )


    def update_job_result(self, result):
        key = {
            'job_id': self.job_id,
            'job_status': 'Complete'
        }
        expression = "set job_result = :r"

        values = {
            ':r': result
        }

        self.dbliason.update_item(key, expression, values, 'Jobs')


    def finish_job(self):
        self.update_job_status('Complete')
        result = self.collect_work()
        result['payload'] = self.payload
        self.update_job_result(result)
        print('calling callback:')
        r = requests.post(self.callback, json=result)
        print(r)
        return {"status": "Complete", "msg": "Job Finished", "result": result}


    def check_job(self):
        self.job_id = self.payload['job_id']
        tasks = self.dbliason.get_job_tasks(self.job_id, 'Tasks')
        all_complete = True
        for task in tasks:
            if task['task_status'] != 'Complete':
                all_complete = False
        if all_complete:
            self.finish_job()

    def collect_work(self):
        results = {}
        tasks = self.dbliason.get_job_tasks(self.job_id, 'Tasks')
        for task in tasks:
            results[task['task_id']] = {
                'status': task['task_status'],
                'result': task['task_result']
            }

        print('results:')
        print(results)

        return results
