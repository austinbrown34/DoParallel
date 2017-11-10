# DoParallel

DoParallel utilizes AWS Lambda and AWS SQS to execute a list of tasks (represented as a job) in parallel. Each task runs as a separate Lambda instance and reports it's return value and status as a message to it's corresponding job's SQS queue. After each task is complete and has submitted it's message to the queue, the queue is checked to see if all the tasks for the job are done. When all tasks in a job are completed, all messages are retrieved from the job's queue to collect each tasks results, a final results object is returned.

## Getting Started

These instructions will get you a copy of the project up and running on AWS Lambda.

### Prerequisites

Before you begin, make sure you are running Python 2.7 or Python 3.6 and you have a valid AWS account and your AWS credentials file is properly installed. Please note that DoParallel must be installed into your project's virtual environment.


### Installing

1. Clone git@github.com:austinbrown34/DoParallel.git

```
git clone git@github.com:austinbrown34/DoParallel.git
```

2. Create a virtual environment in this directory.

```
virtualenv env
```

3. Activate your virtual environment.

```
. env/bin/activate
```

4. Install Requirements

```
pip install -r requirements.txt
```

5. Initialize Zappa and Follow Instructions

```
zappa init
```

6. Deploy Zappa

```
zappa deploy
```

7. Login to Lambda Console and Set Environment Variables

```
FINISH_JOB_URL: '/v1/finish' endpoint of your Lambda url
DO_TASK_URL: '/v1/task' endpoint of your Lambda url
```


### Usage

Post a job to the '/v1/job' endpoint of your Lambda url as json.

The payload of your post should include the following attributes:

```
callback - The url that completed job results should be posted back to.

tasks - A list of task objects that should be executed in parallel.
```

A task object should have the following attributes:

```
endpoint - The url to the Lambda function that you set up to run a single task.

params - Any parameters that define the specific variables needed to run your Lambda function for this task.
```

An example payload might look like this:

```
{
  "callback": "http://myapi.com/endpoint/receive/results",
  "tasks": [
    {
      "endpoint": "http://mylambdafunction.com/endpoint",
      "params": {
        "variable_a": "22",
        "variable_b": "44"
      }
    },
    {
      "endpoint": "http://mylambdafunction.com/endpoint",
      "params": {
        "variable_a": "12",
        "variable_b": "14"
      }
    }
  ]
}
```

When all tasks in your job have finished, a results object will be posted to your specified callback url containing the overall job status, status message, and result:

```
{
  "status": "Success",
  "msg:": "Job Finished",
  "result": {
    "1": {
      "status": "Complete",
      "result": "Some return value"
    },
    "2": {
      "status": "Complete",
      "result": "Some other return value"
    },
    ...
  }
}
```

* To test DoParallel without a separate Lambda setup for your task execution, you can use the '/v1/test' endpoint of your Lambda url as an endpoint for your tasks.

## Authors

* **Austin Brown** - *Initial work* - [AustinBrown](https://github.com/austinbrown34)


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
