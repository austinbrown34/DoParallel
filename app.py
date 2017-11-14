from flask import Flask, jsonify, make_response, request, abort
from manager import Manager
from worker import Worker

app = Flask(__name__)


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


@app.route('/')
def do_parallel():
    return "Let's Do it Parallel!"


@app.route('/v1/test', methods=['POST'])
def test_job():
    if not (request.json):
        abort(400)

    return jsonify({"result": "true"})


@app.route('/v1/finish', methods=['POST'])
def finish_job():
    if not (request.json):
        abort(400)
    manager = Manager(request.json)
    response = manager.check_job()

    return jsonify(response)


@app.route('/v1/task', methods=['POST'])
def do_task():
    if not (request.json):
        abort(400)
    worker = Worker(request.json)
    response = worker.do_task()

    return jsonify(response)


@app.route('/v1/job', methods=['POST'])
def do_job():
    if not (request.json):
        abort(400)
    manager = Manager(request.json)
    response = manager.do_job()

    return jsonify(response)

if __name__ == '__main__':
    app.run()
