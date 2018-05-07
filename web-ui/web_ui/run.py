import json
import logging
import requests
from flask import Flask, url_for, render_template, request, jsonify, flash, redirect, send_from_directory
from werkzeug.utils import secure_filename

from six.moves.urllib_parse import urljoin

from scrapyd.utils import native_stringify_dict


from web_ui import config
from web_ui import scrapyd_utils


logging.basicConfig(level=logging.DEBUG)
app = Flask(__name__)
app.config.from_object(config)


@app.route('/')
def home():
    worker_projects = scrapyd_utils.scrapyd_http_api(app.config.get('SCRAPYD_MASTER_HOST'), 'listprojects.json', 'POST')
    return render_template('workers.html', worker_projects=worker_projects)


@app.route('/worker', methods=['GET'])
def worker_info():
    worker = request.args['worker']
    result = scrapyd_utils.listprojects(worker)
    return render_template('worker.html', result=result)


@app.route('/project', methods=['GET'])
def project_info():
    project = request.args['project']
    job_result = scrapyd_utils.scrapyd_http_api(app.config.get('SCRAPYD_MASTER_HOST'), 'listjobs.json', 'POST', param={'project': project})
    spider_result = scrapyd_utils.scrapyd_http_api(app.config.get('SCRAPYD_MASTER_HOST'), 'listallspiders.json', 'POST', param={'project': project})
    return render_template('project.html', project=project, job_result=job_result, spider_result=spider_result)


@app.route('/project/workers', methods=['GET'])
def project_workers_info():
    project = request.args['project']
    workers = scrapyd_utils.scrapyd_http_api(app.config.get('SCRAPYD_MASTER_HOST'), 'listworkers.json', 'POST', param={'project': project})
    spiders = scrapyd_utils.scrapyd_http_api(app.config.get('SCRAPYD_MASTER_HOST'), 'listallspiders.json', 'POST', param={'project': project})
    return render_template('project_workers.html', project=project, workers=workers, spiders=spiders)


@app.route('/project/spider', methods=['GET'])
def project_spider_info():
    project = request.args['project']
    spider = request.args['spider']
    workers = request.args['worker'].split(',')
    jobs = scrapyd_utils.scrapyd_http_api(app.config.get('SCRAPYD_MASTER_HOST'), 'listspiderjobs.json', 'POST', param={'project': project, 'spider': spider})
    return render_template('spider.html', spider=spider, project=project, workers=workers, jobs_result=jobs)


@app.route('/project/spider/run', methods=['GET'])
def run_spider():
    project = request.args['project']
    spider = request.args['spider']
    workers = request.args['worker']
    scrapyd_utils.scrapyd_http_api(app.config.get('SCRAPYD_MASTER_HOST'), 'schedule.json', 'POST',
                                   param={'project': project, 'spider': spider, 'hosts': workers})
    return redirect(url_for('project_info', project=project))


@app.route('/project/spider/stop', methods=['GET'])
def stop_spider():
    project = request.args['project']
    job = request.args['job']
    workers = request.args['worker']
    scrapyd_utils.scrapyd_http_api(app.config.get('SCRAPYD_MASTER_HOST'), 'cancel.json', 'POST',
                                   param={'project': project, 'job': job, 'hosts': workers})
    return redirect(url_for('project_info', project=project))


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=7777)
