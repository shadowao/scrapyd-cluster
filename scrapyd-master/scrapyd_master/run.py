import json
import logging

from flask import Flask, url_for, render_template, request, jsonify, flash, redirect, send_from_directory
from werkzeug.utils import secure_filename

from scrapyd.utils import native_stringify_dict

from scrapyd_master import config
from scrapyd_master.zk_utils import ZooWatcher
from scrapyd_master import scrapyd_utils
from scrapyd_master.scrapyd_utils import json_call

logging.basicConfig(level=logging.DEBUG)
app = Flask(__name__)
app.config.from_object(config)
zw = ZooWatcher(app.config.get('ZOOKEEPER_PATH'), hosts=app.config.get('ZOOKEEPER_HOSTS'))


@app.route('/')
def index():
    return jsonify(native_stringify_dict(zw.children, keys_only=False))


@app.route('/daemonstatus.json', methods=['POST'])
def daemonstatus():
    req_arg = request.form.to_dict()
    hosts = req_arg.pop('hosts', zw.children.values())
    if isinstance(hosts, str):
        hosts = hosts.split(',')
    return jsonify(json_call(hosts, 'daemonstatus.json', 'GET', req_arg))


@app.route('/addversion.json', methods=['POST'])
def addversion():
    req_arg = request.form.to_dict()
    hosts = req_arg.pop('hosts', zw.children.values())
    if isinstance(hosts, str):
        hosts = hosts.split(',')
    return jsonify(json_call(hosts, 'addversion.json', 'POST', req_arg, request.files))


@app.route('/schedule.json', methods=['POST'])
def schedule():
    req_arg = request.form.to_dict()
    hosts = req_arg.pop('hosts', zw.children.values())
    if isinstance(hosts, str):
        hosts = hosts.split(',')
    scrapyd_utils.version_check(hosts, req_arg['project'])
    return jsonify(json_call(hosts, 'schedule.json', 'POST', req_arg))


@app.route('/cancel.json', methods=['POST'])
def cancel():
    req_arg = request.form.to_dict()
    hosts = req_arg.pop('hosts', zw.children.values())
    if isinstance(hosts, str):
        hosts = hosts.split(',')
    return jsonify(json_call(hosts, 'cancel.json', 'POST', req_arg))


@app.route('/listprojects.json', methods=['POST'])
def listprojects():
    req_arg = request.form.to_dict()
    hosts = req_arg.pop('hosts', zw.children.values())
    if isinstance(hosts, str):
        hosts = hosts.split(',')
    return jsonify(json_call(hosts, 'listprojects.json', 'GET', req_arg))


@app.route('/listversions.json', methods=['POST'])
def listversions():
    req_arg = request.form.to_dict()
    hosts = req_arg.pop('hosts', zw.children.values())
    if isinstance(hosts, str):
        hosts = hosts.split(',')
    return jsonify(json_call(hosts, 'listversions.json', 'GET', req_arg))


@app.route('/listjobs.json', methods=['POST'])
def listjobs():
    req_arg = request.form.to_dict()
    hosts = req_arg.pop('hosts', zw.children.values())
    if isinstance(hosts, str):
        hosts = hosts.split(',')
    return jsonify(json_call(hosts, 'listjobs.json', 'GET', req_arg))


@app.route('/delversion.json', methods=['POST'])
def delversion():
    req_arg = request.form.to_dict()
    hosts = req_arg.pop('hosts', zw.children.values())
    if isinstance(hosts, str):
        hosts = hosts.split(',')
    return jsonify(json_call(hosts, 'delversion.json', 'POST', req_arg))


@app.route('/delproject.json', methods=['POST'])
def delproject():
    req_arg = request.form.to_dict()
    hosts = req_arg.pop('hosts', zw.children.values())
    if isinstance(hosts, str):
        hosts = hosts.split(',')
    return jsonify(json_call(hosts, 'delproject.json', 'POST', req_arg))


@app.route('/listspiders.json', methods=['POST'])
def listspiders():
    req_arg = request.form.to_dict()
    hosts = req_arg.pop('hosts', zw.children.values())
    if isinstance(hosts, str):
        hosts = hosts.split(',')
    return jsonify(json_call(hosts, 'listspiders.json', 'POST', req_arg))


@app.route('/listworkers.json', methods=['POST'])
def listworkers():
    req_arg = request.form.to_dict()
    project = req_arg.pop('project', None)
    return jsonify(scrapyd_utils.listworkers(list(zw.children.values()), project))


@app.route('/listallprojects.json', methods=['POST'])
def listallprojects():
    return jsonify(scrapyd_utils.listallprojects(list(zw.children.values())))


@app.route('/listallspiders.json', methods=['POST'])
def listallspiders():
    req_arg = request.form.to_dict()
    project = req_arg.pop('project', None)
    return jsonify(scrapyd_utils.listallspiders(list(zw.children.values()), project))


@app.route('/listspiderjobs.json', methods=['POST'])
def listspiderjobs():
    req_arg = request.form.to_dict()
    project = req_arg.pop('project', None)
    spider = req_arg.pop('spider', None)
    return jsonify(scrapyd_utils.listspiderjobs(list(zw.children.values()), project, spider))

if __name__ == '__main__':
    app.run()

