import requests
import logging
from collections import defaultdict
from distutils.version import LooseVersion
from six.moves.urllib_parse import urljoin

from scrapy.utils.python import to_native_str, to_bytes

logger = logging.getLogger(__name__)


def json_parse(method, url, param=None, files=None):
    try:
        if method == 'POST':
            r = requests.post(url, data=param, files=files)
        else:
            r = requests.get(url, params=param)
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

    try:
        return r.json()
    except Exception as e:
        logger.exception(e)
        return {'status': 'error', 'message': str(e)}


def daemonstatus(host):
    """ To check the load status of a service.
        Supported Request Methods: GET
    """
    url = urljoin(host, 'daemonstatus.json')
    return json_parse('GET', url)


def addversion(host, param, files):
    """ Add a version to a project, creating the project if it doesn’t exist.
        Supported Request Methods: POST
        Parameters:
            project (string, required) - the project name
            version (string, required) - the project version
            egg (file, required) - a Python egg containing the project’s code
    """
    url = urljoin(host, 'addversion.json')
    return json_parse('POST', url, param, files)


def schedule(host, param):
    """ Schedule a spider run (also known as a job), returning the job id.
        Supported Request Methods: POST
        Parameters:
            project (string, required) - the project name
            spider (string, required) - the spider name
            setting (string, optional) - a Scrapy setting to use when running the spider
            jobid (string, optional) - a job id used to identify the job, overrides the default generated UUID
            _version (string, optional) - the version of the project to use
            any other parameter is passed as spider argument
    """
    url = urljoin(host, 'schedule.json')
    return json_parse('POST', url, param)


def cancel(host, param):
    """ Cancel a spider run (aka. job). If the job is pending, it will be removed. If the job is running, it will be terminated.
        Supported Request Methods: POST
        Parameters:
            project (string, required) - the project name
            job (string, required) - the job id
    """
    url = urljoin(host, 'cancel.json')
    return json_parse('POST', url, param)


def listprojects(host):
    """ Get the list of projects uploaded to this Scrapy server.
        Supported Request Methods: GET
        Parameters: none
    """
    url = urljoin(host, 'listprojects.json')
    return json_parse('GET', url)


def listversions(host, param):
    """ Get the list of versions available for some project. The versions are returned in order, the last one is the currently used version.
        Supported Request Methods: GET
        Parameters:
            project (string, required) - the project name
    """
    url = urljoin(host, 'listversions.json')
    return json_parse('GET', url, param)


def listspiders(host, param):
    """ Get the list of spiders available in the last (unless overridden) version of some project.
        Supported Request Methods: GET
        Parameters:
            project (string, required) - the project name
            _version (string, optional) - the version of the project to examine
    """
    url = urljoin(host, 'listspiders.json')
    return json_parse('GET', url, param)


def listjobs(host, param):
    """ Get the list of pending, running and finished jobs of some project.
        Supported Request Methods: GET
        Parameters:
            project (string, option) - restrict results to project name
    """
    url = urljoin(host, 'listjobs.json')
    return json_parse('GET', url, param)


def delversion(host, param):
    """ Delete a project version. If there are no more versions available for a given project, that project will be deleted too.
        Supported Request Methods: POST
        Parameters:
            project (string, required) - the project name
            version (string, required) - the project version
    """
    url = urljoin(host, 'delversion.json')
    return json_parse('POST', url, param)


def delproject(host, param):
    """ Delete a project version. If there are no more versions available for a given project, that project will be deleted too.
        Supported Request Methods: POST
        Parameters:
            project (string, required) - the project name
            version (string, required) - the project version
    """
    url = urljoin(host, 'delproject.json')
    return json_parse('POST', url, param)


def listworkers(hosts, project):
    if not project:
        return hosts
    else:
        ret = []
        res = json_call(hosts, 'listprojects.json', 'GET')
        for h, r in res.items():
            if r['status'] == 'ok':
                projects = set(r['projects'])
                if project in projects:
                    ret.append(h)
            else:
                logger.debug('Worker %s , Message %s' % (h, str(r['message'])))
        return ret


def listallprojects(hosts):
    ret = set()
    res = json_call(hosts, 'listprojects.json', 'GET')
    for h, r in res.items():
        if r['status'] == 'ok':
            projects = set(r['projects'])
            ret.update(projects)
        else:
            logger.debug('Worker %s , Message %s' % (h, str(r['message'])))
    return list(ret)


def listallspiders(hosts, project):
    ret = defaultdict(list)
    res = json_call(hosts, 'listspiders.json', 'GET', {'project': project})
    for h, r in res.items():
        if r['status'] == 'ok':
            spiders = set(r['spiders'])
            for spider in spiders:
                ret[spider].append(h)
        else:
            logger.debug('Worker %s , Message %s' % (h, str(r['message'])))
    return ret


def listspiderjobs(hosts, project, spider):
    ret = {}
    res = json_call(hosts, 'listjobs.json', 'GET', {'project': project})
    for h, r in res.items():
        if r['status'] == 'ok':
            i = {'pending': [], 'running': [], 'finished': []}

            for job in r['pending']:
                if job['spider'] == spider:
                    i['pending'].append(job)

            for job in r['running']:
                if job['spider'] == spider:
                    i['running'].append(job)

            for job in r['finished']:
                if job['spider'] == spider:
                    i['finished'].append(job)

            ret[h] = i
        else:
            logger.debug('Worker %s , Message %s' % (h, str(r['message'])))
    return ret


def scrapyd_http_api(host, endpoint, method, param=None, files=None):
    url = urljoin(host, endpoint)
    return json_parse(method, url, param, files)


def json_call(hosts, endpoint, method, param=None, files=None):
    res = {}
    for h in hosts:
        h = to_native_str(h)
        res[h] = scrapyd_http_api(h, endpoint, method, param, files)
    return res


def version_check(hosts, project):
    versions = json_call(hosts, 'listversions.json', 'GET', {'project': project})
    latest = sorted([v for vs in versions.values() if vs['status'] == 'ok' for v in vs['versions']], key=LooseVersion)[-1]
    latest_workers = []
    update_workers = []
    for h, vs in versions.items():
        if vs['status'] == 'ok':
            if latest not in vs['versions']:
                update_workers.append(h)
            else:
                latest_workers.append(h)
        else:
            logger.debug('Worker %s , Message %s' % (h, str(r['message'])))
    if update_workers:
        egg = requests.get(urljoin(latest_workers[-1], 'eggs/%s/%s.egg' % (project, latest))).content
        json_call(update_workers, 'addversion.json', 'POST', {'project': project, 'version': latest}, files={'egg': egg})


