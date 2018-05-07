#!/usr/bin/env python
from __future__ import absolute_import
import os, sys
sys.path.append(os.path.abspath('..'))
import logging

from twisted.scripts.twistd import run
from os.path import join, dirname
from sys import argv

from scrapy.utils.python import to_bytes

import scrapyd
from scrapyd.config import Config

from scrapyd_worker.zk_utils import Register


def main():
    logging.basicConfig(level=logging.INFO)
    config = Config()
    Register(to_bytes('http://%s:%d' % (config.get('bind_address', '127.0.0.1'), config.getint('http_port', 6800))),
             config.get('register_path', '/scrapyd-cluster/worker'),
             hosts=config.get('zookeeper_hosts', '127.0.0.1:2181'))

    argv[1:1] = ['-n', '-y', join(dirname(scrapyd.__file__), 'txapp.py')]
    run()

if __name__ == '__main__':
    main()
