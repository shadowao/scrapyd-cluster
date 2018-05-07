# -*- coding: utf-8 -*-
import os
import logging
from kazoo.client import KazooClient, KazooState
from kazoo.recipe.watchers import ChildrenWatch, DataWatch

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class ZooWatcher(object):
    def __init__(self, path, value=b'', hosts='127.0.0.1:2181'):
        self.zk = KazooClient(hosts)
        self.zk.start()
        self.children = {}
        self.path = path
        self.zk.ensure_path(self.path)
        self.zk.set(self.path, value)
        self.children_watch = ChildrenWatch(self.zk, path, self.change)

    def change(self, children):
        children = set(children)
        old_children = set(self.children.keys())
        new_children = {c for c in children if c not in old_children}
        lost_children = {c for c in old_children if c not in children}
        logger.info('New Children : %s' % str(new_children))
        logger.info('Lost Children : %s' % str(lost_children))

        def data_watch(zk, path):
            def change(data, stat):
                if data is None:
                    # data is None equal node not exist, so return false to disable future data change call
                    return False
                logger.info('Path %s Data Change From %s to %s' % (path, self.children[path.split('/')[-1]], data))
                self.children[path.split('/')[-1]] = data
            return DataWatch(zk, path, change)
        for c in new_children:
            self.children[c] = None
            data_watch(self.zk, self._get_child_path(c))
        for c in lost_children:
            del self.children[c]
        logger.info('Remain Children : %s' % str(self.children))

    def _get_child_path(self, child):
        return self.path + '/' + child


class Register(object):
    def __init__(self, value, path, hosts='127.0.0.1:2181'):
        self.path = path
        self.value = value
        self.expire = False
        self.zk = KazooClient(hosts)
        self.zk.start()
        self.zk.add_listener(self.listener)

        self.zk.ensure_path(os.path.dirname(self.path))
        self.path = self.zk.create(self.path, self.value, ephemeral=True, sequence=True)
        logger.info('Connected To ZooKeeper(%s) Succeed! Path : %s' % (hosts, self.path))

    def register(self):
        if self.expire:
            logger.info('Renew Ephemeral Node. Path : %s  Value: %s' % (self.path, self.value))
            self.zk.create(self.path, self.value, ephemeral=True)
            self.expire = False

    def listener(self, state):
        if state == KazooState.LOST:
            logger.info('ZooKeeper Connection Lost')
            self.expire = True
        elif state == KazooState.CONNECTED:
            self.zk.handler.spawn(self.register)
