import os
import logging

from kazoo.client import KazooClient, KazooState
from kazoo.recipe.watchers import ChildrenWatch, DataWatch

logger = logging.getLogger(__name__)


class Register(object):
    def __init__(self, value, path='/scrapyd-cluster/worker', hosts='127.0.0.1:2181'):
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
