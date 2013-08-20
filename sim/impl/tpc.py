import logging
import random

from SimPy.Simulation import now
from SimPy.Simulation import waitevent, hold

from core import BThread, IDable, infinite
from locking import Lockable, LockThread
from rand import RandInterval
from rti import MsgXeiver
from system import BaseSystem, StorageNode
from txns import TxnRunner

class TPCLockingSystem(BaseSystem):
    """Two phase commit protocol with two phase locking implementation. """
    def newStorageNode(self, cnode, index, configs):
        return TPCSNode(cnode, index, configs)

    def profile(self):
        BaseSystem.profile(self)
        pass

class TPCSNode(StorageNode):
    def __init__(self, cnode, index, configs):
        StorageNode.__init__(self, cnode, index, configs)
        self.proxies = {}       #{txn : proxy}
        self.latest = -1

    def newTxnRunner(self, txn):
        return TPCTxnRunner(self, txn)

    def createProxy(self, txn, runner):
        #magic method for the runner to create a proxy on this snode.
        if txn not in self.proxies:
            proxy = TPLProxy(self, txn, runner)
            self.proxies[txn] = proxy
            proxy.start()
        return self.proxies[txn]

class TPCProtocol(object):
    RUNNING, COMMITTING, ABORTED, COMMITTED, \
            SUCCEEDED, FAILED = range(6)
    STATES_STR = \
            ('RUNNING', 'COMMITTING', 'ABORTED', 'COMMITTED')

class ConnResource(IDable):
    """Connection resource for deadlock detection."""
    def __init__(self, runner, proxy):
        IDable.__init__(self, '%s--%s'%(runner.ID, proxy.ID))

class TPCTxnRunner(TxnRunner, MsgXeiver):
    def __init__(self, snode, txn):
        TxnRunner.__init__(self, snode, txn)
        MsgXeiver.__init__(self, snode.ID)
        self.attemptNo = 0
        self.writeset = {}
        self.proxies = set([])
        self.ts = 0

    def read(self, itemID, attr):
        if itemID.gid in self.snode.groups:
            #the item is on the snode
            self.monitor.start('local.lock.acquire.shared')
            item = self.snode.groups[itemID.gid][itemID]
            self.logger.debug('%s read %s from local at %s'
                              %(self.ID, itemID, now()))
            try:
                for step in self.lock(item, Lockable.SHARED):
                    yield step
            except Exception as e:
                self.monitor.stop('local.lock.acquire.shared')
                raise e
            self.locks.add(item)
            self.monitor.stop('local.lock.acquire.shared')
            self.logger.debug('%s read %s from local "success" at %s'
                              %(self.ID, itemID, now()))
        else:
            #the item is on another snode
            cnode = self.snode.cnode
            host = cnode.groupLocations[itemID.gid]
            proxy = host.createProxy(self.txn, self)
            self.proxies.add(proxy)
            self.logger.debug('%s send read request %s to %s at %s'
                              %(self.ID, itemID, proxy.ID, now()))
            self.sendMsg(proxy, 'msg',
                         (self.attemptNo, TPCProtocol.RUNNING, itemID))
            self.released(proxy.conn)
            self.tryWait(proxy.conn)
            until = now() + self.snode.configs.get('tpc.conn.timeout', infinite)
            while True:
                if not self.checkMsg('msg'):
                    for step in self.waitMsg('msg', until - now()):
                        yield step
                succeeded = False
                for content in self.popContents('msg'):
                    p, attemptNo, label, result, e = content
                    assert p == proxy and attemptNo <= self.attemptNo
                    if attemptNo < self.attemptNo:
                        continue
                    assert label == TPCProtocol.RUNNING
                    if result == TPCProtocol.FAILED:
                        #the only reason it fails should be deadlock
                        self.logger.debug(
                            '%s acquire read lock %s from %s "failed" at %s' 
                            %(self.ID, itemID, proxy.ID, now()))
                        raise e
                    elif result == TPCProtocol.SUCCEEDED:
                        succeeded = True
                        break
                    else:
                        raise ValueError('Invalid result for proxy message: %s'
                                         %result)
                if succeeded:
                    self.logger.debug(
                        '%s acquire read lock %s from %s "succeeded" at %s' 
                        %(self.ID, itemID, proxy.ID, now()))
                    self.endWait(proxy.conn)
                    self.acquired(proxy.conn)
                    break

    def write(self, itemID, attr):
        self.writeset[itemID] = attr
        yield hold, self

    def trycommit(self):
        self.logger.debug('%s "try commit" at %s' %(self.ID, now()))
        writesets = self.getSnodeWritesets()
        commitProxies = set([])
        for snode, writeset in writesets.iteritems():
            proxy = snode.createProxy(self.txn, self)
            self.proxies.add(proxy)
            commitProxies.add(proxy)
            self.logger.debug('%s send try commit request to %s at %s'
                              %(self.ID, proxy.ID, now()))
            self.sendMsg(proxy, 'msg',
                         (self.attemptNo, TPCProtocol.COMMITTING, writeset))
            self.released(proxy.conn)
            self.tryWait(proxy.conn)
        #wait for all commit proxies ready
        until = now() + self.snode.configs.get('tpc.conn.timeout', infinite)
        while len(commitProxies) > 0:
            if not self.checkMsg('msg'):
                for step in self.waitMsg('msg', until - now()):
                    yield step
            for content in self.popContents('msg'):
                p, attemptNo, label, result, attr = content
                assert attemptNo <= self.attemptNo
                if attemptNo < self.attemptNo:
                    continue
                assert label <= TPCProtocol.COMMITTING
                if result == TPCProtocol.FAILED:
                    #the only reason it fails should be deadlock
                    self.logger.debug(
                        '%s try commit from %s "failed" at %s' 
                        %(self.ID, proxy.ID, now()))
                    raise attr
                elif result == TPCProtocol.SUCCEEDED:
                    ts = attr
                    commitProxies.remove(p)
                    self.endWait(p.conn)
                    self.acquired(p.conn)
                    if self.ts < ts:
                        self.ts = ts
                else:
                    raise ValueError('Invalid result for proxy message: %s'
                                     %result)
        self.logger.debug('%s got all commit proxy return at %s'
                          %(self.ID, now()))

    def getSnodeWritesets(self):
        writesets = {}
        system = self.snode.system
        for cnode in system.cnodes:
            for itemID, value in self.writeset.iteritems():
                snode = cnode.groupLocations[itemID.gid]
                if snode not in writesets:
                    writesets[snode] = {}
                writesets[snode][itemID] = value
        return writesets

    def commit(self):
        for proxy in self.proxies:
            self.sendMsg(proxy, 'msg',
                         (self.attemptNo, TPCProtocol.COMMITTED, self.ts))
            self.released(proxy.conn)
        self.logger.debug(
            '%s "sent commit requests" at %s' %(self.ID, now()))
        yield hold, self

    def abort(self):
        for proxy in self.proxies:
            self.sendMsg(proxy, 'msg',
                         (self.attemptNo, TPCProtocol.ABORTED, None))
            self.released(proxy.conn)
        self.logger.debug(
            '%s "sent abort requests" at %s' %(self.ID, now()))
        yield hold, self

class TPLProxy(LockThread, MsgXeiver):
    def __init__(self, snode, txn, runner):
        LockThread.__init__(self, '%s/pr-%s'%(snode.ID, txn.ID))
        MsgXeiver.__init__(self, snode.ID)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.txn = txn
        self.snode = snode
        self.runner = runner
        self.shouldRun = True
        self.progress = (0, TPCProtocol.RUNNING)
        self.locks = set([])
        self.writeset = {}  #{itemID : value}
        self.conn = ConnResource(self.runner, self)

    def close(self):
        self.shouldRun = False

    def run(self):
        while self.shouldRun:
            self.tryWait(self.conn)
            if not self.checkMsg('msg'):
                for step in self.waitMsg('msg'):
                    yield step
            try:
                for step in self.handle():
                    yield step
            except BThread.DeadlockException as e:
                self.logger.debug('%s get deadlock %s at %s'
                                  %(self.ID, str(e), now()))
                for step in self.releaseLocks():
                    yield step
        #release locks before exit
        for step in self.releaseLocks():
            yield step
        del self.snode.proxies[self.txn]

    def handle(self):
        #notify deadlock management
        self.endWait(self.conn)
        self.acquired(self.conn)
        #handle message
        latestNo = (0, 0)
        latest = None
        #only looking for the latest message
        for content in self.popContents('msg'):
            attemptNo, label, attr = content
            self.logger.debug(
                '%s received message attemptNo=%s, label=%s at %s' 
                %(self.ID, attemptNo, TPCProtocol.STATES_STR[label], now()))
            if latestNo <= (attemptNo, label):
                latestNo = (attemptNo, label)
                latest = content
        attemptNo, label, attr = latest
        #sanity check and ignore previous attempts
        if label == TPCProtocol.COMMITTED:
            #we must in the state of committing to get a committed request
            assert self.progress == (attemptNo, TPCProtocol.COMMITTING)
        if latestNo < self.progress:
            #ignore the previous attempts will be fine
            return
        if latestNo == self.progress:
            #committing, aborted and committed transition need only happen once
            #in each attempt.
            if self.progress[1] != TPCProtocol.RUNNING:
                return
        #do work
        # if future attempt arrives, it means this attempt is vetoed and
        # aborted ,so we abort the current attempt and let others have the
        # locks
        if attemptNo > self.progress[0] and \
           not self.progress[1] == TPCProtocol.ABORTED:
            for step in self.abort(latest):
                yield step
        if label == TPCProtocol.RUNNING:
            for step in self.read(latest):
                yield step
        elif label == TPCProtocol.COMMITTING:
            for step in self.trycommit(latest):
                yield step
        elif label == TPCProtocol.ABORTED:
            for step in self.abort(latest):
                yield step
        elif label == TPCProtocol.COMMITTED:
            for step in self.commit(latest):
                yield step
        else:
            raise ValueError('Invalid label for proxy message: %s' %label)
        self.released(self.conn)

    def read(self, content):
        attemptNo, label, itemID = content
        self.progress = (attemptNo, TPCProtocol.RUNNING)
        try:
            item = self.snode.groups[itemID.gid][itemID]
            for step in self.lock(item, Lockable.SHARED):
                yield step
            self.locks.add(item)
            self.logger.debug(
                '%s read item %s succeeded at %s' %(self.ID, item, now()))
            self.sendMsg(self.runner, 'msg',
                         (self, attemptNo, label, TPCProtocol.SUCCEEDED, None))
        except BThread.DeadlockException as e:
            self.logger.debug(
                '%s read items %s failed at %s' %(self.ID, item, now()))
            #we know it will abort
            self.sendMsg(self.runner, 'msg',
                         (self, attemptNo, label, TPCProtocol.FAILED, e))
            for step in self.abort(content):
                yield step

    def trycommit(self, content):
        attemptNo, label, writeset = content
        self.progress = (attemptNo, TPCProtocol.COMMITTING)
        try:
            for itemID, value in writeset.iteritems():
                item = self.snode.groups[itemID.gid][itemID]
                for step in self.lock(item, Lockable.EXCLUSIVE):
                    yield step
                self.locks.add(item)
                self.writeset[itemID] = value
            self.snode.latest += 1
            self.sendMsg(self.runner, 'msg',
                         (self, attemptNo, label, TPCProtocol.SUCCEEDED, self.snode.latest))
            self.logger.debug('%s try commit succeeded at %s' %(self.ID, now()))
        except BThread.DeadlockException as e:
            self.sendMsg(self.runner, 'msg',
                         (self, attemptNo, label, TPCProtocol.FAILED, e))
            self.logger.debug('%s try commit failed by deadlock %s at %s' 
                              %(self.ID, e, now()))
            #we know it will abort
            for step in self.abort(content):
                yield step

    def abort(self, content):
        attemptNo = content[0]
        for item in self.locks:
            for step in self.unlock(item):
                yield step
        self.progress = (attemptNo, TPCProtocol.ABORTED)
        self.logger.debug('%s aborted with attemptNo %s at %s' 
                          %(self.ID, attemptNo, now()))

    def commit(self, content):
        attemptNo, label, ts = content
        for itemID, value in self.writeset.iteritems():
            item = self.snode.groups[itemID.gid][itemID]
            assert ts > item.version
            item.write(value, ts)
            yield hold, self, RandInterval.get(*self.txn.config.get(
                'commit.intvl.dist', ('fix', 0)))
        #write to the original atomically
        dataset = self.snode.system.dataset
        for itemID, value in self.writeset.iteritems():
            dataset[itemID].write(value, ts)
        self.progress = (attemptNo, TPCProtocol.COMMITTED)
        self.logger.debug('%s committed with attemptNo %s at %s' 
                          %(self.ID, attemptNo, now()))
        self.close()

    def releaseLocks(self):
        for lock in self.locks:
            for step in self.unlock(lock):
                yield step

