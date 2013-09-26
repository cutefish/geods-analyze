import logging

from SimPy.Simulation import hold, now

import sim
from sim.locking import Lockable, LockThread
from sim.perf import Profiler
from sim.rand import RandInterval
from sim.system import BaseSystem, StorageNode
from sim.txns import TxnRunner

class CentralDyLockSystem(BaseSystem):
    """
    Centralized Dynamic Locking System.
    """
    def newStorageNode(self, cnode, index, configs):
        return DLSNode(cnode, index, configs)

    def profile(self):
        BaseSystem.profile(self)
        rootMon = Profiler.getMonitor('/')
        numLockAcquire = rootMon.getElapsedCount('.*lock.acquire')
        bmean, bstd, bhisto, bcount = \
                rootMon.getElapsedStats('.*%s'%LockThread.LOCK_BLOCK_KEY)
        self.logger.info('lock.block.prob=%s'%(float(bcount) / numLockAcquire))
        self.logger.info('lock.block.time.mean=%s'%bmean)
        self.logger.info('lock.block.time.std=%s'%bstd)
        self.logger.info('lock.block.time.histo=(%s, %s)'%(bhisto))
        numExecTxns = rootMon.getElapsedCount(
            '.*%s'%BaseSystem.TXN_EXEC_KEY_PREFIX)
        dmean, dstd, dhisto, dcount = \
                rootMon.getElapsedStats('.*abort.deadlock')
        self.logger.info('abort.deadlock.prob=%s'%(float(dcount) / numExecTxns))
        self.logger.info('abort.deadlock.time.mean=%s'%dmean)
        self.logger.info('abort.deadlock.time.std=%s'%dstd)
        self.logger.info('abort.deadlock.time.histo=(%s, %s)'%(dhisto))
        cmean, cstd, chisto, ccount = \
                rootMon.getObservedStats('.*deadlock.cycle.length')
        self.logger.info('deadlock.cycle.length.mean=%s'%cmean)
        self.logger.info('deadlock.cycle.length.std=%s'%cstd)
        self.logger.info('deadlock.cycle.length.histo=(%s, %s)'%(chisto))
        hmean, hstd, hhisto, hcount = \
                rootMon.getObservedStats('.*%s'%LockThread.LOCK_BLOCK_HEIGHT_KEY)
        self.logger.info('block.height.mean=%s'%hmean)
        self.logger.info('block.height.std=%s'%hstd)
        self.logger.info('block.height.histo=(%s, %s)'%(hhisto))
        wmean, wstd, whisto, wcount = \
                rootMon.getObservedStats('.*%s'%LockThread.LOCK_BLOCK_WIDTH_KEY)
        self.logger.info('block.width.mean=%s'%wmean)
        self.logger.info('block.width.std=%s'%wstd)
        self.logger.info('block.width.histo=(%s, %s)'%(whisto))

class DLSNode(StorageNode):
    def newTxnRunner(self, txn):
        return DLTxnRunner(self, txn)

class DLTxnRunner(TxnRunner):
    def __init__(self, snode, txn):
        TxnRunner.__init__(self, snode, txn)
        self.locks = set([])
        self.writeset = {}

    def read(self, itemID, attr):
        self.monitor.start('lock.acquire.shared')
        item = self.snode.groups[itemID.gid][itemID]
        try:
            for step in self.lock(item, Lockable.SHARED):
                yield step
        except Exception as e:
            self.monitor.stop('lock.acquire.shared')
            raise e
        self.locks.add(item)
        self.monitor.stop('lock.acquire.shared')

    def write(self, itemID, attr):
        self.monitor.start('lock.acquire.exclusive')
        item = self.snode.groups[itemID.gid][itemID]
        try:
            for step in self.lock(item, Lockable.EXCLUSIVE):
                yield step
        except Exception as e:
            self.monitor.stop('lock.acquire.exclusive')
            raise e
        self.locks.add(item)
        self.writeset[itemID] = attr
        self.monitor.stop('lock.acquire.exclusive')

    def releaseLocks(self):
        for lock in self.locks:
            for step in self.unlock(lock):
                yield step

    def abort(self):
        for step in self.releaseLocks():
            yield step

    def commit(self):
        self.logger.debug('%s start commit at %s'%(self, now()))
        wsStrings = []
        #write values to local group
        for itemID, value in self.writeset.iteritems():
            item = self.snode.groups[itemID.gid][itemID]
            item.write(value)
            if self.logger.isEnabledFor(logging.DEBUG):
                wsStrings.append('(%s, %s)'%(itemID, value))
            yield hold, self, RandInterval.get(*self.txn.config.get(
                'commit.intvl.dist', ('fixed', 0))).next()
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug('%s commit {%s} at %s'
                              %(self.ID, ', '.join([s for s in wsStrings]), now()))
        #write to the original atomically
        dataset = self.snode.system.dataset
        for itemID, value in self.writeset.iteritems():
            dataset[itemID].write(value)
            dataset[itemID].lastWriteTxn = self.txn
        #release locks
        for step in self.releaseLocks():
            yield step
