from SimPy.Simulation import hold

from locking import Lockable, LockThread
from perf import Profiler
from rand import RandInterval
from system import BaseSystem, StorageNode
from txns import TxnRunner

class DynamicLockingSystem(BaseSystem):
    """
    Dynamic Locking System.
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
        #write values to local group
        for itemID, value in self.writeset.iteritems():
            item = self.snode.groups[itemID.gid][itemID]
            item.write(value)
            yield hold, self, RandInterval.get(*self.txn.config.get(
                'commit.intvl.dist', ('fix', 0)))
        #write to the original atomically
        dataset = self.snode.system.dataset
        for itemID, value in self.writeset.iteritems():
            dataset[itemID].write(value)
        #release locks
        for step in self.releaseLocks():
            yield step
