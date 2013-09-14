import logging

from SimPy.Simulation import SimEvent
from SimPy.Simulation import hold, waitevent, now

from core import BThread
from locking import Lockable, LockThread
from perf import Profiler
from rand import RandInterval
from system import BaseSystem, StorageNode
from txns import TxnRunner

def StrictFCFSAlgo(lockable):
    # we use a simple algorithm:
    #   Wake up the first thread in queue. If it acquires a shared mode,
    #   then wake up all the other shared threads in queue before a exclusive. 
    if len(lockable.blockQueue) == 0:
        return None, None
    threads = []
    first = lockable.blockQueue[0]
    state, event = lockable.blockedThreads[first]
    lockable.logger.debug('Lockable %r wake up %s' %(lockable, first.ID))
    event.signal()
    threads.append(first)
    if state == Lockable.SHARED:
        for thread in lockable.blockQueue[1:]:
            s, e = lockable.blockedThreads[thread]
            if s == Lockable.SHARED:
                lockable.logger.debug('Lockable %r wake up %s' 
                                  %(lockable, thread.ID))
                e.signal()
                threads.append(thread)
            else:
                break
    return threads, state

class CentralDetmnSystem(BaseSystem):
    """
    Centrialized Deterministic System.
    """
    def __init__(self, configs):
        BaseSystem.__init__(self, configs)
        Lockable.WakeupAlgo = StrictFCFSAlgo

    def newStorageNode(self, cnode, index, configs):
        return CDSNode(cnode, index, configs)

    def profile(self):
        BaseSystem.profile(self)
        rootMon = Profiler.getMonitor('/')
        wmean, wstd, whisto, wcount = \
                rootMon.getElapsedStats('.*wait.lock')
        self.logger.info('wait.lock.time.mean=%s'%wmean)
        self.logger.info('wait.lock.time.std=%s'%wstd)
        self.logger.info('wait.lock.time.histo=(%s, %s)'%whisto)
        numLockAcquire = rootMon.getElapsedCount('.*lock.acquire')
        bmean, bstd, bhisto, bcount = \
                rootMon.getElapsedStats('.*%s'%LockThread.LOCK_BLOCK_KEY)
        self.logger.info('lock.block.prob=%s'%(float(bcount) / numLockAcquire))
        self.logger.info('lock.block.time.mean=%s'%bmean)
        self.logger.info('lock.block.time.std=%s'%bstd)
        self.logger.info('lock.block.time.histo=(%s, %s)'%(bhisto))
        nlmean, nlstd, nlhisto, nlcount = \
                rootMon.getObservedStats('.*num.blocking.lock')
        self.logger.info('num.blocking.lock.mean=%s'%nlmean)
        self.logger.info('num.blocking.lock.std=%s'%nlstd)
        self.logger.info('num.blocking.lock.histo=(%s, %s)'%(nlhisto))
        hmean, hstd, hhisto, hcount = \
                rootMon.getObservedStats('.*%s.cond'%LockThread.LOCK_BLOCK_HEIGHT_KEY)
        self.logger.info('block.height.mean=%s'%hmean)
        self.logger.info('block.height.std=%s'%hstd)
        self.logger.info('block.height.histo=(%s, %s)'%(hhisto))
        wmean, wstd, whisto, wcount = \
                rootMon.getObservedStats('.*%s'%LockThread.LOCK_BLOCK_WIDTH_KEY)
        self.logger.info('block.width.mean=%s'%wmean)
        self.logger.info('block.width.std=%s'%wstd)
        self.logger.info('block.width.histo=(%s, %s)'%(whisto))
        h = rootMon.getObservedMean('.*%s.abs'%LockThread.LOCK_BLOCK_HEIGHT_KEY)
        self.logger.info('block.height.mean.noncond=%s'%h)
        dw = rootMon.getObservedMean('.*lock.block.direct.width')
        self.logger.info('block.dwidth.mean=%s'%dw)

class CDSNode(StorageNode):
    def __init__(self, cnode, index, configs):
        StorageNode.__init__(self, cnode, index, configs)
        #txns that are not yet granted locks, in FCFS order
        self.lockingQueue= []
        self.nextEvent = SimEvent()
        self.ts = 0

    def run(self):
        #the big while loop
        while True:
            yield waitevent, self, (self.closeEvent, self.newTxnEvent)
            while len(self.newTxns) > 0:
                txn = self.newTxns.pop(0)
                #add txn to the locking queue
                self.lockingQueue.append(txn)
                thread = StorageNode.TxnStarter(self, txn)
                thread.start()
            if self.shouldClose:
                self.logger.info(
                    '%s closing. Wait for threads to terminate at %s'
                    %(self.ID, now()))
                #wait for running threads to terminate and close
                for thread in self.runningThreads:
                    if not thread.isFinished():
                        yield waitevent, self, thread.finish
                break

    def newTxnRunner(self, txn):
        return DETxnRunner(self, txn)

class DETxnRunner(TxnRunner):
    def __init__(self, snode, txn):
        TxnRunner.__init__(self, snode, txn)
        self.locks = set([])
        self.writeset = {}
        self.snode.ts += 1
        self.ts = self.snode.ts

    def nonblockLock(self, lockable, state):
        self.logger.debug(
            '%s lock %r at %s' %(self.ID, lockable, now()))
        #try acquire the lockable
        acquired = lockable.tryAcquire(self, state)
        if acquired:
            self.logger.debug(
                '%s acquired %s at %s' %(self.ID, lockable, now()))
            #notify deadlock detection
            self.acquired(lockable)
            return None
        #cannot acquire, block until timeout
        self.logger.debug(
            '%s "blocked" on %r at %s' %(self.ID, lockable, now()))
        # here we first ensure all owners who blocked us are all alive
        # and no deadlock
        lockable.ensureOwnersAlive()
        try:
            self.tryWait(lockable)
        except BThread.DeadlockException as e:
            assert 0, 'should not have deadlock: %s'%str(e)
        # we pass the tests, now we wait
        blockEvt = lockable.block(self, state)
        return blockEvt

    def begin(self):
        self.monitor.start('wait.lock')
        index = -1
        while index != 0:
            try:
                index = self.snode.lockingQueue.index(self.txn)
                self.logger.debug('%s current position at %s' %(self.ID, index))
                if index == 0:
                    break
            except:
                raise ValueError(
                    '%s not on %s lockingQueue %s upon start'
                    %(self.txn, self.snode,
                      '[%s]'%','.join([t.ID for t in self.snode.lockingQueue])))
            yield waitevent, self, self.snode.nextEvent
        self.logger.debug('%s start locking process at %s' %(self.ID, now()))
        self.monitor.stop('wait.lock')
        #now we are at the head of the queue
        self.monitor.start('lock.acquire')
        blockEvts = {}
        for action in self.txn.actions:
            if action.isRead():
                state = Lockable.SHARED
            else:
                assert action.isWrite()
                state = Lockable.EXCLUSIVE
            itemID = action.itemID
            item = self.snode.groups[itemID.gid][itemID]
            self.locks.add(item)
            blockEvt = self.nonblockLock(item, state)
            if blockEvt is not None:
                blockEvts[blockEvt] = item
        #we have queued all the locks and got them, the next can proceed
        txn = self.snode.lockingQueue.pop(0)
        assert txn == self.txn
        self.snode.nextEvent.signal()
        #we wait for all blockEvts to happen
        #the underlying lockable wakeup algorithm is garanteed to wake up
        #threads in FCFS order.
        self.monitor.observe('%s.abs'%LockThread.LOCK_BLOCK_HEIGHT_KEY, self.height)
        if len(blockEvts) > 0:
            self.monitor.observe('num.blocking.lock', len(blockEvts))
            self.monitor.observe('%s.cond'%LockThread.LOCK_BLOCK_HEIGHT_KEY, self.height)
            self.monitor.observe(LockThread.LOCK_BLOCK_WIDTH_KEY, self.width)
            self.monitor.observe('lock.block.direct.width', self.dwidth)
            self.monitor.start(LockThread.LOCK_BLOCK_KEY)
            while len(blockEvts) != 0:
                yield waitevent, self, tuple(blockEvts)
                for evt in self.eventsFired:
                    self.endWait(blockEvts[evt])
                    self.acquired(blockEvts[evt])
                    del blockEvts[evt]
            self.monitor.stop(LockThread.LOCK_BLOCK_KEY)
        else:
            assert self.height == 0
        self.logger.debug('%s acquired all locks at %s' %(self.ID, now()))
        self.monitor.stop('lock.acquire')

    def write(self, itemID, attr):
        self.writeset[itemID] = attr
        yield hold, self

    def commit(self):
        wsStrings = []
        for itemID, value in self.writeset.iteritems():
            item = self.snode.groups[itemID.gid][itemID]
            assert self.ts > item.version, \
                    'txn=%s, itemID=%s, curr=%s, prev=%s' \
                    %(self.txn.ID, itemID, self.ts, item.version)
            item.write(value, self.ts)
            if self.logger.isEnabledFor(logging.DEBUG):
                wsStrings.append('(%s, %s)'%(itemID, value))
            yield hold, self, RandInterval.get(*self.txn.config.get(
                'commit.intvl.dist', ('fix', 0)))
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug('%s commit {%s}'
                              %(self.ID, ', '.join([s for s in wsStrings])))
        dataset = self.snode.system.dataset
        for itemID, value in self.writeset.iteritems():
            dataset[itemID].write(value, self.ts)
            dataset[itemID].lastWriteTxn = self.txn
        for lock in self.locks:
            for step in self.unlock(lock):
                yield step

