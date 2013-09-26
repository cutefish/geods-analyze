import logging
import random
import sys

from SimPy.Simulation import SimEvent
from SimPy.Simulation import hold, waitevent
from SimPy.Simulation import initialize, simulate, now

import sim
from sim.core import Alarm, IDable, BThread, TimeoutException, infinite
from sim.perf import Profiler

def FCFSAlgo(lockable):
    # we use a simple algorithm:
    #   Wake up the first thread in queue. If it acquires a shared mode,
    #   then wake up all the other shared threads in queue. 
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
    return threads, state

class Lockable(IDable):
    """ An object that can be locked.

    Only a LockThread can acquire Lockable objects. 
    
    Lockable object can be acquired in SHARED or EXCLUSIVE mode. When acquired,
    the lock is owned by a the LockThread. SHARED state can have multiple
    owners all acquired the lock with SHARED mode. EXCLUSIVE will have only one
    onwer. 

    LockThread that cannot be granted the object are blocked. Blocked
    LockThreads wait in a blocked queue until some owner release the lock. 
    
    The Lockable object is reentrant, i.e., an owner acquiring the object has
    no effect when it has already have been granted the object; releasing the
    object multiple times also has no effect.

    """
    #class methods and variables
    UNLOCKED, SHARED, EXCLUSIVE = range(3)
    STATESTRS = ['UN', 'SH', 'EX']
    WakeupAlgo = FCFSAlgo

    def __init__(self, ID):
        IDable.__init__(self, ID)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.owners = set([])               #owners
        self.blockQueue = []                #blocked queue: [thread]
        self.blockedThreads = {}            #block thread info: {thread : (state, event)}
        self.state = Lockable.UNLOCKED

    def __repr__(self):
        return ('ID=%s [%s] o(%s) b(%s)'
                %(self.ID, Lockable.STATESTRS[self.state],
                  ' '.join([o.ID for o in self.owners]),
                  ' '.join(['%s:%s' %(b.ID,
                                      Lockable.STATESTRS[
                                          self.blockedThreads[b][0]])
                            for b in self.blockQueue])))
    def getOwners(self):
        return set(self.owners)

    def getBlockQueue(self):
        return list(self.blockQueue)

    def isUnlocked(self):
        return self.state == Lockable.UNLOCKED

    def isShared(self):
        return self.state == Lock.SHARED

    def isExclusive(self):
        return self.state == Lock.EXCLUSIVE

    def isLockedBy(self, thread):
        return thread in self.owners

    def isState(self, state):
        return self.state == state

    def tryAcquire(self, thread, state):
        """Grant self to the thread."""
        if thread in self.owners:
            #reentrancy
            assert self.state != Lockable.UNLOCKED, str(self)
            if self.state == Lockable.EXCLUSIVE:
                assert len(self.owners) == 1, ' '.join([o in self.owners])
                self.logger.debug(
                    '%s already has lockable %r' %(thread.ID, self))
                return True
            elif self.state == Lockable.SHARED and state == Lockable.SHARED:
                self.logger.debug(
                    '%s already has lockable %r' %(thread.ID, self))
                return True
            elif self.state == Lockable.SHARED and state == Lockable.EXCLUSIVE and \
                    len(self.owners) == 1:
                self.logger.debug(
                    '%s prmote lockable %r' %(thread.ID, self))
                self.state = state
                return True
            else:
                #self is SHARED, state is EXCLUSIVE but there are other shared owners
                return False
        elif self.state == Lockable.UNLOCKED:
            self.state = state
            self.owners.add(thread)
            self.logger.debug(
                '%s acquired lockable %s from %s to %s'
                %(thread.ID, self.ID, Lockable.STATESTRS[Lockable.UNLOCKED],
                  Lockable.STATESTRS[state]))
            return True
        elif self.state == Lockable.SHARED:
            if state == Lockable.SHARED:
                self.state = state
                self.owners.add(thread)
                self.logger.debug(
                    '%s acquired lockable %s from %s to %s'
                    %(thread.ID, self.ID, Lockable.STATESTRS[Lockable.SHARED],
                      Lockable.STATESTRS[state]))
                return True
        return False

    def release(self, thread):
        """Release self to from thread."""
        if thread not in self.owners:
            self.logger.debug('%s not in lockable %s owner set' 
                              %(thread.ID, self.ID))
            return
        self.owners.remove(thread)
        assert self.state != Lockable.UNLOCKED, str(self)
        if self.state == Lockable.EXCLUSIVE:
            assert len(self.owners) == 0, \
                    ('Lockable: %s, owners: %s' 
                     %(self.ID, ' '.join([o.ID for o in self.owners])))
            self.state = Lockable.UNLOCKED
        #self.state == SHARED
        elif len(self.owners) == 0:
            self.state = Lockable.UNLOCKED
        #wake up blocked thread(s)
        #   To prevent deadlock/livelock, if we still have one owner in the
        #   queue and the owner thread is also blocked(this is possible because
        #   a thread wanted to promote from shared mode to exclusive but failed
        #   because other shared owners), we wake up the owner.
        if len(self.owners) > 1:
            return
        if len(self.owners) == 1:
            owner = iter(self.owners).next()
            if owner in self.blockedThreads:
                state, event = self.blockedThreads[owner]
                assert self.state == Lockable.SHARED and \
                        state == Lockable.EXCLUSIVE, \
                        ('Lockable %s and owner blocked with state %s' 
                         %(self, Lockable.STATESTRS[state]))
                self.logger.debug('%s wake up self for promotion on Lockable %s'
                                  %(owner.ID, self.ID))
                event.signal()
                #promote state to exclusive
                self.state = Lockable.EXCLUSIVE
                self.blockQueue.remove(owner)
                del self.blockedThreads[owner]
        else:
            threads, state = Lockable.WakeupAlgo(self)
            if threads != None:
                self.state = state
                for thread in threads:
                    self.blockQueue.remove(thread)
                    del self.blockedThreads[thread]
                    self.owners.add(thread)

    def block(self, thread, state):
        """Block a thread for this object."""
        assert thread not in self.blockedThreads, \
                '%s, %s, (%s)' %(self.ID, thread.ID, ','.join(
                    [str(t.ID) for t in self.blockedThreads]))
        event = SimEvent()
        self.blockQueue.append(thread)
        self.blockedThreads[thread] = (state, event)
        return event

    def ensureOwnersAlive(self):
        for owner in self.owners:
            assert not owner.isFinished()

class LockThread(IDable, BThread):
    LOCK_BLOCK_KEY = "lock.blocked"
    LOCK_BLOCK_HEIGHT_KEY = "lock.blocked.height"
    LOCK_BLOCK_WIDTH_KEY = "lock.blocked.width"
    def __init__(self, ID):
        IDable.__init__(self, ID)
        BThread.__init__(self)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.monitor = Profiler.getMonitor(self.ID)

    def lock(self, lockable, state, timeout=infinite):
        self.logger.debug(
            '%s lock %r at %s' %(self.ID, lockable, now()))
        #try acquire the lockable
        acquired = lockable.tryAcquire(self, state)
        if acquired:
            self.logger.debug(
                '%s acquired %s at %s' %(self.ID, lockable, now()))
            #notify deadlock detection
            self.acquired(lockable)
            return
        #cannot acquire, block until timeout
        self.logger.debug(
            '%s "blocked" on %r at %s' %(self.ID, lockable, now()))
        # here we first ensure all owners who blocked us are all alive
        # and no deadlock
        lockable.ensureOwnersAlive()
        self.tryWait(lockable)
        # we pass the tests, now we wait
        timeoutEvt = Alarm.setOnetime(timeout)
        self.monitor.start(LockThread.LOCK_BLOCK_KEY)
        blockEvt = lockable.block(self, state)
        self.monitor.observe(LockThread.LOCK_BLOCK_HEIGHT_KEY, self.height)
        self.monitor.observe(LockThread.LOCK_BLOCK_WIDTH_KEY, self.width)
        yield waitevent, self, (blockEvt, timeoutEvt)
        # we are waked up
        self.endWait(lockable)
        self.monitor.stop(LockThread.LOCK_BLOCK_KEY)
        if timeoutEvt in self.eventsFired:
            self.logger.debug(
                '%s "timedout" on %r at %s' %(self.ID, lockable, now()))
            raise TimeoutException(lockable, Lockable.STATESTRS[state])
        else:
            #we are already the owner of the lock
            assert lockable.isLockedBy(self) and lockable.isState(state), \
                    ('%s waked up but is not the owner of %r with state %s'
                     %(self.ID, lockable, Lockable.STATESTRS[state]))
            self.logger.debug(
                '%s acquired %r after wait at %s' %(self.ID, lockable, now()))
            #notify deadlock detection
            self.acquired(lockable)

    def unlock(self, lockable):
        self.logger.debug(
            '%s unlock %s at %s' %(self.ID, lockable.ID, now()))
        lockable.release(self)
        yield hold, self
        self.released(lockable)

#####  TEST  #####

#####  TEST LOCKING  #####
TOTAL_FLOW = 120
FLOW_CAP = 120
OP_INTVL_MAX = 20
NUM_TANKS = 6
OPERATION_TIME = 100000
NUM_FLOWERS = 10
NUM_CHECKERS = 3

numFlowTxns = 0
numCheckTxns = 0
numUpFlowTxns = 0
numAbortedFlowTxns = 0
numAbortedCheckTxns = 0
numAbortedUpFlowTxns = 0

class Tank(Lockable):
    def __init__(self, ID):
        Lockable.__init__(self, 'tank%s'%ID)
        self.value = 0

class Flow(LockThread):
    def __init__(self, ID, tanks, nodeadlock=True):
        LockThread.__init__(self, 'flow%s'%ID)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.tanks = tanks
        self.nodeadlock = True

    def run(self):
        """Repeatitively flow from some upper tank to lower tank."""
        while now() < OPERATION_TIME:
            #sleep
            yield hold, self, random.randint(1, 5 * OP_INTVL_MAX)
            self.monitor.start('txn')
            self.logger.debug('%s txn start at %s' %(self.ID, now()))
            #pick two tanks
            first = random.randint(0, len(self.tanks) - 2)
            while True:
                if first == len(self.tanks) - 2:
                    break
                if self.tanks[first].value != 0:
                    break
                first = random.randint(first + 1, len(self.tanks) - 2)
            second = random.randint(first + 1, len(self.tanks) - 1)
            first = self.tanks[first]
            second = self.tanks[second]
            if first.value == 0:
                self.monitor.stop('txn')
                self.logger.debug('%s txn stop at %s' %(self.ID, now()))
                continue
            #a large try clause for the transaction
            try:
                #lock tanks
                for tank in (first, second):
                    for step in self.lock(tank, Lockable.EXCLUSIVE):
                        yield step
                    yield hold, self, random.randint(1, OP_INTVL_MAX)
                #lock again for reentrancy test
                for tank in (first, second):
                    for step in self.lock(tank, Lockable.EXCLUSIVE):
                        yield step
                    yield hold, self, random.randint(1, OP_INTVL_MAX)
                #for deadlock test we randomly grab some other locks
                if not self.nodeadlock:
                    for tank in self.tanks:
                        r = random.random()
                        if r < 0.3:
                            continue
                        for step in self.lock(tank, Lockable.EXCLUSIVE):
                            yield step
                        yield hold, self, random.randint(1, OP_INTVL_MAX)
                #flow
                flow = random.randint(0, FLOW_CAP)
                flow = min(first.value, flow)
                first.value -= flow
                yield hold, self, random.randint(1, 20 * OP_INTVL_MAX)
                second.value += flow
                self.logger.debug('%s flow %s from %s to %s at %s'
                                  %(self.ID, flow, first.ID, second.ID, now()))
            except BThread.DeadlockException as e:
                self.logger.debug('%s aborted because of deadlock %s at %s'
                                  %(self.ID, str(e), now()))
                global numAbortedFlowTxns
                numAbortedFlowTxns += 1
            finally:
                #unlock
                for tank in (first, second):
                    for step in self.unlock(tank):
                        yield step
                    yield hold, self, random.randint(1, OP_INTVL_MAX)
                global numFlowTxns
            numFlowTxns += 1
            self.monitor.stop('txn')
            self.logger.debug('%s txn stop at %s' %(self.ID, now()))

class Checker(LockThread):
    def __init__(self, ID, tanks, flowUp=False):
        LockThread.__init__(self, 'checker%s'%ID)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.tanks = tanks
        self.flowUp = flowUp

    def run(self):
        """Repeatitively check consistency on the tanks."""
        while now() < OPERATION_TIME:
            #sleep
            yield hold, self, random.randint(1, 10 * OP_INTVL_MAX)
            self.monitor.start('txn')
            self.logger.debug('%s txn start at %s' %(self.ID, now()))
            #a large try clause for the transaction
            try:
                #lock
                for tank in self.tanks:
                    for step in self.lock(tank, Lockable.SHARED):
                        yield step
                    yield hold, self, random.randint(1, OP_INTVL_MAX)
                #check
                tanks = []
                for tank in self.tanks:
                    tanks.append(tank)
                    yield hold, self, random.randint(1, 2 * OP_INTVL_MAX)
                verifyTanks(tanks)
                #flow up
                if self.flowUp and self.tanks[-1].value == TOTAL_FLOW:
                    global numUpFlowTxns
                    numUpFlowTxns += 1
                    for tank in (self.tanks[0], self.tanks[-1]):
                        for step in self.lock(tank, Lockable.EXCLUSIVE):
                            yield step
                        yield hold, self, random.randint(1, OP_INTVL_MAX)
                    assert self.tanks[-1].value == TOTAL_FLOW
                    self.tanks[0].value = TOTAL_FLOW
                    yield hold, self, random.randint(1, 5 * OP_INTVL_MAX)
                    self.tanks[-1].value = 0
                    self.logger.debug('%s up flow at %s' %(self.ID, now()))
            except BThread.DeadlockException as e:
                self.logger.debug('%s aborted because of deadlock %s at %s'
                                  %(self.ID, str(e), now()))
                global numAbortedCheckTxns
                global numAbortedUpFlowTxns
                numAbortedCheckTxns += 1
                if self.flowUp and self.tanks[-1].value == TOTAL_FLOW:
                    numAbortedUpFlowTxns += 1
            finally:
                #unlock
                for tank in self.tanks:
                    for step in self.unlock(tank):
                        yield step
                    yield hold, self, random.randint(1, OP_INTVL_MAX)
            global numCheckTxns
            numCheckTxns += 1
            self.monitor.stop('txn')
            self.logger.debug('%s txn stop at %s' %(self.ID, now()))

def verifyTanks(tanks):
    total = 0
    for tank in tanks:
        total += tank.value
    assert total == TOTAL_FLOW, \
            'total=%s, expected=%s'%(total, TOTAL_FLOW)

def verifyThreads(threads):
    for thread in threads:
        assert thread.isFinished(), \
                '%s is not finished, possibly deadlock' %thread.ID

def test(nodeadlock=True):
    logging.basicConfig(level=logging.DEBUG)
    initialize()
    tanks = []
    threads = []
    for i in range(NUM_TANKS):
        tank = Tank(i)
        tank.value = TOTAL_FLOW / NUM_TANKS
        tanks.append(tank)
    tanks[0].value += (TOTAL_FLOW - TOTAL_FLOW / NUM_TANKS * NUM_TANKS)
    for i in range(NUM_FLOWERS):
        flow = Flow(i, tanks, nodeadlock)
        flow.start()
        threads.append(flow)
    for i in range(NUM_CHECKERS):
        if i == 0:
            checker = Checker(i, tanks, True)
        elif i == 1:
            checker = Checker(i, tanks, not nodeadlock)
        else:
            checker = Checker(i, tanks, False)
        checker.start()
        threads.append(checker)
    simulate(until=10 * OPERATION_TIME)
    verifyTanks(tanks)
    verifyThreads(threads)
    print 'TEST PASSED'
    print ('numFlowTxns=%s, numCheckTxns=%s, numUpFlowTxns=%s'
           %(numFlowTxns, numCheckTxns, numUpFlowTxns))
    print ('numAbortedFlowTxns=%s, numAbortedCheckTxns=%s, numAbortedUpFlowTxns=%s'
           %(numAbortedFlowTxns, numAbortedCheckTxns, numAbortedUpFlowTxns))
    for thread in threads:
        runTime, std, histo, count = \
                thread.monitor.getElapsedStats('.*txn')
        waitTime, std, histo, count = \
                thread.monitor.getElapsedStats('.*%s'%LockThread.LOCK_BLOCK_KEY)
        print '%s runtime=%s, waittime=%s' %(thread.ID, runTime, waitTime)

def main():
    if len(sys.argv) != 2:
        print 'locking.py <test target>'
        print '  test target:'
        print '    locking'
        print '    deadlock'
        sys.exit()
    target = sys.argv[1]
    if target == 'locking':
        test(True)
    elif target == 'deadlock':
        test(False)
    else:
        print 'locking.py <test target>'
        print '  test target:'
        print '    locking'
        print '    deadlock'

if __name__ == '__main__':
    main()
