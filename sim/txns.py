import logging

from SimPy.Simulation import now
from SimPy.Simulation import hold, waitevent

from sim.core import IDable, BThread, TimeoutException
from sim.locking import LockThread
from sim.rand import RandInterval

class Action(object):
    READ, WRITE = ('r', 'w')
    def __init__(self, label, itemID, attr=None):
        self.label = label
        self.itemID = itemID
        self.attr = attr

    def isRead(self):
        return self.label == Action.READ

    def isWrite(self):
        return self.label == Action.WRITE

    def __str__(self):
        return '(%s %s)' %(self.label, self.itemID)

class Transaction(IDable):
    def __init__(self, ID, zoneID, actions, config):
        IDable.__init__(self, 'txn%s'%ID)
        self.zoneID = zoneID
        self.actions = actions
        self.gids = set([])
        self.config = config
        for action in actions:
            self.gids.add(action.itemID.gid)

    def __repr__(self):
        return ('%s{(%s) [%s] [%s]}'
                %(self.ID, self.zoneID,
                  ', '.join([str(g) for g in self.gids]),
                  ', '.join([str(a) for a in self.actions])))

class TxnRunner(LockThread):
    PREPARING, RUNNING, ABORTING, COMMITTING, COMMITTED, FINISHED = range(6)
    STATESTR = [
        'PREPARING', 'RUNNING', 'ABORTING', 'COMMITTING', 'COMMITTED', 'FINISHED'
    ]
    def __init__(self, snode, txn):
        LockThread.__init__(self, '%s/tr-%s' %(snode.ID, txn.ID))
        self.logger = logging.getLogger(self.__class__.__name__)
        self.snode = snode
        self.txn = txn
        self.state = None

    #states
    def Preparing(self):
        assert self.state is None, \
                '%s %s' %(self.txn, TxnRunner.STATESTR[self.state])
        self.state = TxnRunner.PREPARING
        self.monitor.start('preparing')

    def isPreparing(self):
        return self.state == TxnRunner.PREPARING

    def Running(self):
        if self.isPreparing():
            self.monitor.stop('preparing')
        elif self.isAborting():
            self.monitor.stop('aborting')
        else:
            raise ValueError(
                'Txn wrong status: %s before running' %TxnRunner.STATESTR[self.state])
        self.state = TxnRunner.RUNNING
        self.monitor.start('running')

    def isRunning(self):
        return self.state == TxnRunner.RUNNING

    def Committing(self):
        assert self.isRunning(), \
                '%s %s' %(self.txn, TxnRunner.STATESTR[self.state])
        self.state = TxnRunner.COMMITTING
        self.monitor.stop('running')
        self.monitor.start('committing')

    def isCommitting(self):
        return self.state == TxnRunner.COMMITTING

    def Aborting(self):
        if self.isRunning():
            self.monitor.stop('running')
        elif self.isCommitting():
            self.monitor.stop('committing')
        else:
            raise ValueError(
                'Txn wrong status: %s before aborting' %TxnRunner.STATESTR[self.state])
        self.state = TxnRunner.ABORTING
        self.monitor.start('aborting')

    def isAborting(self):
        return self.state == TxnRunner.ABORTING

    def Committed(self):
        assert self.state == TxnRunner.COMMITTING, \
                '%s %s' %(self.txn, TxnRunner.STATESTR[self.state])
        self.state = TxnRunner.COMMITTED
        self.monitor.stop('committing')
        self.monitor.start('committed')

    def isCommitted(self):
        return self.state == TxnRunner.COMMITTED

    def Finished(self):
        assert self.isCommitted(), \
                '%s %s' %(self.txn, TxnRunner.STATESTR[self.state])
        self.state = TxnRunner.FINISHED
        self.monitor.stop('committed')

    def isFinished(self):
        return self.state == TxnRunner.FINISHED

    #skeleton run
    def run(self):
        self.logger.debug('%s start at %s' %(self.ID, now()))
        self.Preparing()
        for step in self.prepare():
            yield step
        while True:
            try:
                #start
                self.Running()
                for step in self.begin():
                    yield step
                #read and write
                for action in self.txn.actions:
                    if action.isRead():
                        for step in self.read(action.itemID, action.attr):
                            yield step
                    else:
                        assert action.isWrite()
                        for step in self.write(action.itemID, action.attr):
                            yield step
                    #simulate the cost of each read/write step
                    yield hold, self, RandInterval.get(
                        *self.txn.config['action.intvl.dist']).next()
                #try commit
                self.Committing()
                for step in self.trycommit():
                    yield step
                #the commit phase is error free
                for step in self.commit():
                    yield step
                self.Committed()
                break
            except BThread.DeadlockException as e:
                self.logger.debug('%s aborted because of deadlock %s at %s'
                                  %(self.ID, str(e), now()))
                self.monitor.observe('deadlock.cycle.length',
                                     len(e.waiters) + 1)
                self.monitor.start('abort.deadlock')
                self.Aborting()
                for step in self.abort():
                    yield step
                #wait for one of the waiters to leave
                waitEvts = []
                for w in e.waiters:
                    waitEvts.append(w.finish)
                yield waitevent, self, waitEvts
                self.monitor.stop('abort.deadlock')
            except TimeoutException as e:
                self.monitor.observe('abort.timeout', 0)
                self.logger.debug(
                    '%s aborted because of timeout on %r with state %s'
                    %(e.args[0], e.args[1]))
        for step in self.cleanup():
            yield step
        self.Finished()

    #intermediate methods to override
    def prepare(self):
        yield hold, self

    def begin(self):
        yield hold, self

    def read(self, itemID, attr):
        yield hold, self

    def write(self, itemID, attr):
        yield hold, self

    def trycommit(self):
        yield hold, self

    def abort(self):
        yield hold, self

    def commit(self):
        yield hold, self

    def cleanup(self):
        yield hold, self

