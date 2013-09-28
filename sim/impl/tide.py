import logging
import random

from SimPy.Simulation import now, activate, stopSimulation
from SimPy.Simulation import waitevent, hold
from SimPy.Simulation import Process, SimEvent

import sim
from sim.core import Alarm, IDable, Thread
from sim.rti import MsgXeiver
from sim.system import BaseSystem, ClientNode, StorageNode

from sim.impl.cendet import CDSNode

class DeterministicReplicationSystem(BaseSystem):
    """Deterministic replication system."""
    def newClientNode(self, idx, configs):
        return DRCNode(self, idx, configs)

    def newStorageNode(self, cnode, index, configs):
        return DRSNode(cnode, index, configs)

class DRCNode(ClientNode):
    def __init__(self, system, ID, configs):
        ClientNode.__init__(self, system, ID, configs)
        self.epochLength = self.configs['tide.epoch.length']
        skew = self.calcSkew()
        self.skew = skew if skew > 0 else -skew
        self.epochQueue = []
        self.acceptor = Acceptor(self)
        self.learner = Learner(self)
        self.sysAcceptors = []
        self.sysLearners = []

    def calcSkew(self):
        mu = self.configs.get('tide.epoch.skew.mu', 0)
        sigma = self.configs.get('tide.epoch.skew.sigma', 5)
        return random.normalvariate(mu, sigma)

    def now(self):
        return now() - self.skew

    def onTxnArrive(self, txn):
        if len(self.runningTxns) < self.maxNumTxns:
            self.system.onTxnArrive(txn)
            self.runningTxns[txn] = None
            self.epochQueue.append(txn)
        else:
            self.system.onTxnLoss(txn)

    def onTxnDepart(self, txn):
        if txn not in self.runningTxns:
            return
        del self.runningTxns[txn]
        self.system.onTxnDepart(txn)

    def run(self):
        for cnode in self.system.cnodes:
            self.sysAcceptors.append(cnode.acceptor)
        for cnode in self.system.cnodes:
            self.sysLearners.append(cnode.learner)
        yield hold, self, self.skew
        self.acceptor.start()
        self.learner.start()
        timer = Alarm.setPeriodic(self.epochLength, 'epoch')
        while not self.shouldClose:
            yield waitevent, self, (self.closeEvent, timer)
            if timer in self.eventsFired:
                batch = []
                while len(self.epochQueue) > 0:
                    batch.append(self.epochQueue.pop(0))
                eid = int((self.now() + 0.01)/ self.epochLength)
                Proposer(self).propose(eid, list(batch))
                if len(batch) > 0:
                    self.logger.debug(
                        '%s proposed batch %s.%s %s at %s' 
                        %(self, self, eid,
                          '[%s]'%(', '.join([txn.ID for txn in batch])),
                          now()))
            elif self.shouldClose:
                self.acceptor.close()
                self.learner.close()
                self._close()

class Proposer(MsgXeiver):
    """Proposer of epochID.cnodeID. 
    Send a epochID.cnodeID batch to all acceptors.
    """
    def __init__(self, cnode):
        MsgXeiver.__init__(self, cnode.ID)
        self.cnode = cnode

    def propose(self, eid, batch):
        for acceptor in self.cnode.sysAcceptors:
            self.sendMsg(acceptor, 'accept', (eid, self.cnode.ID, batch))

class Acceptor(IDable, Thread, MsgXeiver):
    """Epoch replication acceptor.
    Algorithm:
        (1)
        wait for batch proposal epochID.cnodeID from proposer
        if not accepted epochID.cnodeID:
            accept
        send learners accepted value of epochID.cnodeID
        (2)
        if epochID.cnodeID timeout
            start proposer of epochID.cnodeID to propose empty batch
    """
    def __init__(self, cnode):
        IDable.__init__(self, '%s/acc'%cnode.ID)
        Thread.__init__(self)
        MsgXeiver.__init__(self, cnode.ID)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cnode = cnode
        self.accepted = {}      #{(eid, cnodeID) : batch}
        self.timeout = cnode.configs.get('tide.acceptor.epoch.timeout', 500)
        self.shouldClose = False

    def close(self):
        self.shouldClose = True

    def run(self):
        expired = 0
        epochLength = self.cnode.epochLength
        lastgc = 0
        while not self.shouldClose:
            next = (self.cnode.now() / epochLength + 1) * epochLength
            timeout = next - now()
            try:
                for step in self.waitMsg('accept', timeout):
                    yield step
            except:
                pass
            #check propose messages
            for content in self.popContents('accept'):
                eid, cnodeID, batch = content
                if len(batch) > 0:
                    self.logger.debug('%s accepted batch %s.%s %s at %s'
                                      %(self, cnodeID, eid,
                                        '[%s]'%(', '.join([txn.ID for txn in batch])),
                                        now()))
                if (eid, cnodeID) not in self.accepted:
                    self.accepted[(eid, cnodeID)] = batch
                #tell all learners 
                for learner in self.cnode.sysLearners:
                    self.sendMsg(learner, 'learn',
                                 (eid, self.cnode.ID, (cnodeID, batch)))
            #expire epoch
            if next >= now():
                newExpire = int((now() + 0.01 - self.timeout) / epochLength)
                newExpire = newExpire if newExpire >= 0 else 0
                for eid in range(expired + 1, newExpire + 1):
                    for cnode in self.cnode.system.cnodes:
                        if (eid, cnode.ID) not in self.accepted:
                            Proposer(self.cnode).propose(eid, [])
                            self.logger.debug('%s expried batch %s.%s at %s'
                                              %(self, cnode.ID, eid, now()))
                expired = newExpire
            #garbage collection
            interval = 1000000
            if now() - lastgc > interval:
                lastgc = now()
                safe = expired - interval / epochLength
                for key in self.accepted:
                    eid, cnodeID = key
                    if eid < safe:
                        del self.accepted[(eid, cnodeID)]

class Learner(IDable, Thread, MsgXeiver):
    """Epoch replication learner.

    Learns the batch of eid.cnodeID. A batch is learned if the majority has the
    same value.
    """
    def __init__(self, cnode):
        IDable.__init__(self, '%s/lnr'%cnode.ID)
        Thread.__init__(self)
        MsgXeiver.__init__(self, cnode.ID)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cnode = cnode
        self.shouldClose = False
        self.learned = {}       #{eid : {cnodeID : batch}}
        self.votes = {}         #{(eid, cnodeID) : votes}

    def close(self):
        self.shouldClose = True

    def run(self):
        self.total = len(self.cnode.system.cnodes)
        executed = 0
        while not self.shouldClose:
            for step in self.waitMsg('learn'):
                yield step
            #handling messages
            for content in self.popContents('learn'):
                eid, accID, batchinfo = content
                cnodeID, batch = batchinfo
                #ignore already executed epochs or already reach concensus
                if eid <= executed:
                    continue
                if eid in self.learned:
                    if cnodeID in self.learned[eid]:
                        continue
                #vote
                if (eid, cnodeID) not in self.votes:
                    self.votes[(eid, cnodeID)] = Votes(self.total)
                votes = self.votes[(eid, cnodeID)]
                votes.add(accID, batch)
                #check vote result
                if votes.final is not None:
                    if eid not in self.learned:
                        self.learned[eid] = {}
                    assert cnodeID not in self.learned[eid]
                    self.learned[eid][cnodeID] = batch
                    del self.votes[(eid, cnodeID)]
            #check for epochs ready for execution
            while True:
                next = executed + 1
                if next not in self.learned:
                    break
                if len(self.learned[next]) < self.total:
                    break
                #next is ready
                txns = []
                for cnodeID in sorted(self.learned[next].keys()):
                    for txn in self.learned[next][cnodeID]:
                        txns.append(txn)
                for snode in self.cnode.snodes:
                    snode.onTxnsArrive(txns)
                del self.learned[next]
                executed = next
                if len(txns) != 0:
                    self.logger.debug(
                        '%s learned %s %s at %s'
                        %(self, next,
                          '[%s]'%(', '.join([txn.ID for txn in txns])),
                          now()))

class Votes(object):
    def __init__(self, total):
        self.total = total
        self.nmaj = total / 2 + 1
        self._final = None
        self.bset = set([])
        self.batch = None
        self.eset = set([])

    def add(self, accID, batch):
        if len(batch) > 0:
            if self.batch is None:
                self.batch = batch
            else:
                assert self.batch == batch
            self.bset.add(accID)
            assert accID not in self.eset
        else:
            self.eset.add(accID)
            assert accID not in self.bset

    @property
    def final(self):
        assert len(self.bset) + len(self.eset) <= self.total, \
                ('bset:[%s], eset:[%s], total:%s'
                 %(', '.join([acc for acc in self.bset]),
                   ', '.join([acc for acc in self.eset]), self.total))
        if self._final != None:
            return self._final
        if len(self.bset) >= self.nmaj:
            self._final = self.batch
        elif len(self.eset) >= self.nmaj:
            self._final = []
        return self._final

class DRSNode(CDSNode):
    #here I directly CDSNode code, which disables reading remote data.
    #the implementation of reading remote data has a little complex:
    #   (1)the ownership of txn runnner and read item message is not clear.
    #   Read item message should live a long time since other snode will read
    #   the data; (2) the fault-tolerance of read item message is not clear.
    pass
