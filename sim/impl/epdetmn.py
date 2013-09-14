import logging
import random

from SimPy.Simulation import now, activate, stopSimulation
from SimPy.Simulation import waitevent, hold
from SimPy.Simulation import Process, SimEvent

from core import Alarm, IDable, infinite
from system import BaseSystem, ClientNode, StorageNode

from paxos import initPaxosCluster
from impl.cdetmn import CDSNode, DETxnRunner

class EPaxosDetmnSystem(BaseSystem):
    """Deterministic system with master timestamp assignment."""
    def newClientNode(self, idx, configs):
        return EPDCNode(self, idx, configs)

    def newStorageNode(self, cnode, index, configs):
        return EPDSNode(cnode, index, configs)

    def startupPaxos(self):
        initPaxosCluster(
            self.cnodes, self.cnodes, False, False, False, True, infinite)

class EPDCNode(ClientNode):
    pass

class Batch(IDable):
    def __init__(self, ID):
        IDable.__init__(self, ID)
        self.batch = []

    def append(self, txn):
        self.batch.append(txn)

    def __iter__(self):
        for txn in self.batch:
            yield txn

class EPDSNode(CDSNode):
    def __init__(self, cnode, index, configs):
        CDSNode.__init__(self, cnode, index, configs)
        self.nextIID = 0
        self.eLen = self.configs['epdetmn.epoch.length']
        mu = self.configs.get('epdetmn.epoch.skew.mu', 0)
        sigma = self.configs.get('epdetmn.epoch.skew.sigma', 0)
        self.skew = random.normalvariate(mu, sigma)

    def run(self):
        yield hold, self, 10 * self.eLen + self.skew
        periodEvent = Alarm.setPeriodic(self.eLen)
        lastEpochTime = -1
        count = 0
        while True:
            #handle batch transaction event
            if now() > lastEpochTime + self.eLen:
                batch = Batch('%s-%s'%(self, count))
                while len(self.newTxns) > 0:
                    txn = self.newTxns.pop()
                    batch.append(txn)
                #propose the txn for instance
                self.cnode.paxosPRunner.addRequest(batch)
                lastEpochTime = now()
                count += 1
            #handle new instance
            instances = self.cnode.paxosLearner.instances
            while self.nextIID in instances:
                readyBatch = instances[self.nextIID]
                for txn in readyBatch:
                    self.lockingQueue.append(txn)
                    thread = StorageNode.TxnStarter(self, txn)
                    thread.start()
            #wait for new event
            yield waitevent, self, \
                    (periodEvent, self.cnode.paxosLearner.newInstanceEvent)

