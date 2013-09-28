import logging
import random

from SimPy.Simulation import now, activate, stopSimulation
from SimPy.Simulation import waitevent, hold
from SimPy.Simulation import Process, SimEvent

import sim
from sim.core import Alarm, IDable, infinite
from sim.perf import Profiler
from sim.system import BaseSystem, ClientNode, StorageNode

from sim.paxos import initPaxosCluster
from sim.impl.cdetmn import CentralDetmnSystem, CDSNode, DETxnRunner

class EPaxosDetmnSystem(CentralDetmnSystem):
    """Deterministic system with master timestamp assignment."""
    def newClientNode(self, idx, configs):
        return EPDCNode(self, idx, configs)

    def newStorageNode(self, cnode, index, configs):
        return EPDSNode(cnode, index, configs)

    def startupPaxos(self):
        initPaxosCluster(
            self.cnodes, self.cnodes, False, False, 'all', 
            True, True, infinite)

    def profile(self):
        CentralDetmnSystem.profile(self)
        rootMon = Profiler.getMonitor('/')
        pmean, pstd, phisto, pcount = \
                rootMon.getElapsedStats('.*paxos.propose')
        self.logger.info('paxos.propose.time.mean=%s'%pmean)
        self.logger.info('paxos.propose.time.std=%s'%pstd)
        self.logger.info('paxos.propose.time.histo=(%s, %s)'%(phisto))

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

    def isEmpty(self):
        return len(self.batch) == 0

class EPDSNode(CDSNode):
    def __init__(self, cnode, index, configs):
        CDSNode.__init__(self, cnode, index, configs)
        self.nextIID = 0
        self.eLen = self.configs['epdetmn.epoch.length']
        mu = self.configs.get('epdetmn.epoch.skew.mu', 0)
        sigma = self.configs.get('epdetmn.epoch.skew.sigma', 0)
        self.skew = random.normalvariate(mu, sigma)
        self.gcID = 0

    def run(self):
        initTime = self.skew
        while initTime < 0:
            initTime += self.eLen
        yield hold, self, initTime
        periodEvent = Alarm.setPeriodic(self.eLen, name='epoch')
        lastEpochTime = -1
        count = 0
        lastBatch = False
        while True:
            #handle batch transaction event
            if now() > lastEpochTime + self.eLen and not lastBatch:
                batch = Batch('%s-%s'%(self, count))
                while len(self.newTxns) > 0:
                    txn = self.newTxns.pop()
                    batch.append(txn)
                #propose the txn for instance
                self.monitor.start('paxos.propose.%s'%batch)
                self.cnode.paxosPRunner.addRequest(batch)
                lastEpochTime = now()
                count += 1
                self.logger.debug('%s propose new batch %s at %s'
                                  %(self.ID, batch, now()))
                if self.shouldClose:
                    self.logger.debug('%s sending last batch at %s'
                                      %(self, now()))
                    lastBatch = True
            #handle new instance
            instances = self.cnode.paxosLearner.instances
            while self.nextIID in instances:
                readyBatch = instances[self.nextIID]
                if self.ID in readyBatch.ID:
                    self.monitor.stop('paxos.propose.%s'%readyBatch)
                if not readyBatch.isEmpty():
                    self.logger.debug('%s execute new batch %s at %s'
                                      %(self.ID, readyBatch, now()))
                for txn in readyBatch:
                    self.lockingQueue.append(txn)
                    thread = StorageNode.TxnStarter(self, txn)
                    thread.start()
                self.nextIID += 1
            #garbage collection
            if len(instances) > 1000:
                for i in range(self.gcID, self.nextIID / 2):
                    del instances[i]
                self.gcID = self.nextIID / 2
            #wait for new event
            yield waitevent, self, \
                    (periodEvent, self.cnode.paxosLearner.newInstanceEvent)

