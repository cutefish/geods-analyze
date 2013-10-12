import random

from SimPy.Simulation import now
from SimPy.Simulation import waitevent, hold

from rintvl import RandInterval
from sim.core import Alarm, IDable, infinite
from sim.impl.cdetmn import CentralDetmnSystem, CDSNode
from sim.paxos import initPaxosCluster
from sim.perf import Profiler
from sim.system import ClientNode, StorageNode

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
                rootMon.getElapsedStats('.*order.consensus')
        self.logger.info('order.consensus.time.mean=%s'%pmean)
        self.logger.info('order.consensus.time.std=%s'%pstd)
        #self.logger.info('order.consensus.time.histo=(%s, %s)'%(phisto))
        totalTime = rootMon.getElapsedStats('.*propose_value')
        mean, std, histo, count = totalTime
        self.logger.info('paxos.propose.total.time.mean=%s'%mean)
        self.logger.info('paxos.propose.total.time.std=%s'%std)
        #self.logger.info('paxos.propose.total.time.histo=(%s, %s)'%histo)
        #self.logger.info('paxos.propose.total.time.count=%s'%count)
        succTime = rootMon.getElapsedStats('.*_psucc')
        mean, std, histo, count = succTime
        self.logger.info('paxos.propose.succ.time.mean=%s'%mean)
        self.logger.info('paxos.propose.succ.time.std=%s'%std)
        #self.logger.info('paxos.propose.succ.time.histo=(%s, %s)'%histo)
        #self.logger.info('paxos.propose.succ.time.count=%s'%count)
        failTime = rootMon.getElapsedStats('.*_pfail')
        mean, std, histo, count = failTime
        self.logger.info('paxos.propose.fail.time.mean=%s'%mean)
        self.logger.info('paxos.propose.fail.time.std=%s'%std)
        #self.logger.info('paxos.propose.fail.time.histo=(%s, %s)'%histo)
        #self.logger.info('paxos.propose.fail.time.count=%s'%count)
        ntries = rootMon.getObservedStats('.*ntries')
        mean, std, histo, count = ntries
        self.logger.info('ntries.time.mean=%s'%mean)
        self.logger.info('ntries.time.std=%s'%std)
        #self.logger.info('ntries.time.histo=(%s, %s)'%histo)
        #self.logger.info('ntries.time.count=%s'%count)
        numCol = rootMon.getObservedCount('.*collision')
        numNCol = rootMon.getObservedCount('.*no_collision')
        self.logger.info('num.collision=%s'%numCol)
        self.logger.info('num.no.collision=%s'%numNCol)
        if numCol + numNCol != 0:
            self.logger.info('collision.ratio=%s'%(float(numCol) / (numCol + numNCol)))

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
        self.skew = self.configs['epdetmn.epoch.skew.dist']
        self.gcID = 0

    def run(self):
        periodEvent = Alarm.setPeriodic(self.eLen, name='epoch', drift=self.skew)
        lastEpochTime = 0
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
                self.monitor.start('order.consensus.%s'%batch)
                self.cnode.paxosPRunner.addRequest(batch)
                lastEpochTime += self.eLen
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
                    self.monitor.stop('order.consensus.%s'%readyBatch)
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

