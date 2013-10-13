
from SimPy.Simulation import waitevent, now

from sim.core import infinite
from sim.impl.cdetmn import CentralDetmnSystem, CDSNode
from sim.paxos import initPaxosCluster
from sim.perf import Profiler
from sim.system import ClientNode, StorageNode

class SLPaxosDetmnSystem(CentralDetmnSystem):
    """Deterministic system with master timestamp assignment."""
    def newClientNode(self, idx, configs):
        return SPDCNode(self, idx, configs)

    def newStorageNode(self, cnode, index, configs):
        return SPDSNode(cnode, index, configs)

    def startupPaxos(self):
        initPaxosCluster(
            self.cnodes, self.cnodes, False, False, 'one',
            True, False, infinite)

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
        numCol = rootMon.getObservedCount('.*has_collision')
        numNCol = rootMon.getObservedCount('.*no_collision')
        self.logger.info('num.has.collision=%s'%numCol)
        self.logger.info('num.no.collision=%s'%numNCol)
        if numCol + numNCol != 0:
            self.logger.info('collision.ratio=%s'%(float(numCol) / (numCol + numNCol)))
        mean, std, histo, count = rootMon.getObservedStats('.*master.arrival.interval')
        self.logger.info('master.arrival.interval.mean=%s'%mean)
        self.logger.info('master.arrival.interval.std=%s'%std)

class SPDCNode(ClientNode):
    def __init__(self, system, ID, configs):
        ClientNode.__init__(self, system, ID, configs)
        self.zoneID = ID

    def onTxnArriveMaster(self, txn):
        self.txnsRunning.add(txn)
        self.dispatchTxn(txn)

    def onTxnArrive(self, txn):
        self.system.onTxnArrive(txn)
        if self == self.system.cnodes[0]:
            self.onTxnArriveMaster(txn)
        else:
            self.invoke(self.system.cnodes[0].onTxnArriveMaster, txn).rtiCall()

    def onTxnDepartMaster(self, txn):
        self.txnsRunning.remove(txn)

    def onTxnDepart(self, txn):
        if self == self.system.cnodes[0]:
            self.onTxnDepartMaster(txn)
        if txn.zoneID == self.zoneID:
            self.system.onTxnDepart(txn)

class SPDSNode(CDSNode):
    def __init__(self, cnode, index, configs):
        CDSNode.__init__(self, cnode, index, configs)
        self.nextIID = 0

    def run(self):
        proposingTxns = set([])
        prev = 0
        while True:
            #handle new transaction
            while len(self.newTxns) > 0:
                txn = self.newTxns.pop(0)
                #propose the txn for instance
                self.monitor.start('order.consensus.%s'%txn)
                proposingTxns.add(txn)
                self.cnode.paxosPRunner.addRequest(txn)
                #the arrive interval should be exponential distribution
                #curr = now()
                #interval = curr - prev
                #self.monitor.observe('master.arrival.interval', interval)
                #prev = curr
                #assert len(self.newTxns) == 0
            #handle new instance
            instances = self.cnode.paxosLearner.instances
            while self.nextIID in instances:
                readyTxn = instances[self.nextIID]
                if readyTxn in proposingTxns:
                    self.monitor.stop('order.consensus.%s'%readyTxn)
                    proposingTxns.remove(readyTxn)
                self.logger.debug('%s ready to start runner for %s'
                                  %(self.ID, readyTxn))
                self.lockingQueue.append(readyTxn)
                thread = StorageNode.TxnStarter(self, readyTxn)
                thread.start()
                self.nextIID += 1
            #wait for new event
            yield waitevent, self, \
                    (self.newTxnEvent, self.cnode.paxosLearner.newInstanceEvent)

