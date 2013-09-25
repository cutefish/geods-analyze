
import logging

from SimPy.Simulation import now, activate, stopSimulation
from SimPy.Simulation import waitevent, hold
from SimPy.Simulation import Process, SimEvent

from core import Thread, infinite
from perf import Profiler
from rti import MsgXeiver
from txns import TxnRunner
from system import BaseSystem, ClientNode, StorageNode

from paxos import initPaxosCluster
from impl.cdetmn import CentralDetmnSystem, CDSNode, DETxnRunner

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
                rootMon.getElapsedStats('.*paxos.propose')
        self.logger.info('paxos.propose.time.mean=%s'%pmean)
        self.logger.info('paxos.propose.time.std=%s'%pstd)
        self.logger.info('paxos.propose.time.histo=(%s, %s)'%(phisto))

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
        while True:
            #handle new transaction
            while len(self.newTxns) > 0:
                txn = self.newTxns.pop(0)
                #propose the txn for instance
                self.monitor.start('paxos.propose.%s'%txn)
                proposingTxns.add(txn)
                self.cnode.paxosPRunner.addRequest(txn)
            #handle new instance
            instances = self.cnode.paxosLearner.instances
            while self.nextIID in instances:
                readyTxn = instances[self.nextIID]
                if readyTxn in proposingTxns:
                    self.monitor.stop('paxos.propose.%s'%readyTxn)
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

