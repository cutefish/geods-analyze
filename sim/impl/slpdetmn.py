
import logging

from SimPy.Simulation import now, activate, stopSimulation
from SimPy.Simulation import waitevent, hold
from SimPy.Simulation import Process, SimEvent

from core import Thread, infinite
from rti import MsgXeiver
from txns import TxnRunner
from system import BaseSystem, ClientNode, StorageNode

from paxos import initPaxosCluster
from impl.cdetmn import CDSNode, DETxnRunner

class SLPaxosDetmnSystem(BaseSystem):
    """Deterministic system with master timestamp assignment."""
    def newClientNode(self, idx, configs):
        return SPDCNode(self, idx, configs)

    def newStorageNode(self, cnode, index, configs):
        return SPDSNode(cnode, index, configs)

    def startupPaxos(self):
        initPaxosCluster(
            self.cnodes, self.cnodes, False, False, True, False, infinite)

class SPDCNode(ClientNode):
    def __init__(self, system, ID, configs):
        ClientNode.__init__(self, system, ID, configs)
        self.zoneID = ID

    def onTxnArriveMaster(self, txn):
        waitIfBusy = self.configs.get('txn.wait.if.snodes.busy', False)
        snode = self.dispatchTxn(txn, waitIfBusy)
        if snode:
            self.txnsRunning[txn] = snode
            self.snodeLoads[snode].add(txn)
        else:
            self.system.onTxnLoss(txn)

    def onTxnArrive(self, txn):
        self.system.onTxnArrive(txn)
        if self == self.system.cnodes[0]:
            self.onTxnArriveMaster(txn)
        else:
            self.invoke(self.system.cnodes[0].onTxnArriveMaster, txn).rtiCall()

    def onTxnDepartMaster(self, txn):
        if txn not in self.txnsRunning:
            return
        snode = self.txnsRunning[txn]
        self.snodeLoads[snode].remove(txn)
        del self.txnsRunning[txn]

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
        while True:
            #handle new transaction
            while len(self.newTxns) > 0:
                assert self.cnode.zoneID == 0, \
                        '%s got new txn, but not master'%self.ID
                txn = self.newTxns.pop()
                #propose the txn for instance
                self.cnode.paxosPRunner.addRequest(txn)
            #handle new instance
            instances = self.cnode.paxosLearner.instances
            while self.nextIID in instances:
                readyTxn = instances[self.nextIID]
                self.logger.debug('%s ready to start runner for %s'
                                  %(self.ID, readyTxn))
                self.lockingQueue.append(readyTxn)
                thread = StorageNode.TxnStarter(self, readyTxn)
                thread.start()
                self.nextIID += 1
            #wait for new event
            yield waitevent, self, \
                    (self.newTxnEvent, self.cnode.paxosLearner.newInstanceEvent)

