import logging

from SimPy.Simulation import now, activate, stopSimulation
from SimPy.Simulation import waitevent, hold
from SimPy.Simulation import Process, SimEvent

from core import Thread
from rti import MsgXeiver
from txns import TxnRunner
from system import BaseSystem, ClientNode, StorageNode

from impl.cdylock import DLTxnRunner

class MasterDyLockSystem(BaseSystem):
    """Deterministic replication system."""
    def newClientNode(self, idx, configs):
        return MDLCNode(self, idx, configs)

    def newStorageNode(self, cnode, index, configs):
        return MDLSNode(cnode, index, configs)

class MDLCNode(ClientNode):
    def __init__(self, system, ID, configs):
        ClientNode.__init__(self, system, ID, configs)

    def _onTxnArrive(self, txn):
        waitIfBusy = self.configs.get('txn.wait.if.snodes.busy', False)
        snode = self.dispatchTxn(txn, waitIfBusy)
        if snode:
            self.system.onTxnArrive(txn)
            self.runningTxns[txn] = snode
            self.snodeLoads[snode].add(txn)
        else:
            self.system.onTxnLoss(txn)

    def onTxnArrive(self, txn):
        if self == self.system.cnodes[0]
            self._onTxnArrive(txn)
        else:
            self.invoke(self.system.cnodes[0]._onTxnArrive, txn).rtiCall()

class MDLSNode(StorageNode):
    def __init__(self, cnode, index, configs):
        StorageNode.__init__(self, cnode, index, configs)
        self.nextUpdateIID = 0
        self.index = index

    def newTxnRunner(self, txn):
        return MDLTxnRunner(self, txn)

    def run(self):
        if self.index == 0:
            for step in StorageNode.run():
                yield step
        else:
            for step in runReplicaSNode():
                yield step

    def runReplicaSNode(self):
        while True:
            instances = self.cnode.paxosLearner.instances
            while self.nextUpdateIID in instances:
                writeset = instances[self.lastUpdateIID]
                for itemID, value in writeset.iteritems():
                    item = self.snode.groups[itemID.gid][itemID]
                    item.write(value)
                    #we don't care about this performance here, so we make it
                    #atomic
                    pass
                self.nextUpdateIID += 1
            yield waitevent, self, self.cnode.paxosLearner.newInstanceEvent

class MDLTxnRunner(DLTxnRunner):
    def __init__(self, snode, txn):
        TPCTxnRunner.__init__(self, snode, txn)

    def commit(self):
        #propose to the paxos agents
        response = self.cnode.paxosPRunner.addRequest(self.writeset)
        yield waitevent, self, response.finishedEvent
        #majority knows about this transaction
        #commit on local
        for step in DLTxnRunner.commit(self):
            yield step
