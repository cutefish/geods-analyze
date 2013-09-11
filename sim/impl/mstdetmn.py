
import logging

from SimPy.Simulation import now, activate, stopSimulation
from SimPy.Simulation import waitevent, hold
from SimPy.Simulation import Process, SimEvent

from core import Thread
from rti import MsgXeiver
from txns import TxnRunner
from system import BaseSystem, ClientNode, StorageNode

class MasterDetmnSystem(BaseSystem):
    """Deterministic system with master timestamp assignment."""
    def newClientNode(self, idx, configs):
        return MDCNode(self, idx, configs)

    def newStorageNode(self, cnode, index, configs):
        return MDSNode(cnode, index, configs)

class MDCNode(ClientNode):
    def __init__(self, system, ID, configs):
        ClientNode.__init__(self, system, ID, configs)

    def _onTxnArrive(self, txn):
        waitIfBusy = self.configs.get('txn.wait.if.snodes.busy', False)
        if len(self.runningTxns) < self.maxNumTxns:
            self.system.onTxnArrive(txn)
            self.runningTxns.add(txn)
            for snode in self.snodes:
                self.invoke(snode.onTxnArrive, txn).rtiCall()
        else:
            self.system.onTxnLoss(txn)

    def onTxnArrive(self, txn):
        if self == self.system.cnodes[0]
            self._onTxnArrive(txn)
        else:
            self.invoke(self.system.cnodes[0]._onTxnArrive, txn).rtiCall()

class MDSnode(DRSNode):
    pass
