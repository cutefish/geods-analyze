import logging

from SimPy.Simulation import now, activate, stopSimulation
from SimPy.Simulation import waitevent, hold
from SimPy.Simulation import Process, SimEvent

from core import Thread, infinite
from rand import RandInterval
from rti import RTI, MsgXeiver
from txns import Action, TxnRunner
from system import BaseSystem, ClientNode, StorageNode

from paxos import initPaxosCluster
from impl.cdylock import DLTxnRunner

class MasterDyLockSystem(BaseSystem):
    """Deterministic replication system."""
    def newClientNode(self, idx, configs):
        return MDLCNode(self, idx, configs)

    def newStorageNode(self, cnode, index, configs):
        return MDLSNode(cnode, index, configs)

    def startupPaxos(self):
        initPaxosCluster(
            self.cnodes, self.cnodes, False, False, True, False, infinite)

class MDLCNode(ClientNode):
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

class MDLSNode(StorageNode):
    def __init__(self, cnode, index, configs):
        StorageNode.__init__(self, cnode, index, configs)
        self.zoneID = cnode.zoneID
        self.committer = Committer(cnode, self)

    def newTxnRunner(self, txn):
        return MDLTxnRunner(self, txn)

    def run(self):
        if self.zoneID == 0:
            for step in StorageNode.run(self):
                yield step
        else:
            self.committer.start()

class Committer(Thread, RTI):
    def __init__(self, cnode, snode):
        Thread.__init__(self)
        RTI.__init__(self, snode.ID)
        self.cnode = cnode
        self.snode = snode
        self.nextUpdateIID = 0

    def run(self):
        while True:
            instances = self.cnode.paxosLearner.instances
            while self.nextUpdateIID in instances:
                txn = instances[self.nextUpdateIID]
                #write values
                for action in txn.actions:
                    if action.label == Action.READ:
                        continue
                    itemID = action.itemID
                    value = action.attr
                    item = self.snode.groups[itemID.gid][itemID]
                    item.write(value)
                    yield hold, self, RandInterval.get(*txn.config.get(
                        'commit.intvl.dist', ('fix', 0)))
                #report txn done
                self.invoke(self.cnode.onTxnDepart, txn).rtiCall()
                self.nextUpdateIID += 1
            yield waitevent, self, self.cnode.paxosLearner.newInstanceEvent

class MDLTxnRunner(DLTxnRunner):
    def __init__(self, snode, txn):
        DLTxnRunner.__init__(self, snode, txn)

    def commit(self):
        #propose to the paxos agents
        response = self.snode.cnode.paxosPRunner.addRequest(self.txn)
        yield waitevent, self, response.finishedEvent
        #majority knows about this transaction
        #commit on local
        for step in DLTxnRunner.commit(self):
            yield step
