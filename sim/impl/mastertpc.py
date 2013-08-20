import logging

from SimPy.Simulation import now, activate, stopSimulation
from SimPy.Simulation import waitevent, hold
from SimPy.Simulation import Process, SimEvent

from core import Thread
from rti import MsgXeiver
from txns import TxnRunner
from system import BaseSystem, ClientNode, StorageNode

from impl.tpc import TPCTxnRunner, TPLProxy

class MasterTPCSystem(BaseSystem):
    """Deterministic replication system."""
    def newClientNode(self, idx, configs):
        return MTPCCNode(self, idx, configs)

    def newStorageNode(self, cnode, index, configs):
        return MTPCSNode(cnode, index, configs)

class MTPCCNode(ClientNode):
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

class MTPCSNode(StorageNode):
    def __init__(self, cnode, index, configs):
        StorageNode.__init__(self, cnode, index, configs)
        self.proxies = {}       #{txn : proxy}

    def newTxnRunner(self, txn):
        return MTPCTxnRunner(self, txn)

    def createFollower(self, txn):
        follower = MTPCFollower(self, txn)
        follower.start()
        return follower

class MTPCTxnRunner(TPCTxnRunner):
    def __init__(self, snode, txn):
        TPCTxnRunner.__init__(self, snode, txn)

    def getSnodeWritesets(self):
        writesets = {}
        cnode = self.snode.cnode
        for itemID, value in self.writeset.iteritems():
            snode = cnode.groupLocations[itemID.gid]
            if snode not in writesets:
                writesets[snode] = {}
            writesets[snode][itemID] = value
        return writesets

    def getSnodes(self):
        snodes = set([])
        cnode = self.snode.cnode
        for itemID in self.writeset:
            snode = cnode.groupLocations[itemID.gid]
            snodes.add(snode)
        return snodes

    def getRepSnodeWritesets(self):
        writesets = set([])
        for cnode in self.snode.system.cnodes[1:]:
            for itemID, value in self.writeset.iteritems():
                snode = cnode.groupLocations[itemID.gid]
                if snode not in writesets:
                    writesets[snode] = set([])
                writesets[snode][itemID] = value
        return writesets

    def commit(self):
        #commit the proxies
        for step in TPCTxnRunner.commit(self):
            yield step
        #commit on other replicas
        followers = set([])
        for snode, writeset in self.getRepSnodeWritesets().iteritems():
            follower = snode.createFollower(self.txn)
            self.sendMsg(follower, 'write', (self, writeset, self.ts))
            followers.add(follower)
        #wait until majority response
        total = len(followers)
        nmin = total - (total / 2 + 1)
        while len(followers) < nmin:
            for step in self.waitMsg('write'):
                yield step
            for follower in self.popContents('write'):
                follwers.remove(follower)

class MTPCFollower(Thread, MsgXeiver):
    def __init__(self, snode, txn):
        Thread.__init__(self)
        MsgXeiver.__init__(self, snode.ID)
        self.snode = snode
        self.txn = txn

    def run(self):
        for step in self.waitMsg('write'):
            yield step
        for content in self.popContents('write'):
            #apply thomas write rule
            runner, writeset, ts = content
            for itemID, value in writeset.iteritems():
                item = self.snode.groups[itemID.gid][itemID]
                if item.version < ts:
                    item.write(value, ts)
                    yield hold, self, RandInterval.get(*self.txn.config.get(
                        'commit.intvl.dist', ('fix', 0)))
            self.sendMsg(runner, 'write', self)

