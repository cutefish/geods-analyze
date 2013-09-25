
import logging

from SimPy.Simulation import now, activate, stopSimulation
from SimPy.Simulation import waitevent, hold
from SimPy.Simulation import Process, SimEvent

from core import infinite
from paxos import initPaxosCluster
from system import BaseSystem, ClientNode, StorageNode

from impl.slpdetmn import SLPaxosDetmnSystem, SPDSNode

class FPaxosDetmnSystem(SLPaxosDetmnSystem):
    """Deterministic system with master timestamp assignment."""
    def newClientNode(self, idx, configs):
        return FPDCNode(self, idx, configs)

    def newStorageNode(self, cnode, index, configs):
        return FPDSNode(cnode, index, configs)

    def startupPaxos(self):
        coordinatedRecovery = self.configs.get(
            'fast.paxos.coordinated.recovery', False)
        initPaxosCluster(
            self.cnodes, self.cnodes, coordinatedRecovery, True, 'all',
            False, False, infinite)

class FPDCNode(ClientNode):
    pass

class FPDSNode(SPDSNode):
    pass
