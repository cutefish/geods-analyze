
from sim.core import infinite
from sim.impl.slpdetmn import SLPaxosDetmnSystem, SPDSNode
from sim.paxos import initPaxosCluster
from sim.system import ClientNode

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
