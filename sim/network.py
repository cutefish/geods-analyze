from SimPy.Simulation import hold
from SimPy.Simulation import now

import sim
from sim.rand import RandInterval

class IIDLatencyNetwork(object):
    """A network with iid latency distribution.

    Inet addresses are in the form of 'zone%s/ID'. All addresses with the same
    'zone%s' part have within-zone latency; otherwise, its cross-zone latency.

    """
    WITHIN_KEY = 'nw.latency.within.zone'
    CROSS_KEY = 'nw.latency.cross.zone'
    def __init__(self, configs):
        self.withinGen = RandInterval.get(
            *configs[IIDLatencyNetwork.WITHIN_KEY])
        self.crossGen = RandInterval.get(
            *configs[IIDLatencyNetwork.CROSS_KEY])

    def getWithinZoneLatency(self):
        return self.withinGen.next()

    def getCrossZoneLatency(self):
        return self.crossGen.next()

    def getZone(self, addr):
        return addr.split('/')[0]

    def getLatency(self, src, dst):
        if src == dst:
            return 0
        srcZone = self.getZone(src)
        dstZone = self.getZone(dst)
        if srcZone == dstZone:
            return self.getWithinZoneLatency()
        else:
            return self.getCrossZoneLatency()

    def sendPacket(self, pemproc, src, dst, pktSize):
        latency = self.getLatency(src, dst)
        yield hold, pemproc, latency

#####  TEST  #####
def test():
    config = {
        'nw.latency.within.zone' : ('uniform', 10, {'lb' : 5, 'ub' : 15}),
        'nw.latency.cross.zone' : ('norm', 100,
                                   {'lb' : 50, 'ub' : 150, 'sigma' : 100}),
    }
    network = IIDLatencyNetwork(config)
    for i in range(10):
        print network.getLatency('zone0/sn', 'zone0/cn')
    print
    for i in range(10):
        print network.getLatency('zone1/sn', 'zone0/sn')

def main():
    test()

if __name__ == '__main__':
    main()
