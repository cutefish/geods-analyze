import random

from SimPy.Simulation import hold
from SimPy.Simulation import now

class IIDLatencyNetwork(object):
    """A network with iid latency distribution.

    Inet addresses are in the form of 'zone%s/ID'. All addresses with the same
    'zone%s' part have within-zone latency; otherwise, its cross-zone latency.

    """
    WITHIN_KEY_PREFIX = 'nw.latency.within.zone'
    CROSS_KEY_PREFIX = 'nw.latency.cross.zone'
    def __init__(self, configs):
        self.configs = configs

    def getWithinZoneLatency(self):
        raise NotImplementedError

    def getCrossZoneLatency(self):
        raise NotImplementedError

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


class FixedLatencyNetwork(IIDLatencyNetwork):
    """A network with fixed latency."""
    FIXED_KEY = 'fixed'
    def __init__(self, configs):
        IIDLatencyNetwork.__init__(self, configs)
        self.withinLatency = configs[
            '%s.%s'%(IIDLatencyNetwork.WITHIN_KEY_PREFIX,
                     FixedLatencyNetwork.FIXED_KEY)]
        self.crossZoneLatency = configs[
            '%s.%s'%(IIDLatencyNetwork.WITHIN_KEY_PREFIX,
                     FixedLatencyNetwork.FIXED_KEY)]

    def getWithinZoneLatency(self):
        return self.withinLatency

    def getCrossZoneLatency(self):
        return self.crossZoneLatency

class NormLatencyNetwork(FixedLatencyNetwork):
    WITHIN_ZONE_LATENCY_MU_KEY = 'norm.latency.nw.within.zone.mu'
    WITHIN_ZONE_LATENCY_SIGMA_KEY = 'norm.latency.nw.within.zone.sigma'
    WITHIN_ZONE_LATENCY_LB_KEY = 'norm.latency.nw.within.zone.lb'
    WITHIN_ZONE_LATENCY_UB_KEY = 'norm.latency.nw.within.zone.ub'
    CROSS_ZONE_LATENCY_MU_KEY = 'norm.latency.nw.cross.zone.mu'
    CROSS_ZONE_LATENCY_SIGMA_KEY = 'norm.latency.nw.cross.zone.sigma'
    CROSS_ZONE_LATENCY_LB_KEY = 'norm.latency.nw.cross.zone.lb'
    CROSS_ZONE_LATENCY_UB_KEY = 'norm.latency.nw.cross.zone.ub'
    def __init__(self, configs):
        self.withinMu = \
                configs[NormLatencyNetwork.WITHIN_ZONE_LATENCY_MU_KEY]
        self.withinSigma = \
                configs[NormLatencyNetwork.WITHIN_ZONE_LATENCY_SIGMA_KEY]
        self.withinLB = \
                configs[NormLatencyNetwork.WITHIN_ZONE_LATENCY_LB_KEY]
        self.withinUB = \
                configs[NormLatencyNetwork.WITHIN_ZONE_LATENCY_UB_KEY]
        self.crossMu = \
                configs[NormLatencyNetwork.CROSS_ZONE_LATENCY_MU_KEY]
        self.crossSigma = \
                configs[NormLatencyNetwork.CROSS_ZONE_LATENCY_SIGMA_KEY]
        self.crossLB = \
                configs[NormLatencyNetwork.CROSS_ZONE_LATENCY_LB_KEY]
        self.crossUB = \
                configs[NormLatencyNetwork.CROSS_ZONE_LATENCY_UB_KEY]

    def getLatency(self, src, dst):
        if src == dst:
            return 0
        srcZone = self.getZone(src)
        dstZone = self.getZone(dst)
        if srcZone == dstZone:
            l = random.normalvariate(self.withinMu, self.withinSigma)
            l = self.withinLB if l < self.withinLB else l
            l = self.withinUB if l > self.withinUB else l
            return l
        else:
            l = random.normalvariate(self.crossMu, self.crossSigma)
            l = self.crossLB if l < self.crossLB else l
            l = self.crossUB if l > self.crossUB else l
            return l

class UniformLatencyNetwork(FixedLatencyNetwork):
    WITHIN_ZONE_LATENCY_LB_KEY = 'uniform.latency.nw.within.zone.lb'
    WITHIN_ZONE_LATENCY_UB_KEY = 'uniform.latency.nw.within.zone.ub'
    CROSS_ZONE_LATENCY_LB_KEY = 'uniform.latency.nw.cross.zone.lb'
    CROSS_ZONE_LATENCY_UB_KEY = 'uniform.latency.nw.cross.zone.ub'
    def __init__(self, configs):
        self.withinLB = \
                configs[UniformLatencyNetwork.WITHIN_ZONE_LATENCY_LB_KEY]
        self.withinUB = \
                configs[UniformLatencyNetwork.WITHIN_ZONE_LATENCY_UB_KEY]
        self.crossLB = \
                configs[UniformLatencyNetwork.CROSS_ZONE_LATENCY_LB_KEY]
        self.crossUB = \
                configs[UniformLatencyNetwork.CROSS_ZONE_LATENCY_UB_KEY]

    def getLatency(self, src, dst):
        if src == dst:
            return 0
        srcZone = self.getZone(src)
        dstZone = self.getZone(dst)
        if srcZone == dstZone:
            r = random.random()
            l = self.withinLB + (self.withinUB - self.withinLB) * r
            return l
        else:
            r = random.random()
            l = self.crossLB + (self.crossUB - self.crossLB) * r
            return l



