import logging

from SimPy.Simulation import Process, SimEvent
from SimPy.Simulation import initialize, activate, simulate, now
from SimPy.Simulation import waitevent, hold

from core import IDable, RetVal, Thread, TimeoutException, infinite
from rti import RTI, MsgXeiver

class Proposer(IDable, Thread, MsgXeiver):
    """Paxos proposer."""
    def __init__(self, parent, ID, rnd0, rndstep, acceptors, timeout=infinite):
        IDable.__init__(self, '%s/proposer-%s'%(parent.ID, ID))
        Thread.__init__(self)
        MsgXeiver.__init__(self, parent.inetAddr)
        self.parent = parent
        self.rndstep = rndstep
        self.rnd0 = rnd0
        self.acceptors = acceptors
        self.instances = {}
        self.timeout = timeout
        self.qsize = len(self.acceptors) / 2 + 1
        self.logger = logging.getLogger(self.__class__.__name__)

    def propose(self, instanceID, value):
        self.currRnd = self.rnd0
        while True:
            try:
                pvalue = RetVal()
                for step in self._send1aMsg(instanceID):
                    yield step
                self.logger.debug('%s sent 1a message in round %s at %s'
                                  %(self.ID, self.currRnd, now()))
                for step in self._recv1bMsg(instanceID, value, pvalue):
                    yield step
                self.logger.debug('%s recv 1b message'
                                  ' with value %s in round %s at %s'
                                  %(self.ID, pvalue.get(), self.currRnd, now()))
                for step in self._send2aMsg(instanceID, pvalue):
                    yield step
                self.logger.debug('%s sent 2a message in round %s at %s'
                                  %(self.ID, self.currRnd, now()))
                for step in self._recv2bMsg(instanceID):
                    yield step
                self.logger.debug('%s recv 2b message in round %s at %s'
                                  %(self.ID, self.currRnd, now()))
                assert instanceID not in self.instances, \
                        'instances[%s] = %s'%(
                            instanceID, self.instances[instanceID])
                self.instances[instanceID] = pvalue
                break
            except TimeoutException as e:
                self.logger.info('%s starts a new round at %s. Cause: %s'
                                 %(self.ID, now(), e))
                self.currRnd += self.rndstep
        self.logger.debug('%s reach concensus for '
                          'instance=%s with value=%s at %s' 
                          %(self.ID, instanceID, pvalue.get(), now()))

    def _send1aMsg(self, instanceID):
        for acc in self.acceptors:
            self.sendMsg(acc, 'paxos 1a', (self, instanceID, self.currRnd))
        yield hold, self

    def _recv1bMsg(self, instanceID, pvalue, retval):
        messages = {}
        while True:
            for content in self.popContents('paxos %s 1b'%instanceID):
                acc, crnd, vrnd, value = content
                self.logger.debug('%s recv message: '
                                  'acc=%s, crnd=%s, vrnd=%s, value=%s '
                                  'at %s'
                                  %(self.ID, acc.ID, crnd, vrnd, value, now()))
                assert vrnd <= crnd, \
                        'vrndNo = %s <= %s = rndNo' %(vrnd, crnd)
                if crnd > self.currRnd:
                    #a new round has started on majority nodes
                    raise TimeoutException('new round', crnd)
                elif crnd == self.currRnd:
                    #add the acceptor into the response set
                    if acc in messages:
                        assert messages[acc] == (vrnd, value)
                    else:
                        messages[acc] = (vrnd, value)
                else:
                    pass
            if len(messages) >= self.qsize:
                break
            for step in self.waitMsg(
                'paxos %s 1b'%instanceID, self.timeout):
                yield step
        #prepare to send the phase 2a message
        maxvrnd = -1
        toPropose = None
        for msg in messages.values():
            vr, v = msg
            if vr > maxvrnd:
                maxvrnd = vr
                toPropose = v
            elif vr == maxvrnd:
                assert toPropose is None or toPropose == v, \
                        'value=%s == %s or None'%(toPropose, v)
        if toPropose is None:
            retval.set(pvalue)
        else:
            retval.set(toPropose)
        self.logger.debug('%s is to propose %s in round %s at %s'
                          %(self.ID, retval.get(), self.currRnd, now()))

    def _send2aMsg(self, instanceID, pvalue):
        for acc in self.acceptors:
            self.sendMsg(acc, 'paxos 2a',
                         (self, instanceID, self.currRnd, pvalue.get()))
        yield hold, self

    def _recv2bMsg(self, instanceID):
        acceptset = set([])
        rejectset = set([])
        while True:
            for content in self.popContents('paxos %s 2b'%instanceID):
                acc, accepted = content
                if accepted:
                    acceptset.add(acc)
                else:
                    rejectset.add(acc)
            if len(acceptset) >= self.qsize:
                break
            elif len(rejectset) >= self.qsize:
                raise TimeoutException('phase 2b reject')
            for step in self.waitMsg(
                'paxos %s 2b'%instanceID, self.timeout):
                yield step

class Acceptor(IDable, Thread, MsgXeiver):
    """Paxos acceptor."""
    def __init__(self, parent, ID):
        IDable.__init__(self, '%s/acceptor-%s'%(parent.ID, ID))
        Thread.__init__(self)
        MsgXeiver.__init__(self, parent.inetAddr)
        self.parent = parent
        self.rndNo = {}
        self.vrnd = {}
        self.value = {}
        self.close = False
        self.logger = logging.getLogger(self.__class__.__name__)

    def close(self):
        self.close = True

    def run(self):
        while not self.close:
            self._recv1aMsg()
            self._recv2aMsg()
            for step in self.waitMsg(['paxos 1a', 'paxos 2a']):
                yield step

    def _recv1aMsg(self):
        for content in self.popContents('paxos 1a'):
            proposer, instanceID, rndNo = content
            self.logger.debug('%s recv 1a message '
                              'proposer=%s, intanceID=%s, rndNo=%s '
                              'at %s'
                              %(self.ID, proposer.ID, instanceID, rndNo, now()))
            if instanceID not in self.rndNo:
                self.rndNo[instanceID] = rndNo
                self.vrnd[instanceID] = -1
                self.value[instanceID] = None
            elif rndNo > self.rndNo[instanceID]:
                self.rndNo[instanceID] = rndNo
            crnd = self.rndNo[instanceID]
            vrnd = self.vrnd[instanceID]
            value = self.value[instanceID]
            self.sendMsg(proposer, 'paxos %s 1b'%instanceID,
                         (self, crnd, vrnd, value))

    def _recv2aMsg(self):
        for content in self.popContents('paxos 2a'):
            proposer, instanceID, crnd, value = content
            self.logger.debug('%s recv 2a message '
                              'proposer=%s, intanceID=%s, crnd=%s, value=%s '
                              'at %s'
                              %(self.ID, proposer.ID, instanceID,
                                crnd, value, now()))
            if instanceID not in self.rndNo:
                self.rndNo[instanceID] = -1
            if self.rndNo[instanceID] <= crnd:
                #accept the value
                self.rndNo[instanceID] = crnd
                self.vrnd[instanceID] = crnd
                self.value[instanceID] = value
                self.sendMsg(proposer, 'paxos %s 2b'%instanceID,
                             (self, True))
            else:
                self.sendMsg(proposer, 'paxos %s 2b'%instanceID,
                             (self, False))

##### TEST #####
import random
from network import UniformLatencyNetwork

class AcceptorParent(object):
    def __init__(self):
        self.inetAddr = 'accParent'
        self.ID = 'accParent'

class ProposerParent(object):
    def __init__(self):
        self.inetAddr = 'propParent'
        self.ID = 'propParent'

class ProposerRunner(Proposer):
    def run(self):
        for step in self.propose(0, self.ID):
            yield step

class ProposerStarter(Thread):
    def __init__(self, proposers, interval):
        Thread.__init__(self)
        self.proposers = proposers
        self.interval = interval

    def run(self):
        while len(self.proposers) > 0:
            r = random.random()
            if r > 0.5:
                proposer = self.proposers.pop(0)
                proposer.start()
            yield hold, self, self.interval

def testPaxos():
    configs = {
        'network.sim.class' : 'network.UniformLatencyNetwork',
        UniformLatencyNetwork.WITHIN_ZONE_LATENCY_LB_KEY: 0,
        UniformLatencyNetwork.WITHIN_ZONE_LATENCY_UB_KEY: 0,
        UniformLatencyNetwork.CROSS_ZONE_LATENCY_LB_KEY: 10,
        UniformLatencyNetwork.CROSS_ZONE_LATENCY_UB_KEY: 500,
    }
    initialize()
    RTI.initialize(configs)
    numAcceptors = 7
    numProposers = 4
    accparent = AcceptorParent()
    proparent = ProposerParent()
    acceptors = []
    for i in range(numAcceptors):
        acc = Acceptor(accparent, i)
        acceptors.append(acc)
    proposers = []
    for i in range(numProposers):
        prop = ProposerRunner(proparent, i, i, numProposers, acceptors, 1000)
        proposers.append(prop)
    for acceptor in acceptors:
        acceptor.start()
    starter = ProposerStarter(list(proposers), 300)
    starter.start()
    simulate(until=1000000)
    p0 = proposers[0]
    value = proposers[0].instances[0]
    for proposer in proposers:
        assert proposer.instances[0] == value, \
                ('%s.value = %s == %s = %s.value'
                 %(proposer.ID, proposer.instances[0],
                   value, p0.ID))


def test():
    logging.basicConfig(level=logging.DEBUG)
    testPaxos()

def main():
    test()

if __name__ == '__main__':
    main()
