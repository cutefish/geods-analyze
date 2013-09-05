import logging
import math

from SimPy.Simulation import Process, SimEvent
from SimPy.Simulation import initialize, activate, simulate, now
from SimPy.Simulation import waitevent, hold

from core import IDable, RetVal, Thread, TimeoutException, infinite
from rti import RTI, MsgXeiver

class VPickQuorum(object):
    """Quorum to pick value(e.g. phase 1b message from normal paxos)."""
    NOTREADY, NONE, SINGLE, MULTIPLE, COLLISION = range(5)
    STATES = ['NOTREADY', 'NONE', 'SINGLE', 'MULTIPLE', 'COLLISION']
    def __init__(self, qsize, total):
        self.currRnd = -1
        self.crValues = {}
        self.votes = {}
        self.qsize = qsize
        self.total = total
        self.outstanding = None
        self.state = self.__class__.NOTREADY

    def add(self, voter, rnd, value):
        #update current round values, only the max round values are useful
        if rnd > self.currRnd:
            self.currRnd = rnd
            self.crValues = {}
            if value is not None:
                self.crValues[value] = set([])
                self.crValues[value].add(voter)
        elif rnd == self.currRnd:
            if value is not None:
                if value not in self.crValues:
                    self.crValues[value] = set([])
                self.crValues[value].add(voter)
        #update voter
        if voter in self.votes:
            #phase 1b message cannot send different value for the same round.
            r, v = self.votes[voter]
            assert r == rnd and v == value, \
                    ('voter=%s, rnd=%s, pv=%s == %s=cv'
                     %(voter, rnd, v, value))
        else:
            self.votes[voter] = (rnd, value)
        if len(self.votes) >= self.qsize:
            self.getState()

    def getState(self):
        if len(self.crValues) == 0:
            self.state = self.__class__.NONE
        elif len(self.crValues) == 1:
            value, voters = iter(self.crValues.iteritems()).next()
            self.outstanding = value
            self.state =  self.__class__.SINGLE
        else:
            for value, voters in self.crValues.iteritems():
                size = len(voters)
                if size + self.total - len(self.votes) >= self.qsize:
                    assert self.outstanding is None, \
                            ('outstanding=%s, curr=%s, values={%s}'
                             %(outstanding, value, self))
                    self.outstanding = value
                    self.state = self.__class__.MULTIPLE
            if self.outstanding is None:
                self.state = self.__class__.COLLISION

    @property
    def isReady(self):
        return self.state != self.__class__.NOTREADY

    @property
    def value(self):
        return self.outstanding

    def __str__(self):
        return ('currRnd=%s, values={%s}, votes={%s}'
                %(', '.join([str(k, str(v))
                             for k, v in self.crValues.iteritems()]),
                  ', '.join([str(str(k), v)
                             for k, v in self.votes.iteritems()])
                 ))

class VLearnQuorum(object):
    """Quorum to learn a value."""
    def __init__(self, qsize):
        self.qsize = qsize
        self.rndValues = {}
        self.finalrnd = -1
        self.finalval = None

    def add(self, voter, rnd, value):
        if rnd not in self.rndValues:
            self.rndValues[rnd] = {}
        if value not in self.rndValues[rnd]:
            self.rndValues[rnd][value] = set([])
        self.rndValues[rnd][value].add(voter)
        if len(self.rndValues[rnd][value]) >= self.qsize:
            if self.finalval is None:
                self.finalrnd = rnd
                self.finalval = value
            else:
                assert self.finalval == value, \
                        'final = %s == %s = value' %(self.finalval, value)

    @property
    def isReady(self):
        return self.finalval is not None

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
                break
            except TimeoutException as e:
                self.logger.info('%s will start a new round at %s. Cause: %s'
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
        quorum = VPickQuorum(self.qsize, len(self.acceptors))
        while True:
            for content in self.popContents('paxos 1b'):
                acc, iid, crnd, vrnd, value = content
                self.logger.debug('%s recv 1b message: acc=%s, iid=%s, '
                                  'crnd=%s, vrnd=%s, value=%s at %s'
                                  %(self.ID, acc.ID, iid,
                                    crnd, vrnd, value, now()))
                assert iid <= instanceID, \
                        'iid = %s <= %s = instanceID'%(iid, instanceID)
                if iid < instanceID:
                    continue
                assert vrnd <= crnd, \
                        'vrndNo = %s <= %s = rndNo' %(vrnd, crnd)
                if crnd >= self.currRnd:
                    quorum.add(acc, vrnd, value)
                else:
                    #ignore previous round messages
                    pass
            if quorum.isReady:
                break
            for step in self.waitMsg('paxos 1b', self.timeout):
                yield step
        #prepare to send the phase 2a message
        assert (quorum.state == VPickQuorum.NONE or
                quorum.state == VPickQuorum.SINGLE), \
                ('quorum state == %s(not NONE or SINGLE), '
                 'quorum = %s' 
                 %(VPickQuorum.STATES[quorum.state], quorum))
        if quorum.state == VPickQuorum.NONE:
            retval.set(pvalue)
        else:
            retval.set(quorum.outstanding)
        self.logger.debug('%s is to propose %s in round %s at %s'
                          %(self.ID, retval.get(), self.currRnd, now()))

    def _send2aMsg(self, instanceID, pvalue):
        for acc in self.acceptors:
            self.sendMsg(acc, 'paxos 2a',
                         (self, instanceID, self.currRnd, pvalue.get()))
        yield hold, self

    def _recv2bMsg(self, instanceID):
        quorum = VLearnQuorum(self.qsize)
        while True:
            for content in self.popContents('paxos 2b'):
                acc, iid, rnd, value = content
                self.logger.debug('%s recv 2b message: acc=%s, iid=%s, '
                                  'rnd=%s, value=%s at %s'
                                  %(self.ID, acc.ID, iid, rnd, value, now()))
                assert iid <= instanceID, \
                        'iid = %s <= %s = instanceID'%(iid, instanceID)
                if iid < instanceID:
                    continue
                quorum.add(acc, rnd, value)
            if quorum.isReady:
                break
            for step in self.waitMsg('paxos 2b', self.timeout):
                yield step
        self.instances[instanceID] = quorum.finalval

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
        self.closed = False
        self.logger = logging.getLogger(self.__class__.__name__)

    def close(self):
        self.closed = True

    def run(self):
        while not self.closed:
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
            self.sendMsg(proposer, 'paxos 1b',
                         (self, instanceID, crnd, vrnd, value))

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
            self.sendMsg(proposer, 'paxos 2b',
                         (self, instanceID,
                          self.vrnd[instanceID], self.value[instanceID]))

class FastProposer(Proposer):
    def __init__(self, parent, ID, acceptors, timeout=infinite):
        Proposer.__init__(self, parent, ID, 0, 0, acceptors, timeout)
        n = len(acceptors)
        self.qsize = int(n - math.ceil(n / 3.0) + 1)
        
    def propose(self, instanceID, value):
        while True:
            try:
                self._propose(instanceID, value)
                self._recv2bMsg(instanceID)
                break
            except:
                pass

    def _propose(self, instanceID, value):
        for acc in self.acceptors:
            self.sendMsg(acc, 'fast paxos 2a', (self, instanceID, value))

class FastCoordinator(IDable, Thread, MsgXeiver):
    def __init__(self, parent, ID, acceptors, timeout=infinite):
        IDable.__init__(self, '%s/coordinator-%s'%(parent.ID, ID))
        Thread.__init__(self)
        MsgXeiver.__init__(self, parent.inetAddr)
        self.parent = parent
        self.acceptors = acceptors
        self.pickQuorums = {}
        self.learnQuorums = {}
        self.instances = {}
        self.total = len(acceptors)
        self.qsize = int(self.total - math.ceil(self.total / 3.0) + 1)
        self.timeout = timeout
        self.closed = False

    def close():
        self.closed = True

    def run(self):
        self._startup()
        for step in self._run():
            yield step

    def _startup(self):
        #the start up procedure is for fault recovery
        #do nothing for our simulation
        pass

    def _run(self):
        while not self.closed:
            for content in self.popContents('paxos 2b'):
                acc, iid, rnd, value = content
                self.logger.debug('%s recv 2b message: acc=%s, iid=%s, '
                                  'rnd=%s, value=%s at %s'
                                  %(self.ID, acc.ID, iid, rnd, value, now()))
                if iid in self.instances:
                    finalrnd, finalvalue = self.instances[iid]
                    assert rnd < finalrnd or value == finalvalue, \
                            ('rnd = %s < %s = finalrnd or'
                             'value = %s == %s = finalvalue'
                             %(rnd, finalrnd, value, finalvalue))
                else:
                    if iid not in self.pickQuorums:
                        self.pickQuorums[iid] = VPickQuorum(
                            self.qsize, self.total)
                    if iid not in self.learnQuorums:
                        self.pickQuorums[iid] = VLearnQuorum(self.qsize)
                    self.pickQuorums[iid].add(acc, rnd, value)
                    self.learnQuorums[iid].add(acc, rnd, value)
                    if self.learnQuorums[iid].isReady:
                        #we got the final value of iid
                        frnd = self.learnQuorums[iid].finalrnd
                        fval = self.learnQuorums[iid].finalval
                        self.instances[iid] = (frnd, fval)
                        #no need for quorums
                        del self.pickQuorums[iid]
                        del self.learnQuorums[iid]
                    elif self.pickQuorums[iid].isReady:
                        #check if the latest round has collision
                        if self.pickQuorums[iid].state == \
                           VPickQuorum.COLLISION:
                            self._resolveCollision()

#class FastAcceptor(object):
#    def run(self):
#        while True:
#            self._recvFast2aMsg()
#            self._sendFast2bMsg()

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
        UniformLatencyNetwork.CROSS_ZONE_LATENCY_UB_KEY: 1000,
    }
    initialize()
    RTI.initialize(configs)
    numAcceptors = 7
    numProposers = 5
    accparent = AcceptorParent()
    proparent = ProposerParent()
    acceptors = []
    for i in range(numAcceptors):
        acc = Acceptor(accparent, i)
        acceptors.append(acc)
    proposers = []
    for i in range(numProposers):
        prop = ProposerRunner(proparent, i, i, numProposers, acceptors, 2000)
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
