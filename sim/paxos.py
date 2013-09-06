import logging
import math

from SimPy.Simulation import Process, SimEvent
from SimPy.Simulation import initialize, activate, simulate, now
from SimPy.Simulation import waitevent, hold

from core import IDable, RetVal, Thread, TimeoutException, infinite
from rti import RTI, MsgXeiver

class PaxosRoundType(object):
    NORMAL, FAST = range(2)
    TYPES = ['normal', 'fast']

class VPickQuorum(object):
    """Quorum to pick value(e.g. phase 1b message from normal paxos)."""
    NOTREADY, NONE, SINGLE, MULTIPLE, COLLISION = range(5)
    STATES = ['NOTREADY', 'NONE', 'SINGLE', 'MULTIPLE', 'COLLISION']
    def __init__(self, size, qsize, fqsize, total, keyAccs=None):
        self.maxRnd = -1
        self.mrType = None
        self.mrValues = {}
        self.votes = {}     #for correctness check
        self.size = size
        self.qsize = qsize
        self.fqsize = fqsize
        self.total = total
        self.keyAccs = keyAccs
        self.outstanding = None
        self.state = self.__class__.NOTREADY

    def add(self, voter, rnd, rtype, value):
        if value is None:
            assert rnd == -1 or rtype == PaxosRoundType.FAST, \
                    ('voter=%s, rnd=%s==-1 or rtype=%s==fast, value=%s==None'
                     %(voter.ID, rnd, PaxosRoundType[rtype], value))
        else:
            #update round values, keep the max round value
            if rnd > self.maxRnd:
                self.maxRnd = rnd
                self.mrType = rtype
                self.mrValues = {}
                self.mrValues[value] = set([])
                self.mrValues[value].add(voter)
            elif rnd == self.maxRnd:
                assert self.mrType == rtype, \
                        ('mrType = %s == %s = rtype' %(self.mrType, rtype))
                if value not in self.mrValues:
                    self.mrValues[value] = set([])
                self.mrValues[value].add(voter)
        #check voter consistency
        if voter in self.votes:
            #voter cannot vote different value for one round
            r, v = self.votes[voter]
            assert r == rnd and v == value, \
                    ('voter=%s, rnd=%s, pv=%s == %s=cv'
                     %(voter, rnd, v, value))
        else:
            self.votes[voter] = (rnd, value)
        #if we have enough vote for a quorum, and we have the keyset 
        #then we are ready to compute the state
        if len(self.votes) >= self.size:
            self.getState()

    def getState(self):
        if len(self.mrValues) == 0:
            self.state = self.__class__.NONE
        elif len(self.mrValues) == 1:
            value = iter(self.mrValues.values()).next()
            self.outstanding = value
            self.state =  self.__class__.SINGLE
        else:
            for value, voters in self.mrValues.iteritems():
                size = len(voters)
                assert self.mrType == PaxosRoundType.FAST, \
                        ('mrType = %s == fast'
                         %(PaxosRoundType.TYPES[self.mrType]))
                #if there is a value s.t. O4(v) is true
                ##O4(v) be true iff the acceptors chose the value can form a
                ##quorum with the rest of acceptors that have not reported yet.
                required = self.fqsize
                if size + self.total - len(self.votes) >= required:
                    #there can be only one value satisfy O4(v)
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
    def hasKeyAccs(self):
        if self.keyAccs is None:
            return True
        for acc in self.keyAccs:
            if acc not in self.votes:
                return False
        return True

    @property
    def value(self):
        return self.outstanding

    def __str__(self):
        mrVStrs = []
        for v, accset in self.mrValues.iteritems():
            strings = ['%s:['%str(v)]
            for acc in accset:
                strings.append(str(acc))
            strings.append(']')
            mrVStrs.append(' '.join(strings))
        return ('maxRnd=%s, rndType=%s, mvalues={%s}, votes={%s}'
                %(self.maxRnd, PaxosRoundType.TYPES[self.mrType],
                  ', '.join(mrVStrs),
                  ', '.join([str((str(k), v))
                             for k, v in self.votes.iteritems()]))
                 ))

class VLearnQuorum(object):
    """Quorum to learn a value."""
    def __init__(self, qsize, fqsize):
        self.qsize = qsize
        self.fqsize = fqsize
        self.rndValues = {}
        self.rndTypes = {}
        self.votes = {} #for correctness check
        self.finalrnd = -1
        self.finalval = None

    def add(self, voter, rnd, rtype, value):
        #correctness check
        if (rnd, voter) in self.votes:
            assert self.votes[(rnd, voter)] == value, \
                    ('prev = %s == %s = value' 
                     %(self.votes[(rnd, voter)], value))
        else:
            self.votes[(rnd, voter)] = value
        #update value
        if rnd not in self.rndValues:
            self.rndValues[rnd] = {}
            self.rndTypes[rnd] = rtype
        else:
            assert self.rndTypes[rnd] == rtype, \
                    ('pRndType = %s == %s = cRndType'
                     %(PaxosRoundType.TYPES[self.rndTypes[rnd]], 
                       PaxosRoundType.TYPES[rtype]))
        if value not in self.rndValues[rnd]:
            self.rndValues[rnd][value] = set([])
        self.rndValues[rnd][value].add(voter)
        #check ready
        required = self.qsize \
                if self.rndTypes[rnd] == PaxosRoundType.NORMAL \
                else self.fqsize
        if len(self.rndValues[rnd][value]) >= required:
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
            self.sendMsg(acc, '1a', (self, instanceID, self.currRnd))
        yield hold, self

    def _recv1bMsg(self, instanceID, pvalue, retval):
        quorum = VPickQuorum(self.qsize, len(self.acceptors))
        while True:
            for content in self.popContents('1b'):
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
            for step in self.waitMsg('1b', self.timeout):
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
            self.sendMsg(acc, '2a',
                         (self, instanceID, self.currRnd, pvalue.get()))
        yield hold, self

    def _recv2bMsg(self, instanceID):
        quorum = VLearnQuorum(self.qsize)
        while True:
            for content in self.popContents('2b'):
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
            for step in self.waitMsg('2b', self.timeout):
                yield step
        self.instances[instanceID] = quorum.finalval

class Acceptor(IDable, Thread, MsgXeiver):
    """Paxos acceptor."""
    def __init__(self, parent, ID, acceptors, learners,
                 coordinator, rndstep, coordinatedRecovery=False):
        IDable.__init__(self, '%s/acceptor-%s'%(parent.ID, ID))
        Thread.__init__(self)
        MsgXeiver.__init__(self, parent.inetAddr)
        self.parent = parent
        self.rndNo = {}         #the latest round that it participates
        self.vrnd = {}          #the latest round that it accepts a value
        self.vrtype = {}        #the type of the latest round
        self.value = {}         #the value it accepts in the latest round
        self.iquorums = {}      #quorums of instances to resolve collision
        self.acceptors = acceptors
        self.learners = learners
        self.coordinator = coordinator
        self.rndstep = rndstep
        self.coordinatedRecovery = coordinatedRecovery
        self.total = len(self.acceptors)
        self.qsize = self.total / 2 + 1
        self.fqsize = int(self.total - math.ceil(self.total / 3.0) + 1)
        self.keyAccs = []
        sortedaccs = sorted(self.acceptors)
        for i in range(self.qsize):
            self.keyAccs.append(sortedaccs[i])
        self.closed = False
        self.logger = logging.getLogger(self.__class__.__name__)

    def close(self):
        self.closed = True

    def run(self):
        while not self.closed:
            self._recv1aMsg()
            self._recv2aMsg()
            self._recvFastPropose()
            self._recv2bMsg()
            for step in self.waitMsg(['1a', '2a', 'propose', '2b']):
                yield step

    def _recv1aMsg(self):
        for content in self.popContents('1a'):
            proposer, iid, rndNo = content
            self.logger.debug('%s recv 1a message '
                              'proposer=%s, iid=%s, rndNo=%s at %s'
                              %(self.ID, proposer.ID, iid, rndNo, now()))
            if iid not in self.rndNo:
                self.rndNo[iid] = rndNo
                self.vrnd[iid] = -1
                self.vrtype[iid] = None
                self.value[iid] = None
            elif rndNo > self.rndNo[iid]:
                self.rndNo[iid] = rndNo
            crnd = self.rndNo[iid]
            vrnd = self.vrnd[iid]
            vrtype = self.vrtype[iid]
            value = self.value[iid]
            self.sendMsg(proposer, '1b',
                         (self, iid, crnd, vrnd, vrtype, value))

    def _recv2aMsg(self):
        for content in self.popContents('2a'):
            proposer, iid, crnd, value = content
            self.logger.debug('%s recv 2a message '
                              'proposer=%s, iid=%s, crnd=%s, value=%s at %s'
                              %(self.ID, proposer.ID, iid, crnd, value, now()))
            if iid not in self.rndNo:
                self.rndNo[iid] = crnd
                self.vrnd[iid] = -1
                self.vrtype[iid] = None
                self.value[iid] = None
            if self.rndNo[iid] < crnd:
                #accept the value
                self.rndNo[iid] = crnd
                self.vrnd[iid] = crnd
                self.value[iid] = value
                #decide round type
                if value is None:
                    #if proposer did not send any value, it's a fast round
                    self.vrtype[iid] = PaxosRoundType.FAST
                    #we need to wait a proposal for the value
                    #so do not send a message to the learner
                else:
                    self.vrtype[iid] = PaxosRoundType.NORMAL
                    #let learner knows about the result
                    for lnr in self.learners:
                        self.sendMsg(lnr, '2b',
                                     (self, iid, self.vrnd[iid], 
                                      self.vrtype[iid], self.value[iid]))
            elif self.rndNo[iid] == crnd:
                #proposer can only send one value in one round
                assert self.vrnd[iid] == crnd and \
                        self.value[iid] == value, \
                        ('prnd = %s == %s = crnd, pval = %s == %s = cval'
                         %(self.vrnd[iid], crnd, self.value[iid], value))
            else:
                #ignore this message if we are participating a new round
                pass

    def _recvFastPropose(self):
        for content in self.popContents('propose'):
            proposer, iid, value = content
            self.logger.debug('%s recv propose message '
                              'proposer=%s, iid=%s, value=%s at %s'
                              %(self.ID, proposer.ID, iid, value, now()))
            assert value is not None, \
                    ('proposed value = %s != None' %value)
            if iid not in self.rndNo:
                self.rndNo[iid] = -1
                self.vrnd[iid] = -1
                self.vrtype[iid] = None
                self.value[iid] = None
            #round zero is set to be a fast round
            if self.rndNo[iid] == -1:
                self.rndNo[iid] = 0
                self.vrnd[iid] = 0
                self.vrtype[iid] = PaxosRoundType.FAST
                self.value[iid] = value
            elif self.rndNo[iid] == self.vrnd[iid] and \
                    self.vrtype[iid] == PaxosRoundType.FAST:
                self.value[iid] = value
            else:
                #the latest round is not a fast round
                #or we have already recieved a value for that fast round
                pass
            #send to coordinator or acceptors to resolve collision if any
            if coordinatedRecovery:
                #if coordinated recovery
                self.sendMsg(self.coordinator, '2b',
                             (self, iid, self.vrnd[iid], 
                              self.vrtype[iid], self.value[iid]))
            else:
                for acc in self.acceptors:
                    self.sendMsg(acc, '2b',
                                 (self, iid, self.vrnd[iid], 
                                  self.vrtype[iid], self.value[iid]))
            for lnr in self.learners:
                self.sendMsg(lnr, '2b',
                             (self, iid, self.vrnd[iid], 
                              self.vrtype[iid], self.value[iid]))

    def _recv2bMsg(self):
        for content in self.popContents('2b'):
            acc, iid, rnd, rtype, value = content
            self.logger.debug('%s recv 2b message: acc=%s, iid=%s, '
                              'rnd=%s, type=%s, value=%s at %s'
                              %(self.ID, acc.ID, iid, rnd, 
                                PaxosRoundType.TYPES[rtype],
                                value, now()))
            if iid not in self.iquorums:
                self.iquorums[iid] = VPickQuorum(
                    self.fqsize, self.qsize, self.fqsize, 
                    self.total, self.keyAccs)
            self.iquorums[iid].add(acc, rnd, rtype, value)
            quorum = self.iquorums[iid]
            if quorum.isReady:
                if quorum.state == VPickQuorum.COLLISION:
                    if quorum.hasKeyAccs:
                        #choose the value
                        chosen = 'null'
                        for acc in quorum.keyAccs:
                            r, v = quorum.votes[acc]
                            if r == quorum.maxRnd:
                                chosen = v
                                break
                        #send the chosen value
                        for acc in self.acceptors:
                            self.sendMsg(acc, '2a',
                                         (self, iid,
                                          rnd + self.rndstep, chosen))
                        del self.iquorums[iid]
                else:
                    del self.iquorums[iid]

class Learner(IDable, Thread, MsgXeiver):
    def __init__(self, parent, ID, qsize, fqsize):
        IDable.__init__(self, '%s/learner-%s'%(parent.ID, ID))
        Thread.__init__(self)
        MsgXeiver.__init__(self, parent.inetAddr)
        self.parent = parent
        self.instances = {}
        self.iquorums = {}
        self.qsize = qsize
        self.fqsize = fqsize
        self.closed = False

    def close(self):
        self.closed = True

    def run(self):
        while not self.closed:
            self._recv2bMsg()
            for step in self.waitMsg('2b'):
                yield step

    def _recv2bMsg(self):
        for content in self.popContents('2b'):
            acc, iid, rnd, rtype, value = content
            if iid in self.instances:
                #we have already learned the value
                continue
            if iid not in self.iquorums:
                self.iquorums[iid] = VLearnQuorum(self.qsize, self.fqsize)
            self.iquorums[iid].add(acc, rnd, rtype, value)
            #check iquorum status
            if self.iquorums[iid].isReady:
                self.instances[iid] = self.iquorums[iid].finalval
                del self.iquorums[iid]

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
            self.sendMsg(acc, 'fast propose', (self, instanceID, value))

class Coordinator(IDable, Thread, MsgXeiver):
    """Coordinator is a special proposer which deals with fast rounds."""
    def __init__(self, parent, ID, acceptors, timeout=infinite):
        IDable.__init__(self, '%s/coordinator-%s'%(parent.ID, ID))
        Thread.__init__(self)
        MsgXeiver.__init__(self, parent.inetAddr)
        self.parent = parent
        self.acceptors = acceptors
        self.iquorums = {}
        self.total = len(acceptors)
        self.qsize = self.total / 2 + 1
        self.fqsize = int(self.total - math.ceil(self.total / 3.0) + 1)
        self.closed = False
        self.logger = logging.getLogger(self.__class__.__name__)

    def close():
        self.closed = True

    def run(self):
        while not self.closed:
            for content in self.popContents('2b'):
                acc, iid, rnd, rtype, value = content
                self.logger.debug('%s recv 2b message: acc=%s, iid=%s, '
                                  'rnd=%s, type=%s, value=%s at %s'
                                  %(self.ID, acc.ID, iid, rnd, 
                                    PaxosRoundType.TYPES[rtype],
                                    value, now()))
                assert rtype == PaxosRoundType.FAST, \
                        ('rtype = %s == fast' %(PaxosRoundType.TYPES[rtype]))
                if iid not in self.iquorums:
                    self.iquorums[iid] = VPickQuorum(
                        self.fqsize, self.qsize, self.fqsize, self.total)
                self.iquorums[iid].add(acc, rnd, rtype, value)
                quorum = self.iquorums[iid]
                if quorum.isReady:
                    #check if the latest round has collision
                    if quorum.state == VPickQuorum.COLLISION:
                        self._recoverCollision(iid, pickQuorums)
            for step in self.waitMsg('2b'):
                yield step

    def _recoverCollision(self, instanceID, quorum):
        value = iter(sorted(quorum.mrValues.keys())).next()
        rnd = quorum.maxRnd
        for acc in self.acceptors:
            self.sendMsg(acc, '2a', (self, instanceID, rnd + 1, value))

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
