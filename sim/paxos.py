import logging
import math

from SimPy.Simulation import Process, SimEvent
from SimPy.Simulation import initialize, activate, simulate, now
from SimPy.Simulation import waitevent, hold

import sim
from core import Alarm, IDable, RetVal, Thread, TimeoutException, infinite
from rti import RTI, MsgXeiver

class PaxosRoundType(object):
    NONE, NORMAL, FAST = range(3)
    TYPES = ['none', 'normal', 'fast']

def getClassicQSize(n):
    return n - int(math.ceil(n / 2.0) - 1)

def getFastQSize(n):
    return n - int(math.floor(n / 4.0))

class VPickQuorum(object):
    """Quorum to pick value(e.g. phase 1b message from normal paxos)."""
    NOTREADY, NONE, SINGLE, COL_SINGLE, COL_NONE = range(5)
    STATES = ['NOTREADY', 'NONE', 'SINGLE', 'COL_SINGLE', 'COL_NONE']
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
        #update voter
        if voter in self.votes:
            r, v = self.votes[voter]
            if r > rnd:
                pass
            elif r == rnd:
                #voter cannot vote different value for the same round
                assert v == value, \
                        ('voter=%s, rnd=%s, pv=%s == %s=cv'
                         %(voter, rnd, v, value))
            else:
                self.votes[voter] = (rnd, value)
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
            value = iter(self.mrValues.keys()).next()
            self.outstanding = value
            self.state =  self.__class__.SINGLE
        else:
            #there is a collision
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
                    assert self.outstanding is None or \
                            self.outstanding == value, \
                            ('outstanding=%s, curr=%s, quorum={%s} '
                             'size=%s, total=%s, nvotes=%s, required=%s'
                             %(self.outstanding, value, self,
                               size, self.total, len(self.votes), required))
                    self.outstanding = value
                    self.state = self.__class__.COL_SINGLE
            if self.outstanding is None:
                self.state = self.__class__.COL_NONE

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
            r, v = self.votes[acc]
            if r < self.maxRnd:
                return False
        return True

    @property
    def value(self):
        return self.outstanding

    def __str__(self):
        mrVStrs = []
        vStrs = []
        for v, accset in self.mrValues.iteritems():
            strings = ['%s:['%str(v)]
            for acc in accset:
                strings.append(str(acc))
            strings.append(']')
            mrVStrs.append(' '.join(strings))
        for a, rv in self.votes.iteritems():
            r, v = rv
            vStrs.append('%s: (%s, %s)'%(a, r, v))
        return ('maxRnd=%s, rndType=%s, mvalues={%s}, votes={%s}'
                %(self.maxRnd, PaxosRoundType.TYPES[self.mrType],
                  ', '.join(mrVStrs),
                  ', '.join(vStrs)))

class VLearnQuorum(object):
    """Quorum to learn a value."""
    def __init__(self, qsize, fqsize):
        self.qsize = qsize
        self.fqsize = fqsize
        self.maxRnd = -1
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
        try:
            if value not in self.rndValues[rnd]:
                self.rndValues[rnd][value] = set([])
        except TypeError:
            print voter, rnd, rtype, value
        if value not in self.rndValues[rnd]:
            self.rndValues[rnd][value] = set([])
        self.rndValues[rnd][value].add(voter)
        #update max round
        if rnd > self.maxRnd:
            self.maxRnd = rnd
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

class Acceptor(IDable, Thread, MsgXeiver):
    """Paxos acceptor."""
    def __init__(self, parent, rndstep, coordinatedRecovery=False):
        IDable.__init__(self, '%s/acceptor'%parent.ID)
        Thread.__init__(self)
        MsgXeiver.__init__(self, parent.inetAddr)
        self.parent = parent
        self.rndNo = {}         #the latest round that it participates
        self.vrnd = {}          #the latest round that it accepts a value
        self.vrtype = {}        #the type of the latest round
        self.value = {}         #the value it accepts in the latest round
        self.iquorums = {}      #quorums of instances to resolve collision
        self.acceptors = None
        self.learners = None
        self.coordinator = None
        self.rndstep = rndstep
        self.coordinatedRecovery = coordinatedRecovery
        self.closed = False
        self.logger = logging.getLogger(self.__class__.__name__)

    def init(self, acceptors, learners, coordinator):
        self.acceptors = acceptors
        self.learners = learners
        self.coordinator = coordinator
        self.total = len(self.acceptors)
        self.qsize = getClassicQSize(self.total)
        self.fqsize = getFastQSize(self.total)
        self.keyAccs = []
        sortedaccs = sorted(self.acceptors)
        for i in range(self.qsize):
            self.keyAccs.append(sortedaccs[i])

    def close(self):
        self.closed = True

    def run(self):
        if self.acceptors is None:
            raise ValueError('init before run')
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
                self.vrtype[iid] = PaxosRoundType.NONE
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
                              'proposer=%s, iid=%s, rnd=%s, value=%s at %s'
                              %(self.ID, proposer.ID, iid, crnd, value, now()))
            if iid not in self.rndNo:
                self.rndNo[iid] = crnd
                self.vrnd[iid] = -1
                self.vrtype[iid] = PaxosRoundType.NONE
                self.value[iid] = None
            if self.rndNo[iid] <= crnd:
                if self.vrnd[iid] == crnd:
                    #can only receive one value in one round in 2a phase
                    #fast rounds are dealt with in _recvFastPropose()
                    assert self.value[iid] == value, \
                            ('pval = %s == %s = cval' %(self.value[iid], value))
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
                self.vrtype[iid] = PaxosRoundType.NONE
                self.value[iid] = None
            #round zero is set to be a fast round
            if self.rndNo[iid] == -1:
                self.rndNo[iid] = 0
                self.vrnd[iid] = 0
                self.vrtype[iid] = PaxosRoundType.FAST
                self.value[iid] = value
                self._sendProposeMsg(iid)
            elif self.rndNo[iid] == self.vrnd[iid] and \
                    self.vrtype[iid] == PaxosRoundType.FAST:
                if self.value[iid] == None:
                    #only receive the first propose
                    self.value[iid] = value
                    self._sendProposeMsg(iid)
            else:
                #the latest round is not a fast round
                pass

    def _sendProposeMsg(self, iid):
        #send to coordinator or acceptors to resolve collision if any
        if self.coordinatedRecovery:
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
                    self.qsize, self.qsize, self.fqsize,
                    self.total, self.keyAccs)
            self.iquorums[iid].add(acc, rnd, rtype, value)
            quorum = self.iquorums[iid]
            if quorum.hasKeyAccs:
                #we always propose something for next round to make progress
                assert quorum.isReady
                #we need to form a new quroum with only key acceptors such that
                #everyone will propose the same
                q = VPickQuorum(self.qsize, self.qsize, self.fqsize,
                                self.total, self.keyAccs)
                for acc in self.keyAccs:
                    r, v = quorum.votes[acc]
                    assert r == quorum.maxRnd, 'quorum: %s'%quorum
                    q.add(acc, r, PaxosRoundType.FAST, v)
                #pick a value
                chosen = None
                if q.state == VPickQuorum.COL_NONE:
                    acc = self.keyAccs[0]
                    r, v = q.votes[acc]
                    chosen = v
                    self.logger.debug('%s resolve collision:'
                                      'iid=%s, rnd=%s, type=%s, '
                                      'value=%s, acc=%s, quorum=%s, at %s'
                                      %(self.ID, iid, rnd,
                                        PaxosRoundType.TYPES[rtype],
                                        chosen, acc, quorum, now()))
                else:
                    self.logger.debug('%s resolve collision not needed:'
                                      'iid=%s, rnd=%s, type=%s, '
                                      'value=%s, acc=%s, quorum=%s, at %s'
                                      %(self.ID, iid, rnd,
                                        PaxosRoundType.TYPES[rtype],
                                        chosen, acc, quorum, now()))
                    chosen = q.outstanding
                    assert chosen != None, 'quorum: %s'%q
                #send the chosen value
                for acc in self.acceptors:
                    self.sendMsg(acc, '2a',
                                 (self, iid,
                                  rnd + self.rndstep, chosen))
                del self.iquorums[iid]

class Learner(IDable, Thread, MsgXeiver):
    def __init__(self, parent):
        IDable.__init__(self, '%s/learner'%parent.ID)
        Thread.__init__(self)
        MsgXeiver.__init__(self, parent.inetAddr)
        self.parent = parent
        self.instances = {}
        self.iquorums = {}
        self.newInstanceEvent = SimEvent()
        self.total = -1
        self.closed = False
        self.logger = logging.getLogger(self.__class__.__name__)

    def init(self, total):
        self.total = total
        self.qsize = getClassicQSize(self.total)
        self.fqsize = getFastQSize(self.total)

    def close(self):
        self.closed = True

    def getQuorumMaxRnd(self, instanceID):
        if instanceID in self.instances:
            raise ValueError('instance %s already reach consensus')
        if instanceID not in self.iquorums:
            return -1
        else:
            return self.iquorums[instanceID].maxRnd

    def run(self):
        if self.total == -1:
            raise ValueError('init before run')
        while not self.closed:
            self._recv2bMsg()
            for step in self.waitMsg('2b'):
                yield step

    def _recv2bMsg(self):
        for content in self.popContents('2b'):
            acc, iid, rnd, rtype, value = content
            self.logger.debug('%s recv 2b message: acc=%s, iid=%s, '
                              'rnd=%s, type=%s, value=%s at %s'
                              %(self.ID, acc.ID, iid, rnd,
                                PaxosRoundType.TYPES[rtype],
                                value, now()))
            if iid in self.instances:
                #we have already learned the value
                continue
            if iid not in self.iquorums:
                self.iquorums[iid] = VLearnQuorum(self.qsize, self.fqsize)
            self.iquorums[iid].add(acc, rnd, rtype, value)
            #check iquorum status
            if self.iquorums[iid].isReady:
                finalrnd = self.iquorums[iid].finalrnd
                finalval = self.iquorums[iid].finalval
                self.instances[iid] = finalval
                del self.iquorums[iid]
                self.newInstanceEvent.signal()
                self.logger.debug('%s "LEARNED": iid=%s, rnd=%s, value=%s at %s'
                                  %(self.ID, iid, finalrnd, finalval, now()))

class Coordinator(IDable, Thread, MsgXeiver):
    """A special proposer for fast rounds with starting round number 0.

    Coordinator does two things:
        (1) Resolve fast rounds collision.
        (2) Fast proposals will be lost at the acceptor side if it is not a
        fast round when proposal reaches the acceptor. The coordinator will
        propose rounds once a while to ensure progress.
    """
    def __init__(self, parent, rndstep, timeout=infinite):
        IDable.__init__(self, '%s/coordinator'%parent.ID)
        Thread.__init__(self)
        MsgXeiver.__init__(self, parent.inetAddr)
        self.parent = parent
        self.acceptors = None
        self.learner = None
        self.timeout = timeout
        self.iquorums = {}
        self.chosenValues = {}      #to make sure always choose the same value
        self.rndstep = rndstep
        self.closed = False
        self.logger = logging.getLogger(self.__class__.__name__)

    def init(self, acceptors, learner):
        self.acceptors = acceptors
        self.learner = learner
        self.total = len(acceptors)
        self.qsize = getClassicQSize(self.total)
        self.fqsize = getFastQSize(self.total)

    def close(self):
        self.closed = True

    def cleanup(self):
        for iid in self.iquorums.keys():
            if iid in self.learner.instances:
                del self.iquorums[iid]
        for iid in self.chosenValues.keys():
            if iid in self.learner.instances:
                del self.chosenValues[iid]

    def run(self):
        if self.acceptors is None:
            raise ValueError('init before run')
        while not self.closed:
            try:
                self._recv1bMsg()
                self._recv2bMsg()
                self.cleanup()
                for step in self.waitMsg(['1b', '2b'], self.timeout):
                    yield step
            except TimeoutException as e:
                #if nothing happens for a while, check if we need to start a
                #new round to make sure progress
                self._startNewRounds()

    def _recv1bMsg(self):
        for content in self.popContents('1b'):
            acc, iid, crnd, rnd, rtype, value = content
            self.logger.debug('%s recv 1b message: acc=%s, iid=%s, '
                              'crnd=%s, rnd=%s, type=%s, value=%s at %s'
                              %(self.ID, acc.ID, iid, crnd, rnd,
                                PaxosRoundType.TYPES[rtype],
                                value, now()))
            if iid in self.learner.instances:
                continue
            if iid not in self.iquorums:
                self.iquorums[iid] = VPickQuorum(
                    self.fqsize, self.qsize, self.fqsize, self.total)
            quorum = self.iquorums[iid]
            quorum.add(acc, rund, rtype, value)
            if crnd % self.rndstep != 0:
                #other proposers are in progress, don't bother.
                continue
            if not quorum.isReady:
                continue
            if quorum.state == VPickQuorum.NONE:
                #start a fast round
                for acc in self.acceptors:
                    self.sendMsg(acc, '2a', (iid, crnd, None))
            elif quorum.state == VPickQuorum.SINGLE or \
                    quorum.state == VPickQuorum.COL_SINGLE:
                #propose a possible candidate value
                for acc in self.acceptors:
                    self.sendMsg(acc, '2a', (iid, crnd, quorum.outstanding))
            else:
                #we have a collision and none of the values are possible
                #candidate
                self._recoverCollision(iid, quorum)

    def _recv2bMsg(self):
        for content in self.popContents('2b'):
            acc, iid, rnd, rtype, value = content
            self.logger.debug('%s recv 2b message: acc=%s, iid=%s, '
                              'rnd=%s, type=%s, value=%s at %s'
                              %(self.ID, acc.ID, iid, rnd,
                                PaxosRoundType.TYPES[rtype],
                                value, now()))
            assert rtype == PaxosRoundType.FAST, \
                    ('rtype = %s == fast' %(PaxosRoundType.TYPES[rtype]))
            if iid in self.learner.instances:
                continue
            if iid not in self.iquorums:
                self.iquorums[iid] = VPickQuorum(
                    self.fqsize, self.qsize, self.fqsize, self.total)
            self.iquorums[iid].add(acc, rnd, rtype, value)
            quorum = self.iquorums[iid]
            if quorum.isReady:
                #to make progress, we propose a value for the new round
                #no matter collision or not
                if quorum.state == VPickQuorum.COL_NONE:
                    #collision and none outstanding needs special treatment
                    self._recoverCollision(iid, quorum)
                else:
                    #some value is outstanding
                    assert quorum.state != VPickQuorum.NONE, \
                            'quorum: %s'%quorum
                    for acc in self.acceptors:
                        self.sendMsg(acc, '2a',
                                     (self, iid, rnd + self.rndstep,
                                      quorum.outstanding))

    def _recoverCollision(self, instanceID, quorum):
        rnd = quorum.maxRnd
        if (instanceID, rnd) in self.chosenValues:
            value = self.chosenValues[(instanceID, rnd)]
        else:
            value = iter(sorted(quorum.mrValues.keys())).next()
            self.chosenValues[(instanceID, rnd)] = value
        for acc in self.acceptors:
            self.sendMsg(acc, '2a',
                         (self, instanceID, rnd + self.rndstep, value))

    def _startNewRounds(self):
        for iid in self.iquorums.keys():
            quorum = self.iquorums[iid]
            if iid in self.learner.instances:
                #we already have a value for this instance
                del self.iquorums[iid]
            else:
                #start another fast round for this instance
                rnd = (quorum.maxRnd / rndstep  + 1) * rndstep
                for acc in self.acceptors:
                    self.sendMsg(acc, '1a', (self, iid, rnd))

class RoundFailException(Exception):
    pass

class Proposer(IDable, Thread, MsgXeiver):
    """Paxos proposer."""
    def __init__(self, prunner, rnd0, rndstep, acceptors, learner,
                 instanceID, value, timeout=infinite,
                 isFast=False, noPhase1=False):
        IDable.__init__(self,
                        '%s/prop-(%s, %s)'%(
                            prunner.ID, str(instanceID), str(value)))
        Thread.__init__(self)
        MsgXeiver.__init__(self, prunner.parent.inetAddr)
        assert rnd0 != 0        #saved for fast round and coordinator
        self.prunner = prunner
        self.rnd0 = rnd0
        self.rndstep = rndstep
        self.acceptors = acceptors
        self.learner = learner
        self.instanceID = instanceID
        self.value = value
        self.timeout = timeout
        self.isFast = isFast
        self.noPhase1 = noPhase1
        self.total = len(self.acceptors)
        self.qsize = getClassicQSize(self.total)
        self.fqsize = getFastQSize(self.total)
        self.crnd = rnd0
        self.quorum = None
        self.isSuccess = False
        self.logger = logging.getLogger(self.__class__.__name__)

    def propose(self):
        self.crnd = self.rnd0
        #set pick value
        if self.noPhase1:
            #if we are the only proposer, it is certain that we will pick the
            #value we want.
            pvalue = self.value
        else:
            pvalue = None
        while True:
            try:
                if self.instanceID in self.learner.instances:
                    break
                #if we aren't already sure which value to pick
                if pvalue is None:
                    #initialize the pick quorum
                    self.quorum = VPickQuorum(self.qsize, self.qsize,
                                              self.fqsize, self.total)
                    #send 1a message
                    self._send1aMsg()
                    self.logger.debug('%s sent 1a message in round %s at %s'
                                      %(self.ID, self.crnd, now()))
                    #recv 1b message
                    for step in self._recv1bMsg():
                        yield step
                    self.logger.debug('%s got 1b message in round %s at %s'
                                      %(self.ID, self.crnd, now()))
                    #check with learner
                    if self.instanceID in self.learner.instances:
                        break
                    #choose the value
                    pvalue = self._pickValue()
                    self.logger.debug('%s pick value %s in round %s at %s'
                                      %(self.ID, pvalue, self.crnd, now()))
                #send 2a message
                self._send2aMsg(pvalue)
                self.logger.debug('%s sent 2a message in round %s at %s'
                                  %(self.ID, self.crnd, now()))
                #check value
                for step in self._checkValue():
                    yield step
                #finish
                break
            except RoundFailException:
                #if we see no progress(timeout), and we know of a later round
                #we should start new round to make progress
                self.logger.debug('%s iid=%s timedout and round %s fails;'
                                  ' next: %s at %s'
                                 %(self.ID, self.instanceID, self.crnd,
                                   self.crnd + self.rndstep, now()))
                self.crnd += self.rndstep
                if self.noPhase1:
                    pvalue = value
                else:
                    pvalue = None
            except TimeoutException:
                #if we see no progress and we don't know any later rounds
                #simply resend our messages.
                pass
        self.logger.debug('%s reach concensus for '
                          'instance=%s with value=%s at %s'
                          %(self.ID, self.instanceID,
                            self.learner.instances[self.instanceID], now()))

    def _send1aMsg(self):
        for acc in self.acceptors:
            self.sendMsg(acc, '1a', (self, self.instanceID, self.crnd))

    def _recv1bMsg(self):
        while True:
            for content in self.popContents('1b'):
                acc, iid, crnd, vrnd, vtype, value = content
                self.logger.debug('%s recv 1b message: acc=%s, iid=%s, '
                                  'crnd=%s, vrnd=%s, vtype=%s, value=%s at %s'
                                  %(self.ID, acc.ID, iid,
                                    crnd, vrnd, PaxosRoundType.TYPES[vtype],
                                    value, now()))
                assert iid == self.instanceID, \
                        'iid = %s == %s = instanceID'%(iid, self.instanceID)
                assert vrnd <= crnd, \
                        'vrndNo = %s <= %s = rndNo' %(vrnd, crnd)
                if crnd >= self.crnd:
                    self.quorum.add(acc, vrnd, vtype, value)
                else:
                    #ignore previous round messages
                    pass
            if self.instanceID in self.learner.instances:
                break
            if self.quorum.isReady:
                break
            events = self.getWaitMsgEvents('1b')
            timeoutEvent = Alarm.setOnetime(self.timeout)
            events.append(timeoutEvent)
            events.append(self.learner.newInstanceEvent)
            yield waitevent, self, events
            if timeoutEvent in self.eventsFired:
                #no progress, we need to do something
                if self.quorum.maxRnd > self.crnd:
                    raise RoundFailException
                else:
                    raise TimeoutException

    def _pickValue(self):
        #pick a value from the quorum
        if self.quorum.state == VPickQuorum.NONE or \
           self.quorum.state == VPickQuorum.COL_NONE:
            #for the collision case, because we are not coordinator,
            #just let that round fail and propose a new value
            pvalue = self.value
        elif self.quorum.state == VPickQuorum.SINGLE or \
                self.quorum.state == VPickQuorum.COL_SINGLE:
            pvalue = self.quorum.outstanding
        else:
            raise ValueError('unknown state: %s' %self.quorum.state)
        self.logger.debug('%s got quorum.state=%s and to propose value=%s '
                          'in round=%s at %s'
                          %(self.ID, VPickQuorum.STATES[self.quorum.state],
                            pvalue, self.crnd, now()))
        return pvalue

    def _send2aMsg(self, pvalue):
        for acc in self.acceptors:
            self.sendMsg(acc, '2a', (self, self.instanceID, self.crnd, pvalue))

    def _checkValue(self):
        while True:
            events = []
            timeoutEvent = Alarm.setOnetime(self.timeout)
            events.append(timeoutEvent)
            events.append(self.learner.newInstanceEvent)
            yield waitevent, self, events
            if self.instanceID in self.learner.instances:
                break
            if timeoutEvent in self.eventsFired:
                maxrnd = self.learner.getQuorumMaxRnd(self.instanceID)
                if maxrnd > self.crnd:
                    raise RoundFailException
                else:
                    raise TimeoutException

    def fastPropose(self):
        while True:
            try:
                self._sendFastMsg()
                for step in self._checkValue():
                    yield step
                break
            except (RoundFailException, TimeoutException):
                pass

    def _sendFastMsg(self):
        for acc in self.acceptors:
            self.sendMsg(acc, 'propose', (self, self.instanceID, self.value))

    def run(self):
        if self.isFast:
            for step in self.fastPropose():
                yield step
        else:
            for step in self.propose():
                yield step
        if self.learner.instances[self.instanceID] == self.value:
            self.isSuccess = True
        self.prunner.finishedProposers.append(self)
        self.prunner.newFinishEvent.signal()

class PaxosResponse(object):
    def __init__(self):
        self.finishedEvent = SimEvent()
        self.instanceID = None

class ProposerRunner(IDable, Thread, MsgXeiver):
    """A thread that launches proposers."""
    def __init__(self, parent, rnd0, rndstep, acceptors, learner,
                 timeout=infinite, isFast=False, noPhase1=False,
                 iid0=0, iidstep=1):
        IDable.__init__(self, '%s/proprunner'%parent.ID)
        Thread.__init__(self)
        MsgXeiver.__init__(self, parent.inetAddr)
        assert rnd0 != 0        #saved for fast round and coordinator
        self.parent = parent
        self.rnd0 = rnd0
        self.rndstep = rndstep
        self.acceptors = acceptors
        self.learner = learner
        self.timeout = timeout
        self.isFast = isFast
        self.noPhase1 = noPhase1
        self.newRequestEvent = SimEvent()
        self.newFinishEvent = SimEvent()
        self.requests = []
        self.finishedProposers = []
        self.responses = {}
        self.iidstep = iidstep
        self.nextInstanceID = iid0 - iidstep
        self.closed = False
        self.logger = logging.getLogger(self.__class__.__name__)

    def close(self):
        self.closed = True

    def addRequest(self, value):
        self.requests.append(value)
        self.newRequestEvent.signal()
        assert value not in self.responses, \
                '%s received %s before' %(self.ID, value)
        response = PaxosResponse()
        self.responses[value] = response
        return response

    def getNextInstanceID(self):
        self.nextInstanceID += self.iidstep
        while self.nextInstanceID in self.learner.instances:
            self.nextInstanceID += self.iidstep
        return self.nextInstanceID

    def run(self):
        while not self.closed:
            while len(self.requests) > 0:
                value = self.requests.pop(0)
                instanceID = self.getNextInstanceID()
                proposer = Proposer(self, self.rnd0, self.rndstep,
                                    self.acceptors, self.learner,
                                    instanceID, value,
                                    self.timeout,
                                    self.isFast, self.noPhase1)
                proposer.start()
            while len(self.finishedProposers) > 0:
                prev = self.finishedProposers.pop(0)
                if not prev.isSuccess:
                    value = prev.value
                    instanceID = self.getNextInstanceID()
                    proposer = Proposer(self, self.rnd0, self.rndstep,
                                        self.acceptors, self.learner,
                                        instanceID, value,
                                        self.timeout,
                                        self.isFast, self.noPhase1)
                    proposer.start()
                else:
                    #success, notify the event and the instance
                    response = self.responses[prev.value]
                    response.instanceID = prev.instanceID
                    response.finishedEvent.signal()
            yield waitevent, self, (self.newRequestEvent, self.newFinishEvent)

def initPaxosCluster(pnodes, anodes, coordinatedRecovery, 
                     isFast, propPlacement, noPhase1, 
                     interleavedIID, timeout):
    #on each anode, there is an acceptor
    acceptors = []
    for anode in anodes:
        acc = Acceptor(anode, len(pnodes), coordinatedRecovery)
        acceptors.append(acc)
        anode.paxosAcceptor = acc
    #on each pnode, there is a learner
    learners = []
    for pnode in pnodes:
        lnr = Learner(pnode)
        learners.append(lnr)
        pnode.paxosLearner = lnr
    #on the first pnode, there is a coordinator
    coordinator = Coordinator(pnodes[0], len(pnodes), timeout)
    pnodes[0].paxosCoordinator = coordinator
    #initialization
    for acc in acceptors:
        acc.init(acceptors, learners, coordinator)
        acc.start()
    for lnr in learners:
        lnr.init(len(acceptors))
        lnr.start()
    coordinator.init(acceptors, learners[0])
    coordinator.start()
    #propose runners
    prunners = []
    if propPlacement == 'one':
        prunner = ProposerRunner(pnodes[0], 1, len(pnodes),
                                 acceptors, learners[0],
                                 timeout, isFast, noPhase1)
        prunners.append(prunner)
        pnodes[0].paxosPRunner = prunner
        prunner.start()
    elif propPlacement == 'all':
        for i, pnode in enumerate(pnodes):
            if not interleavedIID:
                prunner = ProposerRunner(pnode, i + 1, len(pnodes),
                                         acceptors, learners[i],
                                         timeout, isFast, noPhase1)
            else:
                prunner = ProposerRunner(pnode, i + 1, len(pnodes),
                                         acceptors, learners[i],
                                         timeout, isFast, noPhase1, 
                                         i, len(pnodes))
            prunners.append(prunner)
            pnode.paxosPRunner = prunner
            prunner.start()
    else:
        raise ValueError('unknown proposer placement policy: %s'
                         %propPlacement)

##### TEST #####
import random

class ANode(object):
    def __init__(self, i):
        self.inetAddr = 'anode'
        self.ID = 'anode%s'%i

class PNode(object):
    def __init__(self, i):
        self.inetAddr = 'pnode'
        self.ID = 'pnode%s'%i

class TestRunner(Thread):
    def __init__(self, values, prunners, threshold, interval):
        Thread.__init__(self)
        self.values = values
        self.prunners = prunners
        self.threshold = threshold
        self.interval = interval

    def run(self):
        while len(self.values) > 0:
            for prunner in self.prunners:
                if len(self.values) == 0:
                    break
                r = random.random()
                if r > self.threshold:
                    value = self.values.pop(0)
                    prunner.addRequest(value)
            yield hold, self, self.interval

NUM_PNODES = 5
NUM_ANODES = 7
NETWORK_CONFIG = {
    'nw.latency.within.zone' : ('uniform', -1, {'lb':10, 'ub':500}),
    'nw.latency.cross.zone' : ('uniform', -1, {'lb':10, 'ub':1000}),
}
THRESHOLD = 0.5
INTERVAL = 500
NUM_VALUES = 100

def initTest():
    initialize()
    RTI.initialize(NETWORK_CONFIG)
    pnodes = []
    for i in range(NUM_PNODES):
        pnode = PNode(i)
        pnodes.append(pnode)
    anodes = []
    for i in range(NUM_ANODES):
        anode = ANode(i)
        anodes.append(anode)
    values = []
    for i in range(NUM_VALUES):
        values.append('proposal-%s'%i)
    return pnodes, anodes, values

def verifyResult(learners):
    learner0 = learners[0]
    logging.info('=== learned values: ===')
    for key, val in learner0.instances.iteritems():
        logging.info('%s: %s'%(key, val))
        for lnr in learners:
            assert val == lnr.instances[key], \
                ('%s.instances[%s] = %s == %s = %s.instances[%s]'
                 %(learner0.ID, key, val, lnr.instances[key], lnr.ID, key))
    logging.info('=====  VERIFICATION PASSED =====')

def testClassicPaxos():
    logging.info('===== START TEST CLASSIC PAXOS =====')
    pnodes, anodes, values = initTest()
    initPaxosCluster(pnodes, anodes, False, False, 'all', False, False, 1500)
    prunners = []
    learners = []
    for pnode in pnodes:
        try:
            prunners.append(pnode.paxosPRunner)
        except AttributeError:
            pass
        learners.append(pnode.paxosLearner)
    testrunner = TestRunner(values, prunners, THRESHOLD, INTERVAL)
    testrunner.start()
    simulate(until=1000000)
    verifyResult(learners)
    logging.info('===== END TEST CLASSIC PAXOS =====')

def testClassicPaxosInterleavedIID():
    logging.info('===== START TEST CLASSIC PAXOS INTERLEAVEDIID =====')
    pnodes, anodes, values = initTest()
    initPaxosCluster(pnodes, anodes, False, False, 'all', False, True, 1500)
    prunners = []
    learners = []
    for pnode in pnodes:
        try:
            prunners.append(pnode.paxosPRunner)
        except AttributeError:
            pass
        learners.append(pnode.paxosLearner)
    testrunner = TestRunner(values, prunners, THRESHOLD, INTERVAL)
    testrunner.start()
    simulate(until=1000000)
    verifyResult(learners)
    logging.info('===== END TEST CLASSIC PAXOS INTERLEAVEDIID =====')

def testMultiplePaxos():
    logging.info('===== START TEST MULTI PAXOS =====')
    pnodes, anodes, values = initTest()
    initPaxosCluster(pnodes, anodes, False, False, 'one', True, False, 1500)
    prunners = []
    learners = []
    for pnode in pnodes:
        try:
            prunners.append(pnode.paxosPRunner)
        except AttributeError:
            pass
        learners.append(pnode.paxosLearner)
    testrunner = TestRunner(values, prunners, THRESHOLD, INTERVAL)
    testrunner.start()
    simulate(until=1000000)
    verifyResult(learners)
    logging.info('===== END TEST MULTI PAXOS =====')

def testMultiplePaxosInterleavedIID():
    logging.info('===== START TEST MULTIPLE PAXOS INTERLEAVEDIID =====')
    pnodes, anodes, values = initTest()
    initPaxosCluster(pnodes, anodes, False, False, 'all', True, True, 1500)
    prunners = []
    learners = []
    for pnode in pnodes:
        try:
            prunners.append(pnode.paxosPRunner)
        except AttributeError:
            pass
        learners.append(pnode.paxosLearner)
    testrunner = TestRunner(values, prunners, THRESHOLD, INTERVAL)
    testrunner.start()
    simulate(until=1000000)
    verifyResult(learners)
    logging.info('===== END TEST CLASSIC PAXOS INTERLEAVEDIID =====')

def testFastPaxosCoordinated():
    logging.info('===== START TEST FAST PAXOS COORDINATED=====')
    pnodes, anodes, values = initTest()
    initPaxosCluster(pnodes, anodes, True, True, 'all', False, False, 1500)
    prunners = []
    learners = []
    for pnode in pnodes:
        try:
            prunners.append(pnode.paxosPRunner)
        except AttributeError:
            pass
        learners.append(pnode.paxosLearner)
    testrunner = TestRunner(values, prunners, THRESHOLD, INTERVAL)
    testrunner.start()
    simulate(until=1000000)
    verifyResult(learners)
    logging.info('===== END TEST FAST PAXOS COORDINATED=====')

def testFastPaxosUncoordinated():
    logging.info('===== START TEST FAST PAXOS UNCOORDINATED=====')
    pnodes, anodes, values = initTest()
    initPaxosCluster(pnodes, anodes, False, True, 'all', False, False, 1500)
    prunners = []
    learners = []
    for pnode in pnodes:
        try:
            prunners.append(pnode.paxosPRunner)
        except AttributeError:
            pass
        learners.append(pnode.paxosLearner)
    testrunner = TestRunner(values, prunners, THRESHOLD, INTERVAL)
    testrunner.start()
    simulate(until=1000000)
    verifyResult(learners)
    logging.info('===== END TEST FAST PAXOS UNCOORDINATED=====')

def test():
    logging.basicConfig(level=logging.DEBUG)
    testClassicPaxos()
    testClassicPaxosInterleavedIID()
    testMultiplePaxos()
    testMultiplePaxosInterleavedIID()
    testFastPaxosCoordinated()
    testFastPaxosUncoordinated()

def main():
    test()

if __name__ == '__main__':
    main()
