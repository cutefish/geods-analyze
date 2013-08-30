from core import IDable, Thread, infinite
from rti import MsgXeiver

class Proposer(IDable, Thread, MsgXeiver):
    """Paxos proposer."""
    def __init__(self, parent, rnd0, rndstep, acceptors, timeout=infinite):
        IDable.__init_(self, '%s/proposer'%(parent.ID))
        Thread.__init__(self)
        MsgXeiver.__init__(self, parent.inetAddr)
        self.parent = parent
        self.rndstep = rndstep
        self.currRnd = rnd0
        self.acceptors = acceptors
        self.instances = {}
        self.timeout = timeout
        self.qsize = len(self.acceptors) / 2 + 1

    def propose(self, instanceID, value):
        self.currRnd = 0
        while True:
            try:
                self._send1aMsg(instanceID)
                pvalue = self._recv1bMsg(instanceID, value)
                self._send2aMsg(instanceID, pvalue)
                self._recv2bMsg(instanceID)
                assert instanceID not in self.instances, \
                        'instances[%s] = %s'%(
                            instanceID, self.instances[instanceID])
                self.instances[instanceID] = pvalue
                break
            except TimeoutException:
                self.currRnd += self.rndstep

    def _send1aMsg(self, instanceID):
        for acc in self.acceptors:
            self.sendMsg(acc, 'paxos 1a', (self, instanceID, self.currRnd))

    def _recv1bMsg(self, instanceID, pvalue):
        messages = {}
        while True:
            for content in self.popContents('paxos %s 1b'%instanceID):
                acc, crnd, vrnd, value = content
                assert vrnd < crnd, \
                        'vrndNo = %s < %s = rndNo' %(vrnd, crnd)
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
                    #ignore previous round messages
                    assert (crnd - self.rnd0) % self.rndstep == 0, \
                            '(crnd=%s - rnd0=%s)%rndstep == 0' %(
                                crnd, self.rnd0, self.rndstep)
                if len(messages) >= self.qsize:
                    break
                for step in self.waitMsg(
                    'paxos %s 1b'%instanceID, self.timeout):
                    yield step
        #prepare to send the phase 2a message
        maxvrnd = 0
        value = None
        for msg in messages:
            vr, v = msg
            if vr > maxvrnd:
                maxvrnd = vr
                value = v
            elif vr == maxvrnd:
                assert value is None or value == v, \
                        'value=%s == %s or None'%(value, v)
        if value is None:
            value = pvalue
        return value

    def _send2aMsg(self, instanceID, value):
        for acc in self.acceptors:
            self.sendMsg(acc, 'paxos 2a', (self, instanceID, self.currRnd, value))

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
    def __init__(self, parent):
        IDable.__init__(self, '%s/acceptor'%(parent.ID))
        Thread.__init__(self)
        MsgXeiver.__init__(self, parent.inetAddr)
        self.parent = parent
        self.rndNo = {}
        self.vrnd = {}
        self.value = {}
        self.close = False

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
            if instanceID not in self.rndNo:
                self.rndNo[instanceID] = -1
            if self.rndNo[instanceID] <= crnd:
                #accept the value
                self.vrnd[instanceID] = crnd
                self.value[instanceID] = value
                self.sendMsg(proposer, 'paxos %s 2b'%instanceID,
                             (self, True))
            else:
                self.sendMsg(proposer, 'paxos %s 2b'%instanceID,
                             (self, False))
