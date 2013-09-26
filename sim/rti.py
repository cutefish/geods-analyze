import logging
import re

from SimPy.Simulation import Process, SimEvent
from SimPy.Simulation import initialize, activate, simulate, now
from SimPy.Simulation import waitevent, hold

import sim
from sim.core import infinite, Alarm, RetVal, Thread, TimeoutException
from sim.importutils import loadClass
from sim.network import IIDLatencyNetwork

class RTI(object):
    """Remote State Transition Interface.

    The RTI interface simulates a process send a message that calls a remote
    object's method that transits its state. This is a similar concept like MPI
    and RPC but in a much more simplified way: 

        (1) the interface invoke a remote object's method, the remote method
        changes the internal state but the transit does not have any simulated
        cost on the remote object; 

        (2) the network effect on this invocation is simulated as that the
        invocation of the remote object's method happens sometime in the future
        according to the network.

        (3) the system effects such as message buffering is not simulated,
        therefore, there is no simulated cost on both src and dst objects and
        invocations are always in order(as opposed to mpi model).

        (4) the invocation may or may not wait for a round trip, which depends
        on the method used for the invocation.

    Before using this interface, it must be initialized using:
        RTI.initialize(configs)

    To send an invocation request and returns immediately, use:
        self.invoke(remoteObject.function, args).rtiCall(**kargs)
    
    To waits for the return of the invocation use:
        for step in self.invoke(remoteObject.function, args).rtiWait(**kargs):
            yield step

    **kargs includes:
        fwPktSize, bwPktSize and timeout

    """
    networkInstance = None
    @classmethod
    def initialize(cls, configs):
        RTI.networkInstance = IIDLatencyNetwork(configs)

    class AnonymousThread(Thread):
        """
        
        Because we want to change the state of the object in a future time
        which depends on the src and dst system and network, we create an
        anonymous thread to handle the events such as buffering or network
        routing. 

        """
        def __init__(self, parent, remoteMethod, *args):
            Thread.__init__(self)
            self.parent = parent
            self.rm = remoteMethod
            self.args = args
            self.roundtrip = False
            self.fwPktSize = 0
            self.bwPktSize = 0

        def run(self):
            srcAddr = self.parent.inetAddr
            dstAddr = self.rm.im_self.inetAddr
            #send a packet through network
            for step in RTI.networkInstance.sendPacket(
                self, srcAddr, dstAddr, self.fwPktSize):
                yield step
            #invoke the remote object's method
            if self.roundtrip:
                self.parent.rtiRVal.set(self.rm(*self.args))
            else:
                self.rm(*self.args)
            if self.roundtrip:
                for step in RTI.networkInstance.sendPacket(
                    self, dstAddr, srcAddr, self.bwPktSize):
                    yield step

    def __init__(self, inetAddr):
        self.inetAddr = inetAddr
        self._thread = None
        self.rtiRVal = RetVal()

    def invoke(self, remoteMethod, *args):
        self._thread = RTI.AnonymousThread(self, remoteMethod, *args)
        return self

    def rtiCall(self, fwPktSize=0):
        self._thread.fwPktSize = fwPktSize
        self._thread.start()

    def rtiWait(self, fwPktSize=0, bwPktSize=0, timeout=infinite):
        self._thread.roundtrip = True
        self._thread.fwPktSize = fwPktSize
        self._thread.bwPktSize = bwPktSize
        self._thread.start()
        tmoutEvt = Alarm.setOnetime(timeout)
        yield waitevent, self, (self._thread.finish, tmoutEvt)
        if tmoutEvt in self.eventsFired:
            raise TimeoutException(
                'rm=%s, args=%s, timeout=%s'
                %(self._thread.rm, self._thread.args, timeout))

class DLLNode(object):
    """A double-linked list node for lru cache."""
    def __init__(self, this):
        self.this = this
        self.prev = None
        self.next = None

class DLList(object):
    """A double-linked list."""
    def __init__(self):
        self.head = None
        self.tail = None

    def append(self, node):
        assert isinstance(node, DLLNode)
        if self.head is None:
            assert self.tail is None
            self.head = node
        else:
            self.tail.next = node
            node.prev = self.tail
        self.tail = node

    def remove(self, node):
        assert isinstance(node, DLLNode)
        if self.head is node:
            if self.tail is node:
                #node is the only element
                self.head = None
                self.tail = None
            else:
                #node is the head
                node.next.prev = None
                self.head = node.next
        elif self.tail is node:
            node.prev.next = None
            self.tail = node.prev
        else:
            node.prev.next = node.next
            node.next.prev = node.prev
        node.prev = None
        node.next = None

    def __repr__(self):
        reprstrs = []
        curr = self.head
        while curr is not None:
            reprstrs.append(str(curr.this))
            reprstrs.append(' -> ')
            if curr.next is None:
                break
            curr = curr.next
        return ''.join(reprstrs)

class MsgXeiver(RTI):
    """MsgXeiver can send and receive message.

    The MsgXeiver supports the following operations:
        send(tag, content)      --  send a message to other
        check(tag)              --  check if messages with @tag has arrived
        wait(tag)               --  wait until some message with @tag arrived
    """
    def __init__(self, inetAddr, maxntags=1000, tagbufsize=1000):
        RTI.__init__(self, inetAddr)
        self.rtiMessages = {}          #{tag: [content]}
        self.rtiTagUseQ = DLList()     #keep the tag put in order
        self.rtiTagNodes = {}          #tag -> DDLNode
        self.rtiNotifiers = {}         #{tag: event}
        self.rtiMaxntags = maxntags
        self.rtiTagbufsize = tagbufsize

    def _put(self, tag, content):
        #add the tag
        if not self.rtiMessages.has_key(tag):
            self.rtiMessages[tag] = []
            node = DLLNode(tag)
            self.rtiTagNodes[tag] = node
            self.rtiTagUseQ.append(node)
        else:
            node = self.rtiTagNodes[tag]
            self.rtiTagUseQ.remove(node)
            self.rtiTagUseQ.append(node)
        #add the content
        self.rtiMessages[tag].append(content)
        #keep the message buffer under certain size
        while len(self.rtiMessages[tag]) > self.rtiTagbufsize:
            self.rtiMessages[tag].pop(0)
        if tag in self.rtiNotifiers:
            self.rtiNotifiers[tag].signal()
        #keep tag number under certain size
        while len(self.rtiMessages) > self.rtiMaxntags:
            #remove empty tags
            for t in self.rtiMessages.keys():
                if len(self.rtiMessages[t]) == 0:
                    node = self.rtiTagNodes[t]
                    self.rtiTagUseQ.remove(node)
                    del self.rtiTagNodes[t]
                    del self.rtiMessages[t]
            #remove tags that is least recently been put
            head = self.rtiTagUseQ.head
            toRemove = head.this
            self.rtiTagUseQ.remove(head)
            del self.rtiTagNodes[toRemove]
            del self.rtiMessages[toRemove]

    def sendMsg(self, other, tag, content):
        self.invoke(other._put, tag, content).rtiCall()

    def checkMsg(self, tags):
        if not (isinstance(tags, list) or isinstance(tags, tuple)):
            tags  = (tags, )
        for tag in tags:
            if tag in self.rtiMessages:
                return len(self.rtiMessages[tag])
        return 0

    def waitMsg(self, tags, timeout=infinite):
        if not (isinstance(tags, list) or isinstance(tags, tuple)):
            tags  = (tags, )
        if self.checkMsg(tags):
            return
        events = self.getWaitMsgEvents(tags)
        timeoutEvt = Alarm.setOnetime(timeout)
        events.append(timeoutEvt)
        yield waitevent, self, events
        if timeoutEvt in self.eventsFired:
            raise TimeoutException('rti.waitMsg', tags)
        for tag in tags:
            del self.rtiNotifiers[tag]

    def getWaitMsgEvents(self, tags):
        events = []
        if not (isinstance(tags, list) or isinstance(tags, tuple)):
            tags  = (tags, )
        for tag in tags:
            if tag not in self.rtiNotifiers:
                event = SimEvent()
                self.rtiNotifiers[tag] = event
            event = self.rtiNotifiers[tag]
            events.append(event)
        return events

    def popContents(self, tag):
        queue = self.rtiMessages.get(tag, [])
        while len(queue) != 0:
            yield queue.pop(0)

#####  Test  #####

class Server(Thread, MsgXeiver):
    def __init__(self, addr):
        Thread.__init__(self)
        MsgXeiver.__init__(self, addr, maxntags=5)

    def add(self, x, y):
        print ('server %s computing %s + %s at %s' 
               %(self.inetAddr, x, y, now()))
        return x + y

    def run(self):
        while now() < 100:
            if self.checkMsg('request'):
                for content in self.popContents('request'):
                    print ('server %s received request %s at %s'
                           %(self.inetAddr, content, now()))
            for step in self.waitMsg('request'):
                yield step
        while now() < 200:
            yield hold, self, 50
            print '%s, messages: %s'%(self.inetAddr, self.rtiMessages)

class Client(Thread, MsgXeiver):
    def __init__(self, addr, servers):
        Thread.__init__(self)
        MsgXeiver.__init__(self, addr)
        self.servers = servers

    def run(self):
        #rti test
        for server in self.servers:
            print ('client %s call %s at %s' 
                   %(self.inetAddr, server.inetAddr, now()))
            self.invoke(server.add, 2, 3).rtiCall()
            yield hold, self, 10
            print ('client %s wait %s at %s' 
                   %(self.inetAddr, server.inetAddr, now()))
            try:
                for step in self.invoke(server.add, 2, 3).rtiWait(timeout=50):
                    yield step
                print ('client %s get result %s from %s at %s' 
                       %(self.inetAddr, self.rtiRVal.get(),
                         server.inetAddr, now()))
            except TimeoutException as e:
                print ('client %s timeout from %s at %s'
                       %(self.inetAddr, server.inetAddr, now()))
        #msgxeiver test
        for i in range(3):
            for server in self.servers:
                self.sendMsg(server, 'request', (2, 3))
                print ('client %s send request to %s at %s' 
                       %(self.inetAddr, server.inetAddr, now()))
                yield hold, self, 5
        #test msg buf tag lru
        for i in range(10):
            for server in self.servers:
                self.sendMsg(server, i, 'request')
                print ('client %s send tag %s to %s at %s' 
                       %(self.inetAddr, i, server.inetAddr, now()))
                yield hold, self, 5

def testRTI():
    print '\n>>> testRTI\n'
    configs = {
        'nw.latency.within.zone' : ('uniform', 10, {'lb' : 5, 'ub' : 15}),
        'nw.latency.cross.zone' : ('norm', 100,
                                   {'lb' : 50, 'ub' : 150, 'sigma' : 100}),
    }
    initialize()
    RTI.initialize(configs)
    server0 = Server('zone0/server')
    server1 = Server('zone1/server')
    client = Client('zone0/client', [server0, server1])
    server0.start()
    server1.start()
    client.start()
    simulate(until=1000)

def testDLL():
    print '\n>>> testDLL\n'
    import random
    nodes = []
    for i in range(10):
        nodes.append(DLLNode(i))
    dll = DLList()
    for node in nodes:
        dll.append(node)
        print 'append %s'%node.this
        print dll
    while len(nodes) > 0:
        r = random.randint(0, len(nodes) - 1)
        node = nodes[r]
        nodes.remove(node)
        dll.remove(node)
        print 'remove %s'%node.this
        print dll

def test():
    testDLL()
    testRTI()


def main():
    test()

if __name__ == '__main__':
    main()
