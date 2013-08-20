import logging
import re

from SimPy.Simulation import Process, SimEvent
from SimPy.Simulation import initialize, activate, simulate, now
from SimPy.Simulation import waitevent, hold

from core import infinite, Alarm, RetVal, Thread, TimeoutException
from importutils import loadClass
from network import FixedLatencyNetwork

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
        networkClsName = configs.get('network.sim.class', 'network.FixedLatencyNetwork')
        networkCls = loadClass(networkClsName)
        RTI.networkInstance = networkCls(configs)

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

    class TimeoutException(Exception):
        """Notify timeout. """
        pass

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
            raise RTI.TimeoutException(
                'rm=%s, args=%s, timeout=%s'
                %(self._thread.rm, self._thread.args, timeout))

class MsgXeiver(RTI):
    """MsgXeiver can send and receive message.

    The MsgXeiver supports the following operations:
        send(tag, content)      --  send a message to other
        check(tag)              --  check if messages with @tag has arrived
        wait(tag)               --  wait until some message with @tag arrived
    """
    def __init__(self, inetAddr):
        RTI.__init__(self, inetAddr)
        self.messages = {}          #{tag: [content]}
        self.notifiers = {}         #{tag: event}
        self.tagIgnoreRe = None     #filtering message

    def _put(self, tag, content):
        if self.tagIgnoreRe and \
           self.tagIgnoreRe.match(tag):
            return
        if not self.messages.has_key(tag):
            self.messages[tag] = []
        self.messages[tag].append(content)
        if tag in self.notifiers:
            self.notifiers[tag].signal()

    def sendMsg(self, other, tag, content):
        self.invoke(other._put, tag, content).rtiCall()

    def checkMsg(self, tag):
        if tag in self.messages:
            return len(self.messages[tag])
        return False

    def waitMsg(self, tag, timeout=infinite):
        if self.checkMsg(tag):
            return
        if tag not in self.notifiers:
            self.notifiers[tag] = SimEvent()
        timeoutEvt = Alarm.setOnetime(timeout)
        yield waitevent, self, (self.notifiers[tag], timeoutEvt)
        if timeoutEvt in self.eventsFired:
            raise TimeoutException('rti.waitMsg', tag)
        del self.notifiers[tag]

    def getContents(self, tag):
        return self.messages.get(tag, [])

    def popContents(self, tag):
        queue = self.messages.get(tag, [])
        while len(queue) != 0:
            yield queue.pop(0)

#####  Test  #####

class Server(Thread, MsgXeiver):
    def __init__(self, addr):
        Thread.__init__(self)
        MsgXeiver.__init__(self, addr)

    def add(self, x, y):
        print ('server %s computing %s + %s at %s' 
               %(self.inetAddr, x, y, now()))
        return x + y

    def run(self):
        while True:
            if self.checkMsg('request'):
                for content in self.popContents('request'):
                    print ('server %s received request %s at %s'
                           %(self.inetAddr, content, now()))
            for step in self.waitMsg('request'):
                yield step

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
            except RTI.TimeoutException as e:
                print ('client %s timeout from %s at %s'
                       %(self.inetAddr, server.inetAddr, now()))
        #msgxeiver test
        for i in range(3):
            for server in self.servers:
                self.sendMsg(server, 'request', (2, 3))
                print ('client %s send request to %s at %s' 
                       %(self.inetAddr, server.inetAddr, now()))
                yield hold, self, 5

def test():
    configs = {
        'network.sim.class' : 'network.FixedLatencyNetwork',
        FixedLatencyNetwork.WITHIN_ZONE_LATENCY_KEY : 5,
        FixedLatencyNetwork.CROSS_ZONE_LATENCY_KEY : 30
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


def main():
    test()

if __name__ == '__main__':
    main()
