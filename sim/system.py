import logging
import random
import sys
import time
from Queue import PriorityQueue

import numpy as np
from SimPy.Simulation import Process, Resource, SimEvent
from SimPy.Simulation import activate, now, initialize, simulate
from SimPy.Simulation import waitevent, hold, request

from core import Alarm, IDable, Thread
from data import Dataset
from perf import Profiler
from rand import RandInterval
from rti import RTI

class BaseSystem(Thread):
    """Base system class.

    The system contains several zones. Each zone has a ClientNode and several
    StorageNodes. The basic functionality of ClientNode is accepting client
    requests and dispatch transactions to StorageNodes. StorageNode handles
    transactions and returns the result back to ClientNode.

    """
    RUNNING, CLOSING, CLOSED = range(3)
    TXN_EXEC_KEY_PREFIX = 'txn.exec'
    TXN_LOSS_KEY_PREFIX = 'txn.loss'
    def __init__(self, configs):
        Thread.__init__(self)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.configs = configs
        self.monitor = Profiler.getMonitor('system')
        #for correctness check
        self.dataset = Dataset(0, configs['dataset.groups'])
        #system components
        self.cnodes = []
        self.snodes = {}
        for i in range(configs['num.zones']):
            node = self.newClientNode(i, configs)
            self.cnodes.append(node)
        for i in range(configs['num.zones']):
            self.snodes[i] = []
            for j in range(configs['num.storage.nodes.per.zone']):
                node = self.newStorageNode(self.cnodes[i], j, configs)
                self.snodes[i].append(node)
        for i, cnode in enumerate(self.cnodes):
            cnode.addSNodes(self.snodes[i])
        self.initializeNodes()
        #txn execution
        self.txns = PriorityQueue()
        self.numTxnsSched = 0
        self.numTxnsArrive = 0
        self.numTxnsDepart = 0
        self.numTxnsLoss = 0
        self.state = None
        #for print progress
        self.simThr = self.configs.get('sim.progress.print.intvl.thr', 1000)
        self.realThr = self.configs.get('real.progress.print.intvl.thr', 10)
        self.lastPrintSimTime = 0
        self.lastPrintRealTime = 0

    def newClientNode(self, idx, configs):
        return ClientNode(self, idx, configs)

    def newStorageNode(self, cnode, index, configs):
        return StorageNode(cnode, index, configs)

    #schedule txn execution, called by generator
    def schedule(self, txn, at):
        self.txns.put((at, txn))
        self.numTxnsSched += 1
        self.logger.debug('scheduling %r at %s' %(txn, at))

    #txn arrive and depart metrics, called by cnodes
    def onTxnArrive(self, txn):
        self.numTxnsArrive += 1
        self.monitor.start('%s.%s'%(BaseSystem.TXN_EXEC_KEY_PREFIX, txn.ID))
        self.logger.debug('Txn %s arrive in system at %s, progress=A:%s/%s'
                         %(txn.ID, now(),
                           self.numTxnsArrive, self.numTxnsSched))

    def onTxnDepart(self, txn):
        self.numTxnsDepart += 1
        self.monitor.stop('%s.%s'%(BaseSystem.TXN_EXEC_KEY_PREFIX, txn.ID))
        self.logger.debug('Txn %s depart from system at %s, progress=D:%s/%s'
                         %(txn.ID, now(),
                           self.numTxnsDepart, self.numTxnsSched))

    def onTxnLoss(self, txn):
        self.numTxnsLoss += 1
        self.numTxnsArrive += 1
        self.numTxnsDepart += 1
        self.monitor.observe('%s.%s'%(BaseSystem.TXN_LOSS_KEY_PREFIX, txn.ID), 0)
        self.logger.debug('Txn %s loss from system at %s, loss rate=%s/%s'
                         %(txn.ID, now(),
                           self.numTxnsLoss, self.numTxnsSched))

    #initialize system components
    def initializeNodes(self):
        self.logger.info('Initializing nodes')
        for i, snodes in self.snodes.iteritems():
            dataset = Dataset(i, self.configs['dataset.groups'])
            cnode = self.cnodes[i]
            #assign groups in a round robin manner
            count = 0
            for gid, group in dataset.groups.iteritems():
                snode = snodes[count]
                cnode.groupLocations[gid] = snode
                snode.groups[gid] = group
                self.logger.info('storage %s hosts group %s'
                                 %(snode.ID, group))
                count += 1
                count = 0 if count == len(snodes) else count

    #system run, called by the sim main
    def run(self):
        #start client and storage nodes
        for cnode in self.cnodes:
            cnode.start()
        for i, snodes in self.snodes.iteritems():
            for snode in snodes:
                snode.start()
        self.state = BaseSystem.RUNNING
        #the big while loop
        while True:
            if self.state == BaseSystem.RUNNING:
                if not self.txns.empty():
                    #simulate txn arrive as scheduled
                    at, txn = self.txns.get()
                    while now() < at:
                        nextArrive = Alarm.setOnetime(at - now())
                        yield waitevent, self, nextArrive
                    cnode = self.cnodes[txn.zoneID]
                    cnode.onTxnArrive(txn)
                else:
                    self.state = BaseSystem.CLOSING
                    self.logger.info(
                        'system: all txns arrived, start closing at %s' %now())
                    for cnode in self.cnodes:
                        cnode.close()
            elif self.state == BaseSystem.CLOSING:
                #check if all closed
                closed = True
                for cnode in self.cnodes:
                    if not cnode.isFinished():
                        closed = False
                        break
                if closed:
                    self.state == BaseSystem.CLOSED
            elif self.state == BaseSystem.CLOSED:
                break
            else:
                raise ValueError('Unknown system status:' %self.state)
            #print progress
            self.printProgress()
            #sleep in sim world if closing
            if self.state == BaseSystem.CLOSING:
                sleep = Alarm.setOnetime(self.simThr)
                yield waitevent, self, sleep

    def printProgress(self):
        #do not overflood the output, so we only print when both the
        #simulation time and real time pass a certain threshold
        if (now() - self.lastPrintSimTime > self.simThr) and \
           (time.time() - self.lastPrintRealTime > self.realThr):
            self.logger.info('progress = %s/%s/%s'%(self.numTxnsArrive,
                                                    self.numTxnsDepart,
                                                    self.numTxnsSched))
            self.lastPrintSimTime = now()
            self.lastPrintRealTime = time.time()

    def profile(self):
        resMean, resStd, resHisto, resCount = \
                Profiler.getMonitor('system').getElapsedStats(
                    '.*%s'%BaseSystem.TXN_EXEC_KEY_PREFIX)
        self.logger.info('res.mean=%s'%resMean)
        self.logger.info('res.std=%s'%resStd)
        self.logger.info('res.histo=(%s,%s)'%(resHisto))
        loss = float(Profiler.getMonitor('system').getObservedCount(
            '.*%s'%BaseSystem.TXN_LOSS_KEY_PREFIX))
        lossRatio = loss / (resCount + loss)
        self.logger.info('loss.ratio=%s'%lossRatio)

class ClientNode(IDable, Thread, RTI):
    """Base client node.  

    The main functionality of this base client node class is dispatch txns to
    storage nodes.

    """
    def __init__(self, system, ID, configs):
        IDable.__init__(self, 'zone%s/cn'%ID)
        Thread.__init__(self)
        RTI.__init__(self, self.ID)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.system = system
        self.snodes = []
        self.configs = configs
        self.groupLocations = {}
        self.runningTxns = {}   #{txn : snode}
        self.snodeLoads = {}    #{snode : set([txns])}
        self.maxNumTxns = configs.get('max.num.txns.per.storage.node', 1024)
        self.shouldClose = False
        self.closeEvent = SimEvent()

    def addSNodes(self, snodes):
        self.snodes.extend(snodes)
        for snode in self.snodes:
            self.snodeLoads[snode] = set([])

    #notify new txn arrive, called by the system
    def onTxnArrive(self, txn):
        waitIfBusy = self.configs.get('txn.wait.if.snodes.busy', False)
        snode = self.dispatchTxn(txn, waitIfBusy)
        if snode:
            self.system.onTxnArrive(txn)
            self.runningTxns[txn] = snode
            self.snodeLoads[snode].add(txn)
        else:
            self.system.onTxnLoss(txn)

    #notify new txn depart, called by the storage nodes
    def onTxnDepart(self, txn):
        if txn not in self.runningTxns:
            #this is possible when multiple storage nodes handles the same
            #transaction.
            return
        snode =  self.runningTxns[txn]
        self.snodeLoads[snode].remove(txn)
        del self.runningTxns[txn]
        self.system.onTxnDepart(txn)

    def dispatchTxn(self, txn, waitIfBusy):
        #we assign txn to one of the storage nodes hosting the txn groups
        #if the storage node is busy, we find another one
        #if all is busy and waitIfBusy == false, we throw if
        #otherwise, we find the least loaded one
        self.logger.debug(
            '%s dispatch: load=%s, max=%s' 
            %(self.ID, '(%s)'%','.join(
                ['{%s:%s}' %(snode.ID, ','.join([t.ID for t in loads]))
                 for snode, loads in self.snodeLoads.iteritems()]),
                self.maxNumTxns))
        hosts = self.getTxnHosts(txn)
        for host in hosts:
            if len(self.snodeLoads[host]) < self.maxNumTxns:
                self.invoke(host.onTxnArrive, txn).rtiCall()
                self.logger.debug('%s local dispatch %s to %s at %s'
                                  %(self.ID, txn.ID, host.ID, now()))
                return host
        #all hosts are busy
        leastLoadedNode = iter(self.snodes).next()
        leastLoad = len(self.snodeLoads[leastLoadedNode])
        for snode in self.snodes:
            load = len(self.snodeLoads[leastLoadedNode])
            if leastLoad < load:
                leastLoad = load
                leastLoadedNode = snode
        if waitIfBusy or leastLoad < self.maxNumTxns:
            self.invoke(leastLoadedNode.onTxnArrive, txn).rtiCall()
            self.logger.debug('%s busy dispatch %s to %s at %s'
                             %(self.ID, txn.ID, leastLoadedNode, now()))
            return leastLoadedNode
        else:
            self.logger.debug('%s throw away %s at %s'
                             %(self.ID, txn.ID, now()))
            return None

    def getTxnHosts(self, txn):
        hosts = set([])
        for gid in txn.gids:
            hosts.add(self.groupLocations[gid])
        return hosts

    def close(self):
        self.shouldClose = True
        self.closeEvent.signal()

    def _close(self):
        #periodically check if we still have txn running
        while True:
            yield hold, self, 100
            if len(self.runningTxns) == 0:
                break
        for snode in self.groupLocations.values():
            snode.close()
        for snode in self.groupLocations.values():
            if not snode.isFinished():
                yield waitevent, self, snode.finish

    def run(self):
        while not self.shouldClose:
            yield waitevent, self, self.closeEvent
            if self.shouldClose:
                self._close()

class StorageNode(IDable, Thread, RTI):
    """Base storage node."""
    def __init__(self, cnode, ID, configs):
        IDable.__init__(self, '%s/sn%s'%(cnode.ID.split('/')[0], ID))
        Thread.__init__(self)
        RTI.__init__(self, self.ID)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.system = cnode.system
        self.configs = configs
        self.cnode = cnode
        self.maxNumTxns = configs.get('max.num.txns.per.storage.node', 1024)
        self.pool = Resource(self.maxNumTxns, name='pool', unitName='thread')
        self.groups = {}    #{gid : group}
        self.outstandingTxns = []
        self.runningTxns = set([])
        self.shouldClose = False
        self.monitor = Profiler.getMonitor(self.ID)
        self.M_POOL_WAIT_PREFIX = '%s.pool.wait' %self.ID
        self.M_TXN_RUN_PREFIX = '%s.txn.run' %self.ID
        self.runningThreads = set([])
        self.closeEvent = SimEvent()
        self.newTxnEvent = SimEvent()

    @property
    def load(self):
        return len(self.runningTxns) + len(self.outstandingTxns)

    def close(self):
        self.shouldClose = True
        self.closeEvent.signal()

    def isBusy(self):
        return self.load >= self.maxNumTxns

    def onTxnArrive(self, txn):
        self.outstandingTxns.append(txn)
        self.newTxnEvent.signal()

    def onTxnsArrive(self, txns):
        self.outstandingTxns.extend(txns)
        self.newTxnEvent.signal()

    def newTxnRunner(self, txn):
        class DefaultTxnRunner(Thread):
            def __init__(self, snode, txn):
                Thread.__init__(self)
                self.snode = snode
                self.txn = txn
                self.logger = logging.getLogger(self.__class__.__name__)
            
            def run(self):
                self.logger.debug('Running transaction %s at %s' 
                                  %(txn.ID, now()))
                yield hold, self, RandInterval.get('expo', 100)
        return DefaultTxnRunner(self, txn)

    class TxnStarter(Thread):
        def __init__(self, snode, txn):
            Thread.__init__(self)
            self.snode = snode
            self.txn = txn

        def run(self):
            #add self and txn to snode
            self.snode.runningThreads.add(self)
            #wait for pool thread resource if necessary
            #self.snode.logger.debug(
            #    '%s start txn=%s, running=%s, outstanding=%s' 
            #    %(self.snode, self.txn.ID,
            #      '(%s)'%(','.join([t.ID for t in self.snode.runningTxns])),
            #      '(%s)'%(','.join([t.ID for t in self.snode.outstandingTxns]))
            #     ))
            if self.snode.isBusy():
                self.snode.monitor.start(
                    '%s.%s'%(self.snode.M_POOL_WAIT_PREFIX, self.txn.ID))
                yield request, self, self.snode.pool
                self.snode.monitor.stop(
                    '%s.%s'%(self.snode.M_POOL_WAIT_PREFIX, self.txn.ID))
            #txn start running add to runningTxns
            self.snode.runningTxns.add(self.txn)
            #start runner and wait for it to finish
            thread = self.snode.newTxnRunner(self.txn)
            thread.start()
            self.snode.monitor.start(
                '%s.%s'%(self.snode.M_TXN_RUN_PREFIX, self.txn.ID))
            yield waitevent, self, thread.finish
            self.snode.monitor.stop(
                '%s.%s'%(self.snode.M_TXN_RUN_PREFIX, self.txn.ID))
            #clean up
            self.snode.runningTxns.remove(self.txn)
            self.snode.runningThreads.remove(self)
            self.snode.invoke(self.snode.cnode.onTxnDepart, self.txn).rtiCall()

    def run(self):
        #the big while loop
        while not self.shouldClose:
            yield waitevent, self, (self.closeEvent, self.newTxnEvent)
            while len(self.outstandingTxns) > 0:
                txn = self.outstandingTxns.pop(0)
                thread = StorageNode.TxnStarter(self, txn)
                thread.start()
            if self.shouldClose:
                self.logger.info(
                    '%s closing. Wait for threads to terminate at %s'
                    %(self.ID, now()))
                #wait for running threads to terminate and close
                for thread in self.runningThreads:
                    if not thread.isFinished():
                        yield waitevent, self, thread.finish

#####  TEST #####

TEST_TXN_ARRIVAL_PERIOD = 10000
TEST_NUM_TXNS = 1000

class FakeTxn(IDable):
    def __init__(self, ID, zoneID, gid):
        IDable.__init__(self, 'txn%s'%ID)
        self.zoneID = zoneID
        r = random.random()
        self.gids = set([gid])

def test():
    try:
        numZones = int(sys.argv[1])
        numSNodes = int(sys.argv[2])
    except:
        numZones = 2
        numSNodes = 2
    print numZones, numSNodes
    #initialize
    logging.basicConfig(level=logging.DEBUG)
    configs = {
        'network.sim.class' : 'network.FixedLatencyNetwork',
        'max.num.txns.per.storage.node' : 1,
        'fixed.latency.nw.within.zone' : 0,
        'fixed.latency.nw.cross.zone' : 0,
    }
    groups = {}
    for i in range(numSNodes):
        groups[i] = 128
    configs['dataset.groups'] = groups
    configs['num.zones'] = numZones
    configs['num.storage.nodes.per.zone'] = numSNodes
    initialize()
    RTI.initialize(configs)
    system = BaseSystem(configs)
    #txn generation
    curr = 0
    for i in range(TEST_NUM_TXNS):
        txnID = i
        zoneID = random.randint(0, configs['num.zones'] - 1)
        gid = random.randint(0, numSNodes - 1)
        txn = FakeTxn(txnID, zoneID, gid)
        at = curr + RandInterval.get('expo', TEST_TXN_ARRIVAL_PERIOD / TEST_NUM_TXNS)
        curr  = at
        system.schedule(txn, at)
        logging.info('txnID=%s, zoneID=%s, gids=%s at=%s'
                     %(txnID, zoneID, txn.gids, at))
    #start simulation
    system.start()
    simulate(until=2 * TEST_TXN_ARRIVAL_PERIOD)

    #profile
    system.profile()
    #calculate m/m/s loss rate
    lambd = float(TEST_NUM_TXNS) / TEST_TXN_ARRIVAL_PERIOD
    mu = 1 / float(configs['fixed.latency.nw.within.zone'] + 100)
    print erlangLoss(lambd / numZones / numSNodes, mu, 1)
    print erlangLoss(lambd / numZones, mu, numSNodes)

def erlangLoss(lambd, mu, s):
    import scipy as sp
    a = lambd / mu
    nom = a**s / sp.misc.factorial(s)
    don = 0
    for i in range(s + 1):
        don += a**i / sp.misc.factorial(i)
    return nom / don


def main():
    test()

if __name__ == '__main__':
    main()


