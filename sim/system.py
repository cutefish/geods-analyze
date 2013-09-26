import logging
import random
import sys
import time
from Queue import PriorityQueue

import numpy as np
from SimPy.Simulation import Process, Resource, SimEvent
from SimPy.Simulation import activate, now, initialize, simulate
from SimPy.Simulation import waitevent, hold, request, release

import sim
from sim.core import Alarm, IDable, Thread, infinite
from sim.data import Dataset
from sim.paxos import initPaxosCluster
from sim.perf import Profiler
from sim.rand import RandInterval
from sim.rti import RTI

class BaseSystem(Thread):
    """Base system class.

    The system contains several zones. Each zone has a ClientNode and several
    StorageNodes. The basic functionality of ClientNode is accepting client
    requests, dispatch transactions to StorageNodes and run paxos protocols.
    StorageNode handles transactions and returns the result back to ClientNode.

    """
    RUNNING, CLOSING, CLOSED = range(3)
    TXN_EXEC_KEY_PREFIX = 'txn.exec'
    TXN_LOSS_KEY_PREFIX = 'txn.loss'
    def __init__(self, configs):
        Thread.__init__(self)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.configs = configs
        self.monitor = Profiler.getMonitor('system')
        #system components
        self.cnodes = []
        self.snodes = {}
        self.createNodes()
        self.initializeStorage()
        #for correctness check
        self.dataset = Dataset(len(self.cnodes), configs['dataset.groups'])
        #txn execution
        self.allowOverLoad = configs.get('system.allow.overload', False)
        self.maxNumTxns = configs.get('max.num.txns.in.system', 1024)
        self.txnsToRun = PriorityQueue()
        self.txnsRunning = set([])
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

    def createNodes(self):
        for i in range(self.configs['num.zones']):
            node = self.newClientNode(i, self.configs)
            self.cnodes.append(node)
        for i in range(self.configs['num.zones']):
            self.snodes[i] = []
            for j in range(self.configs['num.storage.nodes.per.zone']):
                node = self.newStorageNode(self.cnodes[i], j, self.configs)
                self.snodes[i].append(node)
        for i, cnode in enumerate(self.cnodes):
            cnode.addSNodes(self.snodes[i])

    def newClientNode(self, idx, configs):
        return ClientNode(self, idx, configs)

    def newStorageNode(self, cnode, index, configs):
        return StorageNode(cnode, index, configs)

    def initializeStorage(self):
        self.logger.info('Initializing storage')
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

    #schedule txn execution, called by generator
    def schedule(self, txn, at):
        self.txnsToRun.put((at, txn))
        self.numTxnsSched += 1
        self.logger.debug('scheduling %r at %s' %(txn, at))

    #txn arrive and depart metrics, called by cnodes
    def onTxnArrive(self, txn):
        self.txnsRunning.add(txn)
        self.monitor.start('%s.%s'%(BaseSystem.TXN_EXEC_KEY_PREFIX, txn.ID))
        self.logger.debug('Txn %s arrive in system at %s, progress=A:%s/%s'
                         %(txn.ID, now(),
                           self.numTxnsArrive, self.numTxnsSched))

    def onTxnLoss(self, txn):
        self.numTxnsLoss += 1
        self.numTxnsDepart += 1
        self.monitor.observe('%s.%s'%(BaseSystem.TXN_LOSS_KEY_PREFIX, txn.ID), 0)
        self.logger.debug('Txn %s loss from system at %s, loss rate=%s/%s'
                         %(txn.ID, now(),
                           self.numTxnsLoss, self.numTxnsSched))

    def onTxnDepart(self, txn):
        if txn in self.txnsRunning:
            self.numTxnsDepart += 1
            self.monitor.stop('%s.%s'%(BaseSystem.TXN_EXEC_KEY_PREFIX, txn.ID))
            self.logger.debug('Txn %s depart from system at %s, progress=D:%s/%s'
                              %(txn.ID, now(),
                                self.numTxnsDepart, self.numTxnsSched))
            self.txnsRunning.remove(txn)

    #system run, called by the sim main
    def run(self):
        #start client and storage nodes
        self.startupNodes()
        self.startupPaxos()
        #the big while loop
        while True:
            if self.state == BaseSystem.RUNNING:
                if not self.txnsToRun.empty():
                    #simulate txn arrive as scheduled
                    at, txn = self.txnsToRun.get()
                    while now() < at:
                        nextArrive = Alarm.setOnetime(at - now())
                        yield waitevent, self, nextArrive
                        if now() < at:
                            continue
                        self.numTxnsArrive += 1
                        if self.allowOverLoad or \
                           len(self.txnsRunning) < self.maxNumTxns:
                            cnode = self.cnodes[txn.zoneID]
                            cnode.onTxnArrive(txn)
                        else:
                            self.onTxnLoss(txn)
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

    def startupNodes(self):
        for cnode in self.cnodes:
            cnode.start()
        for i, snodes in self.snodes.iteritems():
            for snode in snodes:
                snode.start()
        self.state = BaseSystem.RUNNING

    def startupPaxos(self):
        initPaxosCluster(
            self.cnodes, self.cnodes, False, False, 'all', True, True, infinite)

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
        loadMean, loadStd, loadHisto, loadCount = \
                Profiler.getMonitor('/').getObservedStats('.*.num.txns')
        self.logger.info('load.mean=%s'%loadMean)
        self.logger.info('load.std=%s'%loadStd)
        self.logger.info('load.histo=(%s,%s)'%(loadHisto))

    def printMonitor(self):
        self.logger.debug('monitor: %r' %(Profiler.getMonitor('system')))

class ClientNode(IDable, Thread, RTI):
    """Base client node.  

    Base client node accepts txn requests and dispatch them to storage nodes.
    They are also hosts of paxos protocol entities.

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
        self.txnsRunning = set([])
        self.shouldClose = False
        self.closeEvent = SimEvent()
        #paxos entities
        self.paxosPRunner = None
        self.paxosAcceptor = None
        self.paxosLearner = None

    def addSNodes(self, snodes):
        self.snodes.extend(snodes)

    #notify new txn arrive, called by the system
    def onTxnArrive(self, txn):
        self.system.onTxnArrive(txn)
        self.txnsRunning.add(txn)
        self.dispatchTxn(txn)

    #notify new txn depart, called by the storage nodes
    def onTxnDepart(self, txn):
        if txn in self.txnsRunning:
            self.txnsRunning.remove(txn)
            self.system.onTxnDepart(txn)

    def dispatchTxn(self, txn):
        #just basic load balance
        hosts = self.getTxnHosts(txn)
        bestHost = iter(hosts).next()
        leastLoad = bestHost.load
        for host in hosts:
            if host.load < leastLoad:
                leastLoad = host.load
                bestHost = host
        bestHost.onTxnArrive(txn)
        self.logger.debug('%s dispatch %s to %s at %s'
                          %(self.ID, txn.ID, bestHost, now()))
        return bestHost

    def getTxnHosts(self, txn):
        hosts = set([])
        for gid in txn.gids:
            hosts.add(self.groupLocations[gid])
        return hosts

    def close(self):
        self.logger.info('Closing %s at %s'%(self, now()))
        self.shouldClose = True
        self.closeEvent.signal()

    def _close(self):
        ##periodically check if we still have txn running
        #while True:
        #    yield hold, self, 100
        #    if len(self.txnsRunning) == 0:
        #        break
        for snode in self.groupLocations.values():
            snode.close()
        for snode in self.groupLocations.values():
            if not snode.isFinished():
                yield waitevent, self, snode.finish
        try:
            self.paxosPRunner.close()
            self.paxosAcceptor.close()
            self.paxosLearner.close()
        except:
            pass

    def run(self):
        while not self.shouldClose:
            yield waitevent, self, self.closeEvent
            if self.shouldClose:
                for step in self._close():
                    yield step

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
        self.newTxns = []
        self.txnsRunning = set([])
        self.shouldClose = False
        self.monitor = Profiler.getMonitor(self.ID)
        self.M_POOL_WAIT_PREFIX = '%s.pool.wait' %self.ID
        self.M_TXN_RUN_PREFIX = '%s.txn.run' %self.ID
        self.M_NUM_TXNS_RUN_KEY = '%s.num.txns'%self.ID
        self.runningThreads = set([])
        self.closeEvent = SimEvent()
        self.newTxnEvent = SimEvent()

    @property
    def load(self):
        return len(self.txnsRunning) + len(self.newTxns)

    def close(self):
        self.logger.info('Closing %s at %s'%(self, now()))
        self.shouldClose = True
        self.closeEvent.signal()

    def onTxnArrive(self, txn):
        self.newTxns.append(txn)
        self.newTxnEvent.signal()

    def onTxnsArrive(self, txns):
        self.newTxns.extend(txns)
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
                yield hold, self, RandInterval.get('expo', 100).next()
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
            #      '(%s)'%(','.join([t.ID for t in self.snode.txnsRunning])),
            #      '(%s)'%(','.join([t.ID for t in self.snode.newTxns]))
            #     ))
            self.snode.monitor.start(
                '%s.%s'%(self.snode.M_POOL_WAIT_PREFIX, self.txn.ID))
            yield request, self, self.snode.pool
            self.snode.monitor.stop(
                '%s.%s'%(self.snode.M_POOL_WAIT_PREFIX, self.txn.ID))
            #start runner and wait for it to finish
            thread = self.snode.newTxnRunner(self.txn)
            assert self.txn not in self.snode.txnsRunning, \
                    '%s already started txn %s'%(self.snode, self.txn)
            self.snode.txnsRunning.add(self.txn)
            self.snode.monitor.observe(self.snode.M_NUM_TXNS_RUN_KEY,
                                       len(self.snode.txnsRunning))
            self.snode.monitor.start(
                '%s.%s'%(self.snode.M_TXN_RUN_PREFIX, self.txn.ID))
            thread.start()
            yield waitevent, self, thread.finish
            self.snode.monitor.stop(
                '%s.%s'%(self.snode.M_TXN_RUN_PREFIX, self.txn.ID))
            yield release, self, self.snode.pool  
            #clean up
            self.snode.txnsRunning.remove(self.txn)
            self.snode.runningThreads.remove(self)
            self.snode.cnode.onTxnDepart(self.txn)

    def run(self):
        #the big while loop
        while True:
            yield waitevent, self, self.newTxnEvent
            while len(self.newTxns) > 0:
                #pop from new txn to running txn
                txn = self.newTxns.pop(0)
                #start
                thread = StorageNode.TxnStarter(self, txn)
                thread.start()
            #if self.shouldClose:
            #    self.logger.info(
            #        '%s closing. Wait for threads to terminate at %s'
            #        %(self.ID, now()))
            #    #wait for running threads to terminate and close
            #    for thread in list(self.runningThreads):
            #        if not thread.isFinished():
            #            yield waitevent, self, thread.finish
            #    break

#####  TEST #####

TEST_TXN_ARRIVAL_PERIOD = 10000
TEST_NUM_TXNS = 1000

class FakeTxn(IDable):
    def __init__(self, ID, zoneID, gid):
        IDable.__init__(self, 'txn%s'%ID)
        self.zoneID = zoneID
        r = random.random()
        self.gids = set([gid])

    def __repr__(self):
        return self.ID

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
        'max.num.txns.per.storage.node' : 1,
        'nw.latency.within.zone' : ('fixed', 0),
        'nw.latency.cross.zone' : ('fixed', 0),
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
        at = curr + RandInterval.get(
            'expo', TEST_TXN_ARRIVAL_PERIOD / TEST_NUM_TXNS).next()
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
    mu = 1 / float(100)
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


