import logging
import numpy
import random
import re

from SimPy.Simulation import now

import sim
from core import IDable
from util import PrettyFloat

class StateMonitor(IDable):
    START, STOP, OBSERVE = range(3)
    STATESTR = ['start', 'stop', 'observe']
    def __init__(self, ID):
        IDable.__init__(self, ID)
        self.events = []
        self.timeline = []
        self.attrs = []
        #register self to profiler
        self.parent = None
        self.children = set([])

    def _append(self, name, state, time, attr):
        self.events.append((name, state))
        self.timeline.append(time)
        self.attrs.append(attr)

    def append(self, name, state, time, attr=None):
        self._append(name, state, time, attr)
        curr = self.parent
        while curr is not None:
            curr._append(name, state, time, attr)
            curr = curr.parent

    def start(self, name):
        name = '%s.%s'%(self.ID, name)
        time = now()
        self.append(name, StateMonitor.START, time)

    def stop(self, name):
        name = '%s.%s'%(self.ID, name)
        time = now()
        self.append(name, StateMonitor.STOP, time)

    def observe(self, name, attr):
        name = '%s.%s'%(self.ID, name)
        time = now()
        self.append(name, StateMonitor.OBSERVE, time, attr)

    def getElapsed(self, key):
        elapsed = []
        startTime = {}  #{key: start}
        for i in range(len(self.events)):
            name, state = self.events[i]
            if not re.match(key, name):
                continue
            if state != StateMonitor.START and state != StateMonitor.STOP:
                continue
            if (state == StateMonitor.START and name in startTime) or \
               (state == StateMonitor.STOP and name not in startTime):
                raise ValueError('State transition error: (%s, %s, %s)'
                                 %(name, StateMonitor.STATESTR[state],
                                   self.timeline[i]))
            if not name in startTime:
                startTime[name] = self.timeline[i]
            else:
                elapsed.append(self.timeline[i] - startTime[name])
                del startTime[name]
        return elapsed

    def getElapsedStats(self, key):
        elapsed = self.getElapsed(key)
        if len(elapsed) == 0:
            return 0, 0, ([], []), 0
        freqs, bins = numpy.histogram(elapsed)
        return numpy.mean(elapsed), numpy.std(elapsed), \
                (list(freqs), list(bins)), len(elapsed)

    def getElapsedMean(self, key):
        elapsed = self.getElapsed(key)
        if len(elapsed) == 0:
            return 0
        return numpy.mean(elapsed)

    def getElapsedCount(self, key):
        return len(self.getElapsed(key))

    def getObserved(self, key):
        times = []
        observed = []
        for i in range(len(self.events)):
            name, state = self.events[i]
            if not re.match(key, name):
                continue
            if state != StateMonitor.OBSERVE:
                continue
            times.append(self.timeline[i])
            observed.append(self.attrs[i])
        return times, observed

    def getObservedStats(self, key):
        times, observed = self.getObserved(key)
        if len(times) == 0:
            return 0, 0, ([], []), 0
        freqs, bins = numpy.histogram(observed)
        return numpy.mean(observed), numpy.std(observed), \
                (list(freqs), list(bins)), len(observed)

    def getObservedMean(self, key):
        times, observed = self.getObserved(key)
        if len(times) == 0:
            return 0
        return numpy.mean(observed)

    def getObservedCount(self, key):
        return len(self.getObserved(key)[1])

    def __repr__(self):
        return '%s:[%s]'%(self.ID, ', '.join(
            ['(%s, %s, %s)'
             %(self.events[i][0], StateMonitor.STATESTR[self.events[i][1]],
               self.timeline[i]) for i in range(len(self.timeline))]))

class SMTree(object):
    """State monitor tree."""
    def __init__(self):
        self.root = StateMonitor('/')

    def add(self, name):
        name = self.normalize(name)
        if name == '/':
            return self.root
        node, parent = self.find(name, self.root)
        if node is not None:
            return node
        node = StateMonitor(name)
        for child in list(parent.children):
            if self.isDescendant(name, child.ID):
                parent.children.remove(child)
                node.children.add(child)
                child.parent = node
        node.parent = parent
        parent.children.add(node)
        return node

    def normalize(self, name):
        return '/%s' %name.lstrip('/')

    def find(self, name, parent):
        """Find the position of name under parent."""
        if len(parent.children) == 0:
            return None, parent
        for child in parent.children:
            if child.ID == name:
                return child, parent
            if self.isDescendant(child.ID, name):
                return self.find(name, child)
        return None, parent

    def isDescendant(self, ancestor, descendent):
        #non-inclusive
        asubs = ancestor.split('/')
        dsubs = descendent.split('/')
        if len(dsubs) <= len(asubs):
            return False
        for i, subname in enumerate(asubs):
            if subname != dsubs[i]:
                return False
        return True

    def nameEquivalentTo(self, otherTree):
        return self.isNodeNameEquivalent(self.root, otherTree.root)

    def isNodeNameEquivalent(self, node1, node2):
        if node1.ID != node2.ID:
            return False
        for child1 in node1.children:
            found = False
            for child2 in node2.children:
                if self.isNodeNameEquivalent(child1, child2):
                    found = True
            if not found:
                return False
        return True

    def __str__(self):
        retstrings = []
        self.getNodeIDString(self.root, retstrings)
        return '\n'.join(retstrings)

    def __repr__(self):
        retstrings = []
        self.getNodeString(self.root, retstrings)
        return '\n'.join(retstrings)

    def getNodeIDString(self, node, ret):
        #ret.append(node.ID + ','.join([ch.ID for ch in node.children]))
        ret.append(node.ID)
        for child in node.children:
            self.getNodeIDString(child, ret)

    def getNodeString(self, node, ret):
        ret.append('%r'%node)
        for child in node.children:
            self.getNodeString(child, ret)

class Profiler(object):
    Instance = None
    @classmethod
    def getMonitor(cls, name):
        if Profiler.Instance is None:
            Profiler.Instance = Profiler()
        return Profiler.Instance._getMonitor(name)

    @classmethod
    def get(cls):
        if Profiler.Instance is None:
            Profiler.Instance = Profiler()
        return Profiler.Instance

    @classmethod
    def clear(cls):
        if Profiler.Instance is None:
            Profiler.Instance = Profiler()
        return Profiler.Instance._clear()

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.monitorTree = SMTree()

    def _getMonitor(self, name):
        return self.monitorTree.add(name)

    def _clear(self):
        self.monitorTree = SMTree()

#####  TEST #####
def test():
    names = [
        'zone0/cn',
        'zone0/sn0',
        'zone0/sn1',
        'zone0/sn0/tr-txn0',
        'zone0/sn0/tr-txn1',
        'zone0/sn0/tr-txn2',
        'zone0/sn1/tr-txn3',
        'zone0/sn1/tr-txn4',
        'zone0/sn1/tr-txn5',
        'zone1/cn',
        'zone1/sn0',
        'zone1/sn1',
        'zone1/sn0/tr-txn6',
        'zone1/sn0/tr-txn7',
        'zone1/sn0/tr-txn8',
        'zone1/sn1/tr-txn9',
        'zone1/sn1/tr-txn10',
        'zone1/sn1/tr-txn11',
    ]
    trees = []
    for name in names:
        Profiler.getMonitor(name)
        trees.append(Profiler.get().monitorTree)
    print trees[0]
    for i in range(3):
        Profiler.clear()
        toShuffle = list(names)
        while len(toShuffle) != 0:
            index = random.randint(0, len(toShuffle) - 1)
            name = toShuffle.pop(index)
            Profiler.getMonitor(name)
        trees.append(Profiler.get().monitorTree)
    for i in range(3):
        assert trees[0].nameEquivalentTo(trees[i + 1]), trees[i + 1]
    sn0Mon = Profiler.getMonitor('zone0/sn0')
    txn0Mon = Profiler.getMonitor('zone0/sn0/tr-txn0')
    txn1Mon = Profiler.getMonitor('zone0/sn0/tr-txn1')
    txn0Mon.start('exec.txn0')
    txn0Mon.stop('exec.txn0')
    txn1Mon.start('exec.txn1')
    txn1Mon.stop('exec.txn1')
    print '============='
    print '%r' %Profiler.get().monitorTree

def main():
    test()

if __name__ == '__main__':
    main()
