from SimPy.Simulation import Process, SimEvent
from SimPy.Simulation import hold, waitevent
from SimPy.Simulation import initialize, activate, simulate, now

from rintvl import RandInterval

infinite = -1

class IDable(object):
    def __init__(self, ID):
        self.ID = ID

    def __eq__(self, other):
        if isinstance(other, IDable):
            return self.ID == other.ID
        return False

    def __ne__(self, other):
        if isinstance(other, IDable):
            return self.ID != other.ID
        return True

    def __hash__(self):
        return self.ID.__hash__()

    def __str__(self):
        return str(self.ID)

class RetVal(object):
    def __init__(self):
        self._val = None
        self._isset = False

    @property
    def isset(self):
        return self._isset

    def set(self, val):
        self._val = val
        self._isset = True

    def get(self):
        if not self._isset:
            raise ValueError('Value not set')
        return self._val

    def __eq__(self, val):
        if not self._isset:
            return False
        else:
            return self._val == val

    def __hash__(self):
        return hash(self._val) << 1 + hash(self._isset)

    def __cmp__(self, val):
        if not self._isset:
            return -1
        else:
            return cmp(self._val, val)

class Alarm(Process):
    @classmethod
    def setOnetime(cls, delay, name=None):
        tm = Alarm(delay, name)
        activate(tm, tm.onetime())
        return tm.event

    @classmethod
    def setPeriodic(cls, interval, name=None, at=0,
                    until=infinite, drift=('fixed', 0, {})):
        tm = Alarm(interval, name, until, drift)
        activate(tm, tm.loop(), at=at)
        return tm.event

    def __init__(self, interval, name=None,
                 until=infinite, drift=('fixed', 0)):
        Process.__init__(self)
        self.interval = interval
        if name is not None:
            eventname = name
        else:
            eventname = "a_SimEvent"
        self.event = SimEvent(eventname)
        self.until = until
        try:
            key, mean, cfg = drift
        except ValueError:
            key, mean = drift
            cfg = {}
        lb = cfg.get('lb', 0); ub = cfg.get('ub', interval)
        if lb < 0: raise ValueError('drift lb = %s >= 0' %lb)
        if ub > interval:
            raise ValueError('drift ub = %s < %s = interval' %interval)
        cfg['lb'] = lb; cfg['ub'] = ub
        self.rgen = RandInterval.get(key, mean, cfg)

    def onetime(self):
        yield hold, self, self.interval
        self.event.signal()

    def loop(self):
        left = 0
        while (self.until < 0) or (now() < self.until):
            yield hold, self, left
            wait = self.rgen.next()
            yield hold, self, wait
            self.event.signal()
            left = self.interval - wait

class TimeoutException(Exception):
    pass

class Thread(Process):
    """Thread object.

    Threads have no fundamental difference with process except some convenient
    wrapper functions.

    """
    def __init__(self):
        Process.__init__(self)
        self._signals = {
            'kill' : SimEvent('kill'),
            'finish' : SimEvent('finish'),
        }
        self._finished = False

    def run(self):
        """Override for inherited class. """
        yield hold, self

    def _execute(self):
        for step in self.run():
            yield step
        self._signals['finish'].signal()
        self._finished = True

    def start(self):
        activate(self, self._execute())

    def kill(self):
        self._signals['kill'].signal()

    def isFinished(self):
        return self._finished

    @property
    def finish(self):
        return self._signals['finish']

class BThread(Thread):
    """Blocking thread.

    A blocking thread can be blocked by some other blocking thread. Therefore,
    a deadlock detection is implemented for this class. We detect deadlock on
    every wait. Upon deadlock, a DeadlockException is raised with a set of
    waiters in the same strongly connected components with self.

    """
    wait_graph = {}
    class DeadlockException(Exception):
        def __init__(self, waiters, message=None):
            Exception.__init__(self, message)
            self.waiters = waiters

    def __init__(self):
        Thread.__init__(self)

    def acquired(self, res):
        if res not in BThread.wait_graph:
            BThread.wait_graph[res] = set([])
        BThread.wait_graph[res].add(self)
        try:
            self.checkDeadlock()
        except BThread.DeadlockException as e:
            self.released(res)
            raise e

    def released(self, res):
        try:
            BThread.wait_graph[res].remove(self)
            if len(BThread.wait_graph[res]) == 0:
                del BThread.wait_graph[res]
        except:
            pass

    def tryWait(self, res):
        if self not in BThread.wait_graph:
            BThread.wait_graph[self] = set([])
        BThread.wait_graph[self].add(res)
        try:
            self.checkDeadlock()
        except BThread.DeadlockException as e:
            self.endWait(res)
            raise e

    def endWait(self, res):
        BThread.wait_graph[self].remove(res)
        if len(BThread.wait_graph[self]) == 0:
            del  BThread.wait_graph[self]

    @property
    def height(self):
        """The height of the waiting graph."""
        if self not in BThread.wait_graph:
            return 1
        heights = []
        for res in BThread.wait_graph[self]:
            if res not in BThread.wait_graph:
                heights.append(0)
                continue
            for thread in BThread.wait_graph[res]:
                if thread == self:
                    continue
                heights.append(thread.height)
        return max(heights) + 1

    @property
    def width(self):
        """Number of all threads that self waits for."""
        if self not in BThread.wait_graph:
            return 0
        queue = [self]
        threads = set([])
        while len(queue) != 0:
            thread = queue.pop()
            if thread in threads:
                continue
            threads.add(thread)
            if thread not in BThread.wait_graph:
                continue
            for res in BThread.wait_graph[thread]:
                if res not in BThread.wait_graph:
                    continue
                for bthread in BThread.wait_graph[res]:
                    queue.append(bthread)
        #exclude self
        return len(threads) - 1

    @property
    def dwidth(self):
        """Number of threads that self directly waits for."""
        if self not in BThread.wait_graph:
            return 0
        threads = set([])
        for res in BThread.wait_graph[self]:
            for bthread in BThread.wait_graph[res]:
                threads.add(bthread)
        return len(threads)

    def checkDeadlock(self):
        sccs = BThread.TarjanAlgo.findscc(BThread.wait_graph, self)
        #if only two entities in the scc, then this means A->B->A.
        #this is possible in cases such as lock promotion from shared to
        #exclusive, which is actually not a deadlock.
        #scc with len less than 2 are not deadlocks.
        sccs[:] = [ scc for scc in sccs if len(scc) > 2]
        if len(sccs) == 0:
            return
        assert len(sccs) == 1
        scc = iter(sccs).next()
        waiters = set([])
        for v in scc:
            if isinstance(v, BThread) and v is not self:
                waiters.add(v)
        #construct a string that describes the current situation
        sccstrings = []
        for v in scc:
            strings = ['%s ->' %str(v)]
            for w in BThread.wait_graph[v]:
                if w in scc:
                    strings.append('%s, '%str(w))
            sccstrings.append(''.join(strings))
        sccstring = '  |  '.join(['(%s)' %s for s in sccstrings])
        raise BThread.DeadlockException(waiters, sccstring)

    class Vertex(object):
        def __init__(self, obj):
            self.obj = obj
            self.index = -1
            self.lowlink = -1

    class TarjanAlgo(object):
        """
        http://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
        """
        def __init__(self, graph):
            self.index = 0
            self.stack = []
            self.graph = graph
            self.vertices = {}
            for key in self.graph.keys():
                self.vertices[key] = BThread.Vertex(key)

        def strongconnect(self, vertex, sccs):
            vertex.index = self.index
            vertex.lowlink = self.index
            self.index += 1
            self.stack.append(vertex)
            adj = self.graph.get(vertex.obj, [])
            for w in adj:
                if w not in self.vertices:
                    self.vertices[w] = BThread.Vertex(w)
                w = self.vertices[w]
                if w.index == -1:
                    self.strongconnect(w, sccs)
                    vertex.lowlink = min(vertex.lowlink, w.lowlink)
                elif w in self.stack:
                    vertex.lowlink = min(vertex.lowlink, w.index)
            if vertex.index == vertex.lowlink:
                scc = []
                curr = None
                while curr != vertex:
                    curr = self.stack.pop()
                    scc.append(curr.obj)
                sccs.append(scc)

        def _findscc(self, root):
            """If root is provided, we only check which scc the root is in, else, a
            list of scc is constructed."""
            self.stack = []
            sccs = []
            if root is not None:
                if root not in self.vertices:
                    return []
                self.strongconnect(self.vertices[root], sccs)
            else:
                for v in self.vertices.values():
                    if v.index == -1:
                        self.strongconnect(v, sccs)
            return sccs

        @classmethod
        def findscc(cls, graph, root=None):
            algo = BThread.TarjanAlgo(graph)
            return algo._findscc(root)

#####  Test  #####

class WaitForAlarm(Process):
    def onetime(self, delay):
        print 'set onetime alarm'
        alarmevent = Alarm.setOnetime(delay)
        print 'work on something else until one time alarm'
        yield waitevent, self, alarmevent
        print 'alarmed at %s' %now()

    def periodic(self, interval, until=infinite, drift=('fixed', 0)):
        print 'set periodic alarm'
        alarmevent = Alarm.setPeriodic(interval, until=until, drift=drift)
        count = 20
        while count > 0:
            print 'work on something else until alarm'
            yield waitevent, self, alarmevent
            print 'alarmed at %s' %now()
            count -= 1

def testAlarm():
    wfa1 = WaitForAlarm()
    activate(wfa1, wfa1.onetime(10))
    wfa2 = WaitForAlarm()
    activate(wfa2, wfa2.periodic(5, until=50), at=15)
    wfa3 = WaitForAlarm()
    activate(wfa3, wfa3.periodic(5, drift=('norm', 0, {'sigma':0.1})), at=60)

class Child(Thread):
    def run(self):
        print 'a thread start at %s' %now()
        yield hold, self, 10
        print 'a thread end at %s' %now()

class Main(Thread):
    def run(self):
        print 'main thread at %s' %now()
        yield hold, self, 10
        print 'fork new thread at %s' %now()
        child = Child()
        child.start()
        yield hold, self, 3
        print 'wait for child thread to finish at %s' %now()
        yield waitevent, self, child.finish
        print 'child thread finish at %s' %now()

def testThread():
    Main().start()

def testTanjanAlgo():
    """The graph example is from tarjan's paper:
        doi:10.1137/0201010
    """
    vertices = {
        '1' : IDable('1'),
        '2' : IDable('2'),
        '3' : IDable('3'),
        '4' : IDable('4'),
        '5' : IDable('5'),
        '6' : IDable('6'),
        '7' : IDable('7'),
        '8' : IDable('8'),
    }
    graph = {
        vertices['1'] : [vertices['2']],
        vertices['2'] : [vertices['3'], vertices['8']],
        vertices['3'] : [vertices['4'], vertices['7']],
        vertices['4'] : [vertices['5']],
        vertices['5'] : [vertices['6'], vertices['3']],
        vertices['7'] : [vertices['4'], vertices['6']],
        vertices['8'] : [vertices['1'], vertices['7']],
    }
    for vertex in vertices.values():
        sccs = BThread.TarjanAlgo.findscc(graph, vertex)
        printStr = ('%s: %s'
                    %(vertex.ID, ', '.join(
                        ['[%s]'%(', '.join(['%s'%v.ID for v in scc]))
                         for scc in sccs])))
        print printStr
    sccs = BThread.TarjanAlgo.findscc(graph)
    printStr = ('all: %s'
                %(', '.join(
                    ['[%s]'%(', '.join(['%s'%v.ID for v in scc]))
                     for scc in sccs])))
    print printStr

def main():
    initialize()
    testAlarm()
    testThread()
    simulate(until=1000)
    testTanjanAlgo()

if __name__ == '__main__':
    main()

