import sys

import numpy as np
import matplotlib
matplotlib.use('pdf')
import matplotlib.pylab as plt
import scipy as sp

from randgen import RVGen

class DDist(object):
    def __init__(self, lb, pmfy, cmfy=None, h=0.1):
        """A discretized distribution.
        
        @args:
            lb      --  the least value with non-zero probability
            pmfy    --  values of pmf with the variable taken values 
                        lb + i * self.h

        """
        #[lb, ub]
        self.h = h
        self._lb = int(lb / h) * h
        self._ub = self._lb + (len(pmfy) - 1) * self.h
        self._pmfy = pmfy
        if cmfy is None:
            self._cmfy = [pmfy[0]]
            for i in range(1, len(pmfy)):
                self._cmfy.append(self._cmfy[i - 1] + pmfy[i])
        else:
            self._cmfy = cmfy
        self._mean = None
        self._std = None

    def pmf(self, x):
        i = int((x - self._lb) / self.h)
        if i < 0 or i >= len(self._cmfy):
            return 0
        else:
            return self._pmfy[i]

    def cmf(self, x):
        i = int((x - self._lb) / self.h)
        if i < 0 or i >= len(self._cmfy):
            return 0
        else:
            return self._cmfy[i]

    def pmfi(self, i):
        return self._pmfy[i]

    def iterpmf(self):
        for p in self._pmfy:
            yield p

    def cmfi(self, i):
        return self._cmfy[i]

    def itercmf(self):
        for p in self._cmfy:
            yield p

    def index(self, x):
        if x < self._lb or x > self._ub:
            return None
        return int((x - self._lb) / self.h)

    @property
    def lb(self):
        return self._lb

    @property
    def ub(self):
        return self._ub

    @property
    def length(self):
        return len(self._pmfy)

    @property
    def mean(self):
        if self._mean is None:
            self._mean = 0
            for i, p in enumerate(self._pmfy):
                self._mean += (self.lb + i * self.h) * p
        return self._mean

    @property
    def std(self):
        if self._std is None:
            mean = self.mean
            self._std = 0
            for i, p in enumerate(self._pmfy):
                value = self.lb + i * self.h
                self._std += (value - mean)**2 * p
            self._std = np.sqrt(self._std)
        return self._std

    def __add__(self, ddist):
        if not isinstance(ddist, DDist):
            raise TypeError('%s is not of DDist type'%ddist)
        if self.h != ddist.h:
            raise ValueError('DDists must have the same sample rate to add: '
                             'first:%s, second:%s'%(self.h, ddist.h))
        li1 = int(self.lb / self.h)
        li2 = int(ddist.lb / self.h)
        ui1 = li1 + self.length - 1
        ui2 = li2 + ddist.length - 1
        lb = self.lb + ddist.lb
        ub = self.ub + ddist.ub
        li = li1 + li2
        n = int((ub - lb) / self.h) + 1
        pmfy = [0.0] * n
        #print ('\nself.lb:%s, ddist.lb:%s, li1:%s, li2:%s, ui1:%s, ui2:%s, lb:%s, ub:%s, li:%s, n:%s\n'
        #       %(self.lb, ddist.lb, li1, li2, ui1, ui2, lb, ub, li, n))
        for sidx in range(n):
            s = sidx + li    #absolute index of summation
            start = max(li1, s - ui2)
            end = min(ui1 + 1, s - li2 + 1)
            for i in range(start, end):
                idx1 = i - li1       #relative index of first ddist
                idx2 = s - i - li2   #relative index of second ddist
                pmfy[sidx] += self.pmfi(idx1) * ddist.pmfi(idx2)
        return DDist(lb, pmfy, h=self.h)

    def getPmfxy(self):
        x = []
        for i in range(self.length):
            x.append(self.lb + i * self.h)
        return x, self._pmfy

    def getCmfxy(self):
        x = []
        for i in range(self.length):
            x.append(self.lb + i * self.h)
        return x, self._cmfy

    def plot(self, outfn):
        fig = plt.figure()
        axes = fig.add_subplot(211)
        x, y = self.getPmfxy()
        axes.plot(x, y)
        axes = fig.add_subplot(212)
        x, y = self.getCmfxy()
        axes.plot(x, y)
        fig.savefig('%s'%outfn)

    @classmethod
    def sample(cls, config, h=0.1, num=100000):
        x = RVGen.run(config, num)
        return cls.create(x, h)

    @classmethod
    def create(cls, samples, h=0.1):
        li = int(min(samples) / h)
        ui = int(max(samples) / h)
        pmf = [0.0] * (ui - li + 1)
        for x in samples:
            i = int(x / h) - li
            pmf[i] += 1
        #normalize
        for i in range(len(pmf)):
            pmf[i] /= len(samples)
        lb = li * h
        return DDist(lb, pmf, h=h)

def maxn(n, ddist):
    cmf = []
    for p in ddist.itercmf():
        cmf.append(p**n)
    pmf = [cmf[0]]
    for i in range(1, len(cmf)):
        pmf.append(cmf[i] - cmf[i - 1])
    return DDist(ddist.lb, pmf, cmf, ddist.h)

def cond(p, ddist1, ddist2):
    if ddist1.h != ddist2.h:
        raise ValueError('DDists must have the same sample rate to use cond')
    h = ddist1.h
    lb = min(ddist1.lb, ddist2.lb)
    ub = max(ddist1.ub, ddist2.ub)
    pmf = []
    for i in range(int((ub - lb) / h) + 1):
        x = lb + i * h
        p1 = ddist1.pmf(x)
        p2 = ddist2.pmf(x)
        pmf.append(p * p1 + (1 - p) * p2)
    return DDist(lb, pmf, h=h)

def quorum(n, f, ddist):
    """The quorum latency distribution.

    if leader and learner are on the same node:
        we only need a n-f-1 quorum out of n - 1 nodes
    else :
        we need a n-f quorum:where one of them might be half round trip
        where acceptor and the leader is on the same node
    """
    sTrip = ddist
    rTrip = ddist + ddist
    return cond(1.0 / n,
                _quorumC(n - 1, n - f - 1, rTrip),
                _quorumR(n, n - f, sTrip, rTrip))

def _quorumC(n, m, rTrip):
    cmf = [0.0] * rTrip.length
    for i in range(len(cmf)):
        p = rTrip.cmfi(i)
        for j in range(m, n + 1):
            cmf[i] += sp.misc.comb(n, j) * p**j * (1 - p)**(n - j)
    pmf = [cmf[0]]
    for i in range(1, len(cmf)):
        pmf.append(cmf[i] - cmf[i - 1])
    return DDist(rTrip.lb, pmf, cmf, rTrip.h)

def _quorumR(n, m, sTrip, rTrip):
    if m <=2:
        raise ValueError(
            'We only consider cases when quorum size larger than 2')
    cmf = [0.0] * rTrip.length
    sli = int(sTrip.lb / sTrip.h)
    sui = int(sTrip.ub / sTrip.h)
    rli = int(rTrip.lb / rTrip.h)
    rui = int(rTrip.ub / rTrip.h)
    #There are two special one where leader == acceptor or acceptor ==
    #learner, these two are half round trip
    for i in range(0, sui - rli + 1):
        s = sTrip.cmfi(i + rli - sli)
        r = rTrip.cmfi(i)
        for j in range(m, n + 1):
            #first case, both of half trip is less than t
            cmf[i] += s**2 * sp.misc.comb(n - 2, j - 2) * \
                    r**(j - 2) * (1 - r)**(n - j)
            #second case, only one of them is less than t
            cmf[i] += 2 * (s * (1 - s) * sp.misc.comb(n - 2, j - 1) * \
                           r**(j - 1) * (1 - r)**(n - 2 - j + 1))
            #third case, neither of them is less than t
            cmf[i] += (1 - s)**2 * sp.misc.comb(n - 2, j) * \
                    r**j * (1 - r)**(n - 2 - j)
    for i in range(max(0, sui - rli + 1), len(cmf)):
        r = rTrip.cmfi(i)
        for j in range(m, n + 1):
            cmf[i] += sp.misc.comb(n - 2, j - 2) * \
                    r**(j - 2) * (1 - r)**(n - j)
    pmf = [cmf[0]]
    for i in range(1, len(cmf)):
        pmf.append(cmf[i] - cmf[i - 1])
    return DDist(rTrip.lb, pmf, cmf, rTrip.h)

def oodelay(ddist, config):
    arrproc = config['arrival.process']
    if arrproc == 'fixed':
        intvl = config['fixed.interval']
        step = int(intvl / ddist.h)
        cmf = []
        for i in range(ddist.length):
            curr = 1
            prev = 0
            for j in range(i, ddist.length, step):
                prev = curr
                curr *= ddist.cmfi(j)
                if curr < 1e-10:
                    break
                if abs(prev - curr) / curr < 1e-6:
                    break
            cmf.append(curr)
        pmf = [cmf[0]]
        for i in range(1, len(cmf)):
            pmf.append(cmf[i] - cmf[i - 1])
        return DDist(ddist.lb, pmf, cmf, ddist.h)
    elif arrproc == 'poisson':
        lambd = config['poisson.lambda']
        cmf = []
        for i in range(ddist.length):
            s = 0
            for j in range(i, ddist.length):
                s += (1 - ddist.cmfi(j)) * ddist.h
            p = ddist.cmfi(i) * np.exp(-lambd * s)
            cmf.append(p)
        pmf = [cmf[0]]
        for i in range(1, len(cmf)):
            pmf.append(cmf[i] - cmf[i - 1])
        return DDist(ddist.lb, pmf, cmf, ddist.h)
    else:
        raise ValueError('Arrival process %s not supported'%arrproc)

def getSLPLatencyDist(n, ddist, lambd):
    f = int(np.ceil(n / 2.0) - 1)
    rtrip = quorum(n, f, ddist)
    delay = oodelay(
        rtrip, {'arrival.process' : 'poisson', 'poisson.lambda' : lambd})
    return cond(1.0 / n, delay, ddist + delay), \
            (rtrip.mean, rtrip.std, delay.mean, delay.std)

def getFPLatencyDist(n, ddist, lambd):
    f = int(np.ceil(n / 3.0) - 1)
    rtrip = quorum(n, f, ddist)
    #calculate the probability of conflict
    #p_c = \int \lambda e^(-\lambda t) [1 - R(t)] dt
    #p_c = \int_0^{ub} \lambda e^(-\lambda t) dt - \int_{lb}^{ub} \lambda e^{-lambda t} R(t) dt
    pc = 1 - np.exp(-lambd * ddist.ub)
    for i in range(ddist.length):
        t = ddist.lb + i * ddist.h
        pc -= lambd * np.exp(-lambd * t) * ddist.cmfi(i) * ddist.h
    pc *= float(n - 1) / n
    cRtrip = cond(pc, rtrip + rtrip, rtrip)
    return oodelay(
        cRtrip, {'arrival.process' : 'poisson', 'poisson.lambda' : lambd}), \
            (rtrip.mean, rtrip.std, cRtrip.mean, cRtrip.std)
    
def getEBLatencyDist(n, ddist, sync, elen):
    f = int(np.ceil(n / 2.0) - 1)
    rtrip = quorum(n, f, ddist)
    syncRtrip = rtrip + sync
    elatency = maxn(n, syncRtrip)
    #wait time is a uniform distribution on elen
    lb = 0; ub = elen; length = int((ub - lb) / ddist.h) + 1
    pmf = [1.0 / length] * length
    ewait = DDist(lb, pmf, h=ddist.h)
    return ewait + oodelay(
        elatency, {'arrival.process' : 'fixed', 'fixed.interval' : elen}), \
            (rtrip.mean, rtrip.std, syncRtrip.mean, syncRtrip.std, 
             elatency.mean, elatency.std)

#####  TEST  ##### 

def testAdd():
    print '===== test add =====\n'
    ddist0 = DDist.sample({'rv.name':'twostep','p':0.4, 'inf':-5, 'sup':10}, h=1)
    print ddist0.getPmfxy()
    print ddist0.getCmfxy()
    ddist1 = ddist0 + ddist0
    print ddist1.getPmfxy()
    print ddist1.getCmfxy()
    ddist1.plot('/tmp/test_add_twostep.pdf')
    mu = 10
    sigma = 3 
    x = []
    y = []
    for i in range(100000):
        x1 = np.random.normal(mu, sigma)
        x2 = np.random.normal(mu, sigma)
        x.append(x1)
        x.append(x2)
        y.append(x1 + x2)
    ddist2 = DDist.create(y, h=0.5)
    ddist3 = DDist.create(x, h=0.5)
    ddist4 = ddist3 + ddist3
    print ddist2.mean, ddist2.std, ddist2.lb, ddist2.ub
    print ddist3.mean, ddist3.std
    print ddist4.mean, ddist4.std, ddist4.lb, ddist4.ub
    ddist2.plot('/tmp/test_add1.pdf')
    ddist4.plot('/tmp/test_add2.pdf')
    print '===== end =====\n'

def testMaxn():
    print '===== test maxn =====\n'
    mu = 10
    sigma = 3 
    n = 5
    x = []
    y = []
    for i in range(100000):
        m = -sys.maxint
        for j in range(n):
            r = np.random.normal(mu, sigma)
            x.append(r)
            if r > m:
                m = r
        y.append(m)
    ddist0 = DDist.create(y)
    ddist1 = DDist.create(x)
    ddist2 = maxn(n, ddist1)
    print ddist0.mean, ddist0.std
    print ddist1.mean, ddist1.std
    print ddist2.mean, ddist2.std
    ddist0.plot('/tmp/test_maxn1.pdf')
    ddist2.plot('/tmp/test_maxn2.pdf')
    print '===== end =====\n'

def testCond():
    print '===== test cond =====\n'
    p = 0.8
    mu1 = 10
    sigma1 = 3 
    mu2 = 20
    sigma2 = 1 
    x1 = []
    x2 = []
    y = []
    for i in range(100000):
        r = np.random.random()
        if r < p:
            s = np.random.normal(mu1, sigma1)
            x1.append(s)
            y.append(s)
        else:
            s = np.random.normal(mu2, sigma2)
            x2.append(s)
            y.append(s)
    ddist0 = DDist.create(y)
    ddist1 = DDist.create(x1)
    ddist2 = DDist.create(x2)
    ddist3 = cond(p, ddist1, ddist2)
    print ddist0.mean, ddist0.std
    print ddist1.mean, ddist1.std
    print ddist2.mean, ddist2.std
    print ddist3.mean, ddist3.std
    ddist0.plot('/tmp/test_cond1.pdf')
    ddist3.plot('/tmp/test_cond2.pdf')
    print '===== end =====\n'

def testQuorum():
    print '===== test quorum =====\n'
    lambd = 50
    n = 5
    f = 2
    x = []
    yc = []
    yr = []
    y = []
    leader = 0
    def gen():
        r = np.random.random()
        if r < 0.5:
            return 5
        else:
            return 10
    for i in range(100000):
        latencies = []
        learner = np.random.randint(n)
        for acceptor in range(n):
            if leader == acceptor:
                first = 0
            else:
                first = np.random.exponential(lambd)
                #first = gen()
                x.append(first)
            if acceptor == learner:
                second = 0
            else:
                second = np.random.exponential(lambd)
                #second = gen()
                x.append(second)
            latencies.append(first + second)
        l = sorted(latencies)
        y.append(l[n - f - 1])
        if leader == learner:
            assert l[0] == 0
            yc.append(l[n - f - 1])
        else:
            yr.append(l[n - f - 1])
    ddist0 = DDist.create(x, h=0.5)
    ddist1 = DDist.create(y, h=0.5)
    ddistc1 = DDist.create(yc, h=0.5)
    ddistr1 = DDist.create(yr, h=0.5)
    ddist2 = quorum(n, f, ddist0)
    rtrip = ddist0 + ddist0
    print ddist0.mean, ddist0.std
    print rtrip.mean, rtrip.std, rtrip.lb, rtrip.ub
    ddistc2 = _quorumC(n - 1, n - f - 1, rtrip)
    ddistr2 = _quorumR(n, n - f, ddist0, rtrip)
    print 'latency', ddist0.mean, ddist0.std
    print 'quorum sim', ddist1.mean, ddist1.std
    print 'quorum c sim', ddistc1.mean, ddistc1.std
    print 'quorum r sim', ddistr1.mean, ddistr1.std
    print 'quorum ana', ddist2.mean, ddist2.std
    print 'quorum c ana', ddistc2.mean, ddistc2.std
    print 'quorum r ana', ddistr2.mean, ddistr2.std
    ddist1.plot('/tmp/test_quorum1.pdf')
    ddistc1.plot('/tmp/test_quorumc1.pdf')
    ddistr1.plot('/tmp/test_quorumr1.pdf')
    ddist2.plot('/tmp/test_quorum2.pdf')
    ddistc2.plot('/tmp/test_quorumc2.pdf')
    ddistr2.plot('/tmp/test_quorumr2.pdf')
    print '===== end =====\n'

def testoodelay():
    print '===== test oodelay =====\n'
    def genexpointvl():
        return np.random.exponential(50)

    def genfixedintvl():
        return 50

    def latency():
        return np.random.exponential(100)

    x, y = runoodelay(genfixedintvl, latency)
    ddist0 = DDist.create(x, h=0.5)
    ddist1 = DDist.create(y, h=0.5)
    ddist2 = oodelay(ddist0, {'arrival.process' : 'fixed', 'fixed.interval' : 50})
    print ddist1.mean, ddist1.std
    print ddist2.mean, ddist2.std
    ddist1.plot('/tmp/test_ood_fixed1.pdf')
    ddist2.plot('/tmp/test_ood_fixed2.pdf')
    x, y = runoodelay(genexpointvl, latency)
    ddist0 = DDist.create(x, h=0.5)
    ddist1 = DDist.create(y, h=0.5)
    ddist2 = oodelay(ddist0, {'arrival.process' : 'poisson', 'poisson.lambda' : 1.0 / 50})
    print ddist1.mean, ddist1.std
    print ddist2.mean, ddist2.std
    ddist1.plot('/tmp/test_ood_expo1.pdf')
    ddist2.plot('/tmp/test_ood_expo2.pdf')
    print '===== end =====\n'


def runoodelay(arrintvl, latency):
    x = []
    y = []
    laststart = 0
    lastend = 0
    for i in range(100000):
        currtime = laststart + arrintvl()
        laststart = currtime
        l = latency()
        x.append(l)
        endtime = currtime + l
        lastend = lastend if endtime < lastend else endtime
        y.append(lastend - currtime)
    return x, y

def test():
    testAdd()
    #testMaxn()
    #testCond()
    #testQuorum()
    #testoodelay()

def main():
    test()

if __name__ == '__main__':
    main()

