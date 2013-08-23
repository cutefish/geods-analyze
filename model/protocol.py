import sys

import numpy as np
import matplotlib
matplotlib.use('pdf')
import matplotlib.pylab as plt
import scipy as sp

from randgen import RVGen

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

