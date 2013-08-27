import sys
from math import floor, ceil

import numpy as np
import scipy as sp

from ddist import DDist
from randgen import RVGen

def maxn(n, ddist):
    cmf = []
    for p in ddist.cmfy:
        cmf.append(p**n)
    tcmf = []
    for p in ddist.tcmfy:
        tcmf.append(p**n)
    pmf = [cmf[0]]
    for i in range(1, len(cmf)):
        pmf.append(cmf[i] - cmf[i - 1])
    if len(tcmf) == 0:
        return DDist(ddist.lb, pmf, ddist.h, [], ddist.th)
    else:
        tpmf = [tcmf[0] - cmf[-1]]
        for i in range(1, len(tcmf)):
            tpmf.append(tcmf[i] - tcmf[i - 1])
        return DDist(ddist.lb, pmf, ddist.h, tpmf, ddist.th)

def cond(p, ddist1, ddist2):
    if ddist1.h != ddist2.h or ddist1.th != ddist2.th:
        raise ValueError('DDists must have the same sample rate to use cond')
    h = ddist1.h
    th = ddist1.th
    lb = min(ddist1.lb, ddist2.lb)
    tb = max(ddist1.tb, ddist2.tb)
    ub = max(ddist1.ub, ddist2.ub)
    pmf = []
    tpmf = []
    pmf1 = _paddingPmf(ddist1, lb, tb)
    pmf2 = _paddingPmf(ddist2, lb, tb)
    assert len(pmf1) == len(pmf2), \
            'len(pmf1) = %s == %s = len(pmf2)'%(len(pmf1), len(pmf2))
    tpmf1 = _paddingTpmf(ddist1, tb, ub)
    tpmf2 = _paddingTpmf(ddist2, tb, ub)
    assert len(tpmf1) == len(tpmf2), \
            'len(tpmf1) = %s == %s = len(tpmf2)'%(len(tpmf1), len(tpmf2))
    for i in range(len(pmf1)):
        pmf.append(p * pmf1[i] + (1 - p) * pmf2[i])
    for i in range(len(tpmf1)):
        tpmf.append(p * tpmf1[i] + (1 - p) * tpmf2[i])
    return DDist(lb, pmf, h, tpmf, th)

def _paddingPmf(ddist, lb, tb):
    assert lb <= ddist.lb and tb >= ddist.tb, \
            ('lb = %s <= %s = ddist.lb tb = %s >= %s = ddist.tb'
             %(lb, ddist.lb, tb, ddist.tb))
    #[lb, ddist.lb)
    n = int((ddist.lb - lb + 0.5 * ddist.h) / ddist.h)
    pmf = [0.0] * n
    #[ddist.lb, ddist.tb)
    pmf.extend(ddist.pmfy)
    #[ddist.tb, tb)
    tnh = int((ddist.th + 0.5 * ddist.h) / ddist.h)
    end = min(len(ddist.tpmfy), 
              int((tb - ddist.tb + 0.5 * ddist.th) / ddist.th))
    for i in range(end):
        pmf.append(ddist.tpmfy[i])
        pmf.extend([0.0] * (tnh - 1))
    n = max(0, int(floor((tb - ddist.ub + 0.5 * ddist.h) / ddist.h)))
    pmf.extend([0.0] * n)
    return pmf

def _paddingTpmf(ddist, tb, ub):
    assert tb >= ddist.tb and ub >= ddist.ub, \
            ('tb = %s >= %s = ddist.tb ub = %s >= %s = ddist.ub'
             %(tb, ddist.tb, ub, ddist.ub))
    #ddist.ub <= tb
    if ddist.ub <= tb:
        n = int((ub - tb + 0.5 * ddist.th) / ddist.th)
        return [0.0] * n
    #ddist.ub > tb
    tpmf = []
    #[tb, ddist.ub)
    start = int((tb - ddist.tb + 0.5 * ddist.th) / ddist.th)
    tpmf.extend(ddist.tpmfy[start:])
    #[ddist.ub, ub)
    n = int((ub - ddist.ub + 0.5 * ddist.th) / ddist.th)
    tpmf.extend([0.0] * n)
    return tpmf

def quorum(n, f, ddist):
    """The quorum latency distribution.

    if leader and learner are on the same node:
        we only need a n-f-1 quorum out of n - 1 nodes
    else :
        we need a n-f quorum:where one of them might be half round trip
        where acceptor/leader or acceptor/learner are on the same node.
    """
    sTrip = ddist
    rTrip = ddist + ddist
    return cond(1.0 / n,
                _quorumC(n - 1, n - f - 1, rTrip),
                _quorumR(n, n - f, sTrip, rTrip))

def _quorumC(n, m, rTrip):
    #front distribution
    cmf = [0.0] * len(rTrip.cmfy)
    for i in range(len(cmf)):
        p = rTrip.cmfy[i]
        for j in range(m, n + 1):
            cmf[i] += sp.misc.comb(n, j) * p**j * (1 - p)**(n - j)
    pmf = [cmf[0]]
    for i in range(1, len(cmf)):
        pmf.append(cmf[i] - cmf[i - 1])
    #tail disctribution
    tcmf = [0.0] * len(rTrip.tcmfy)
    for i in range(len(tcmf)):
        p = rTrip.tcmfy[i]
        for j in range(m, n + 1):
            tcmf[i] += sp.misc.comb(n, j) * p**j * (1 - p)**(n - j)
    tpmf = [tcmf[0] - cmf[-1]]
    for i in range(1, len(cmf)):
        tpmf.append(tcmf[i] - tcmf[i - 1])
    return DDist(rTrip.lb, pmf, rTrip.h, tpmf, rTrip.th)

def _quorumR(n, m, sTrip, rTrip):
    if m <= 2:
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
    print '===== test maxn ====='
    print '\n>>>normal\n'
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
    ddist0 = DDist.create(x)
    ddist1 = DDist.create(y)
    ddist2 = maxn(n, ddist0)
    print ddist0.mean, ddist0.std
    print ddist1.mean, ddist1.std
    print ddist2.mean, ddist2.std
    ddist1.plot('/tmp/test_maxn_normal1.pdf')
    ddist2.plot('/tmp/test_maxn_normal2.pdf')
    print '\n>>>pareto\n'
    a = 1.3
    x0 = 0
    x = []
    y = []
    for i in range(100000):
        m = -sys.maxint
        for j in range(n):
            r = np.random.pareto(a) + x0
            x.append(r)
            if r > m:
                m = r
        y.append(m)
    ddist0 = DDist.create(x, tailprob=0.1, tnh=25)
    ddist1 = DDist.create(y, tailprob=0.1, tnh=25)
    #ddist0 = DDist.create(x)
    #ddist1 = DDist.create(y)
    ddist2 = maxn(n, ddist0)
    print ddist0.mean, ddist0.std, ddist0.lb, len(ddist0.pmfy), len(ddist0.tpmfy)
    print ddist1.mean, ddist1.std, ddist1.lb, len(ddist1.pmfy), len(ddist1.tpmfy)
    print ddist2.mean, ddist2.std, ddist2.lb, len(ddist2.pmfy), len(ddist2.tpmfy)
    ddist1.plot('/tmp/test_maxn_pareto1.pdf')
    ddist2.plot('/tmp/test_maxn_pareto2.pdf')
    print '===== end =====\n'

def testCond():
    print '===== test cond ====='
    print '\n>>> normal\n'
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
    ddist0 = DDist.create(x1)
    ddist1 = DDist.create(x2)
    ddist2 = DDist.create(y)
    ddist3 = cond(p, ddist0, ddist1)
    print ddist0.mean, ddist0.std
    print ddist1.mean, ddist1.std
    print ddist2.mean, ddist2.std
    print ddist3.mean, ddist3.std
    ddist2.plot('/tmp/test_cond_norm1.pdf')
    ddist3.plot('/tmp/test_cond_norm2.pdf')
    print '\n>>> pareto\n'
    p = 0.5
    a1 = 1.1
    a2 = 1.2
    x1 = []
    x2 = []
    y = []
    for i in range(100000):
        r = np.random.random()
        if r < p:
            s = np.random.pareto(a1)
            x1.append(s)
            y.append(s)
        else:
            s = np.random.pareto(a2) + 10
            x2.append(s)
            y.append(s)
    ddist0 = DDist.create(x1, tailprob=0.1, tnh=10)
    ddist1 = DDist.create(x2, tailprob=0.1, tnh=10)
    ddist2 = DDist.create(y)
    ddist3 = cond(p, ddist0, ddist1)
    print ddist0.mean, ddist0.std
    print ddist1.mean, ddist1.std
    print ddist2.mean, ddist2.std
    print ddist3.mean, ddist3.std
    ddist2.plot('/tmp/test_cond_pareto1.pdf')
    ddist3.plot('/tmp/test_cond_pareto2.pdf')
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
    testMaxn()
    testCond()
    #testQuorum()
    #testoodelay()

def main():
    test()

if __name__ == '__main__':
    main()

