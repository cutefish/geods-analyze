import sys
from math import floor, ceil

import numpy as np
from scipy.misc import comb

from ddist import DDist

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
    #return cond(1.0 / n,
    #            _quorumC(n - 1, n - f - 1, rTrip),
    #            _quorumR(n, n - f, sTrip, rTrip))
    return _quorumC(n - 1, n - f - 1, rTrip)

def _quorumC(n, m, rTrip):
    #front distribution
    cmf = [0.0] * len(rTrip.cmfy)
    for i in range(len(cmf)):
        p = rTrip.cmfy[i]
        for j in range(m, n + 1):
            cmf[i] += comb(n, j) * p**j * (1 - p)**(n - j)
    #tail disctribution
    tcmf = [0.0] * len(rTrip.tcmfy)
    for i in range(len(tcmf)):
        p = rTrip.tcmfy[i]
        for j in range(m, n + 1):
            tcmf[i] += comb(n, j) * p**j * (1 - p)**(n - j)
    #comput pmf
    pmf = [cmf[0]]
    for i in range(1, len(cmf)):
        pmf.append(cmf[i] - cmf[i - 1])
    if len(tcmf) == 0:
        tpmf = []
    else:
        tpmf = [tcmf[0] - cmf[-1]]
        for i in range(1, len(tcmf)):
            tpmf.append(tcmf[i] - tcmf[i - 1])
    return DDist(rTrip.lb, pmf, rTrip.h, tpmf, rTrip.th)

def _quorumR(n, m, sTrip, rTrip):
    if m <= 2:
        raise ValueError(
            'We only consider cases when quorum size larger than 2')
    scmf, stcmf = _paddingSTrip(sTrip, rTrip.lb, rTrip.tb, rTrip.ub)
    cmf = []
    tcmf = []
    assert len(scmf) == len(rTrip.cmfy) and len(stcmf) == len(rTrip.tcmfy), \
            'scmf.l = %s == %s = r.cmfy.l, tscmf.l = %s == %s = r.tcmfy.l' %(
                len(scmf), len(rTrip.cmfy), len(stcmf), len(rTrip.tcmfy))
    #scmf/cmf and stcmf/tcmf are aligned
    for i in range(len(scmf)):
        ps = scmf[i]; pr = rTrip.cmfy[i]
        p = 0.0
        for j in range(m, n + 1):
            for k in range(3):
                if n - 2 - j + k >= 0 and j - k >= 0:
                    p += comb(2, k) * ps**k * (1 - ps)**(2 - k) * \
                            comb(n - 2, j - k) * \
                            pr**(j - k) * (1 - pr)**(n - 2 - j + k)
        cmf.append(p)
    for i in range(len(stcmf)):
        ps = stcmf[i]; pr = rTrip.tcmfy[i]
        p = 0.0
        for j in range(m, n + 1):
            for k in range(3):
                if n - 2 - j + k >= 0 and j - k >= 0:
                    p += comb(2, k) * ps**k * (1 - ps)**(2 - k) * \
                            comb(n - 2, j - k) * \
                            pr**(j - k) * (1 - pr)**(n - 2 - j + k)
        tcmf.append(p)
    #compute pmf
    pmf = [cmf[0]]
    for i in range(1, len(cmf)):
        pmf.append(cmf[i] - cmf[i - 1])
    if len(tcmf) == 0:
        tpmf = []
    else:
        tpmf = [tcmf[0] - cmf[-1]]
        for i in range(1, len(tcmf)):
            tpmf.append(tcmf[i] - tcmf[i - 1])
    return DDist(rTrip.lb, pmf, rTrip.h, tpmf, rTrip.th)

def _paddingSTrip(sTrip, lb, tb, ub):
    assert sTrip.lb <= lb, 's.lb = %s <= %s = lb'%(sTrip.lb, lb)
    assert sTrip.tb <= tb, 's.tb = %s <= %s = tb'%(sTrip.tb, tb)
    assert sTrip.ub <= ub, 's.ub = %s <= %s = ub'%(sTrip.ub, ub)
    #compute the front dist
    cmf = []
    ##sTrip.cmfy part
    start = max(lb, sTrip.lb)
    end = min(tb, sTrip.tb)
    si = int((start - sTrip.lb + 0.5 * sTrip.h) / sTrip.h)
    ei = int((end - sTrip.lb + 0.5 * sTrip.h) / sTrip.h)
    cmf.extend(sTrip.cmfy[si:ei])
    #print len(cmf), start, end, si, ei
    ##sTrip.tcmfy part
    start = max(lb, sTrip.tb)
    end = min(tb, sTrip.ub)
    si = int((start - sTrip.tb + 0.5 * sTrip.th) / sTrip.th)
    ei = int((end - sTrip.tb + 0.5 * sTrip.th) / sTrip.th)
    tnh = int((sTrip.th + 0.5 * sTrip.h) / sTrip.h)
    for i in range(si, ei):
        for j in range(tnh):
            cmf.append(sTrip.tcmfy[i])
    #print len(cmf), start, end, si, ei
    ##left part
    start = max(lb, sTrip.ub)
    end = tb
    n = int((end - start + 0.5 * sTrip.h) / sTrip.h)
    for i in range(n):
        cmf.append(1.0)
    #print len(cmf), start, end, n
    #compute the tail dist
    tcmf = []
    ##sTrip.tcmfy part
    start = max(tb, sTrip.tb)
    end = min(ub, sTrip.ub)
    si = int((start - sTrip.tb + 0.5 * sTrip.th) / sTrip.th)
    ei = int((end - sTrip.tb + 0.5 * sTrip.th) / sTrip.th)
    tcmf.extend(sTrip.tcmfy[si:ei])
    #print len(tcmf), start, end, si, ei
    ##left part
    start = max(tb, sTrip.ub)
    end = ub
    n = int((end - start + 0.5 * sTrip.th) / sTrip.th)
    for i in range(n):
        tcmf.append(1.0)
    #print len(tcmf), start, end, n
    return cmf, tcmf

def oodelay(ddist, config):
    arrproc = config['arrival.process']
    if arrproc == 'fixed':
        intvl = config['fixed.interval']
        return _oodelayFixedInterval(ddist, intvl)
    elif arrproc == 'poisson':
        lambd = config['poisson.lambda']
        return _oodelayPoisson(ddist, lambd)
    else:
        raise ValueError('Arrival process %s not supported'%arrproc)

def _oodelayFixedInterval(ddist, intvl):
    cmf = []
    tcmf = []
    #front dist
    for i in range(len(ddist.cmfy)):
        curr = 1
        prev = 0
        #front part
        start = ddist.lb + i * ddist.h
        x = start
        while x < ddist.tb:
            j = int(floor((x - ddist.lb) / ddist.h))
            prev = curr
            curr *= ddist.cmfy[j]
            x += intvl
            if curr < 1e-10:
                x = ddist.ub
                break
            if abs(prev - curr) / curr < 1e-6:
                x = ddist.ub
                break
        #tail part
        while x < ddist.ub:
            j = int(floor((x - ddist.tb) / ddist.th))
            prev = curr
            curr *= ddist.tcmfy[j]
            x += intvl
            if curr < 1e-10:
                break
            if abs(prev - curr) / curr < 1e-6:
                break
        cmf.append(curr)
    #tail dist
    for i in range(len(ddist.tcmfy)):
        curr = 1
        prev = 0
        start = ddist.tb + i * ddist.th
        x = start
        while x < ddist.ub:
            j = int(floor((x - ddist.tb) / ddist.th))
            prev = curr
            curr *= ddist.tcmfy[j]
            x += intvl
            if curr < 1e-10:
                break
            if abs(prev - curr) / curr < 1e-6:
                break
        tcmf.append(curr)
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

def _oodelayPoisson(ddist, lambd):
    cmf = []
    tcmf = []
    #front dist
    for i in range(len(ddist.cmfy)):
        s = 0
        #front part
        for j in range(i, len(ddist.cmfy)):
            s += (1 - ddist.cmfy[j]) * ddist.h
        #tail part
        for j in range(len(ddist.tcmfy)):
            s += (1 - ddist.tcmfy[j]) * ddist.th
        p = ddist.cmfy[i] * np.exp(-lambd * s)
        cmf.append(p)
    #tail dist
    for i in range(len(ddist.tcmfy)):
        s = 0
        for j in range(i, len(ddist.tcmfy)):
            s += (1 - ddist.tcmfy[j]) * ddist.th
        p = ddist.tcmfy[i] * np.exp(-lambd * s)
        tcmf.append(p)
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

def getSLPLatencyDist(n, ddist, lambd):
    f = int(np.ceil(n / 2.0) - 1)
    rtrip = quorum(n, f, ddist)
    delay = oodelay(
        rtrip, {'arrival.process' : 'poisson', 'poisson.lambda' : lambd})
    return cond(1.0 / n, delay, ddist + delay), delay, rtrip

#def getFPLatencyDist(n, ddist, lambd):
#    f = int(np.ceil(n / 3.0) - 1)
#    rtrip = quorum(n, f, ddist)
#    #calculate the probability of conflict
#    #p_c = \int \lambda e^(-\lambda t) [1 - R(t)] dt
#    #p_c = \int_0^{ub} \lambda e^(-\lambda t) dt - \int_{lb}^{ub} \lambda e^{-lambda t} R(t) dt
#    pc = 1 - np.exp(-lambd * ddist.ub)
#    for i in range(ddist.length):
#        t = ddist.lb + i * ddist.h
#        pc -= lambd * np.exp(-lambd * t) * ddist.cmfi(i) * ddist.h
#    pc *= float(n - 1) / n
#    cRtrip = cond(pc, rtrip + rtrip, rtrip)
#    return oodelay(
#        cRtrip, {'arrival.process' : 'poisson', 'poisson.lambda' : lambd}), \
#            (rtrip.mean, rtrip.std, cRtrip.mean, cRtrip.std)

def getFPLatencyDist(n, ddist, lambd):
    f = int(np.ceil(n / 3.0) - 1)
    rtrip = quorum(n, f, ddist)
    T = rtrip.mean
    if 1.1 * lambd * T > 1:
        raise ValueError('1/lambda = %s < 1.1 * %s = 1.1T'%(1.0 / lambd, T))
    Q = []          #the probabiltiy of system has n proposers
    CQ0 = [1]       #coefficient of Q interms of Q0
    sumCQ0 = 1      #the summation of CQ0 and stop condition
    poisson = [np.exp(-lambd * T)]    #the poisson result
    factorial=[1]                     #factorial
    #compute CQ0[1]
    q1 = (1-np.exp(-lambd * T))/np.exp(-lambd * T)
    sumCQ0 += q1
    CQ0.append(q1)
    #compute Q[curr]
    #Q[k] = exp(lambd*T)[
    #       Q[k-1](1-lambd*T*exp(-lambd*T)-
    #       Q[0]Poisson[k-1]-
    #       sum_{i=1}^{k-2}Q[i]Poisson[k-i])]
    curr = 2
    while True:
        #compute factorial curr-1
        factorial.append(factorial[curr-2] * (curr - 1))
        #compute poisson curr-1
        poisson.append((lambd * T)**(curr-1)/factorial[curr-1]*np.exp(-lambd * T))
        #compute the summation
        s = 0
        for i in range(1, curr-2+1):
            s += CQ0[i] * poisson[curr - i]
        #compute CQ0[curr]
        q = np.exp(lambd * T) * (CQ0[curr-1]*(1-lambd * T * np.exp(-lambd * T)) - \
                                 CQ0[0] * poisson[curr-1] - s)
        CQ0.append(q)
        #stop condition
        sumCQ0 += q
        if q / sumCQ0 < 1e-5:
            break
        curr += 1
    #normalize CQ0
    CQ = []
    for q in CQ0:
        CQ.append(q / sumCQ0)
    #compute average number of proposers
    eN = 0
    for i, p in enumerate(CQ):
        eN += i * p
    #compute average serving time
    res = eN / lambd
    #debug
    #print 'factorial', factorial[0:10]
    #print 'poisson', poisson[0:10]
    return res, eN

def getEPLatencyDist(n, ddist, sync, elen):
    f = int(np.ceil(n / 2.0) - 1)
    rtrip = quorum(n, f, ddist)
    syncRtrip = rtrip + sync
    elatency = maxn(n, syncRtrip)
    #wait time is a uniform distribution on elen
    lb = 0; ub = elen; length = int((ub - lb) / ddist.h) + 1
    pmf = [1.0 / length] * length
    ewait = DDist(lb, pmf, h=ddist.h, th=ddist.th)
    delay = oodelay(elatency,
                    {'arrival.process' : 'fixed', 'fixed.interval' : elen})
    return ewait + delay, delay, rtrip

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
    print 'base', ddist0.mean, ddist0.std
    print 'sim', ddist1.mean, ddist1.std
    print 'ana', ddist2.mean, ddist2.std
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
    ddist0 = DDist.create(x, tail=('p', 0.1), tnh=25)
    ddist1 = DDist.create(y, tail=('p', 0.1), tnh=25)
    #ddist0 = DDist.create(x)
    #ddist1 = DDist.create(y)
    ddist2 = maxn(n, ddist0)
    print 'base', ddist0.mean, ddist0.std, 'length', len(ddist0.pmfy), len(ddist0.tpmfy)
    print 'sim', ddist1.mean, ddist1.std, 'length', len(ddist1.pmfy), len(ddist1.tpmfy)
    print 'ana', ddist2.mean, ddist2.std, 'length', len(ddist2.pmfy), len(ddist2.tpmfy)
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
    print 'base1', ddist0.mean, ddist0.std
    print 'base2', ddist1.mean, ddist1.std
    print 'sim', ddist2.mean, ddist2.std
    print 'ana', ddist3.mean, ddist3.std
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
    ddist0 = DDist.create(x1, tail=('p', 0.1), tnh=10)
    ddist1 = DDist.create(x2, tail=('p', 0.1), tnh=10)
    ddist2 = DDist.create(y)
    ddist3 = cond(p, ddist0, ddist1)
    print 'base1', ddist0.mean, ddist0.std
    print 'base2', ddist1.mean, ddist1.std
    print 'sim', ddist2.mean, ddist2.std
    print 'ana', ddist3.mean, ddist3.std
    ddist2.plot('/tmp/test_cond_pareto1.pdf')
    ddist3.plot('/tmp/test_cond_pareto2.pdf')
    print '===== end =====\n'

def testQuorum():
    print '===== test quorum ====='
    lambd = 50
    a = 1.8
    n = 5
    f = 2
    x = {'gen' : [], 'exp':[], 'pareto':[]}
    yc = {'gen' : [], 'exp':[], 'pareto':[]}
    yr = {'gen' : [], 'exp':[], 'pareto':[]}
    y = {'gen' : [], 'exp':[], 'pareto':[]}
    dists = ['gen', 'exp', 'pareto']
    leader = 0
    def gen():
        r = np.random.random()
        if r < 0.2:
            return 1
        elif r < 0.4:
            return 2
        elif r < 0.6:
            return 3
        elif r < 0.8:
            return 4
        else:
            return 8

    for i in range(100000):
        first = {}
        second = {}
        latencies = {'gen':[], 'exp':[], 'pareto':[]}
        learner = np.random.randint(n)
        for acceptor in range(n):
            if leader == acceptor:
                for k in dists:
                    first[k] = 0
            else:
                first['gen'] = gen()
                first['exp'] = np.random.exponential(lambd)
                first['pareto'] = np.random.pareto(a)
                for k in dists:
                    x[k].append(first[k])
            if acceptor == learner:
                for k in dists:
                    second[k] = 0
            else:
                second['gen'] = gen()
                second['exp'] = np.random.exponential(lambd)
                second['pareto'] = np.random.pareto(a)
                for k in dists:
                    x[k].append(second[k])
            for k in dists:
                latencies[k].append(first[k] + second[k])
        for k in dists:
            l = sorted(latencies[k])
            y[k].append(l[n - f - 1])
            if leader == learner:
                assert l[0] == 0
                yc[k].append(l[n - f - 1])
            else:
                yr[k].append(l[n - f - 1])
    for k in dists:
        print '\n>>> %s\n'%k
        if k == 'gen':
            ddist0 = DDist.create(x[k], h=1, tail=('b', 4), tnh=4)
        elif k == 'exp':
            ddist0 = DDist.create(x[k], h=0.5)
        else:
            ddist0 = DDist.create(x[k], h=0.5, tail=('p', 0.1), tnh=20)
            print 'created ddist0'
        if k == 'gen':
            ddist1 = DDist.create(y[k], h=1, tail=('b', 8), tnh=4)
            ddistc1 = DDist.create(yc[k], h=1, tail=('b', 8), tnh=4)
            ddistr1 = DDist.create(yr[k], h=1, tail=('b', 8), tnh=4)
            print 'base pmf', ddist1.getPmfxy()
            print 'base c pmf', ddistc1.getPmfxy()
            print 'base r pmf', ddistr1.getPmfxy()
        else:
            ddist1 = DDist.create(y[k], h=0.05)
            ddistc1 = DDist.create(yc[k], h=0.05)
            ddistr1 = DDist.create(yr[k], h=0.05)
        ddist2 = quorum(n, f, ddist0)
        print 'computed quorum'
        rtrip = ddist0 + ddist0
        ddistc2 = _quorumC(n - 1, n - f - 1, rtrip)
        ddistr2 = _quorumR(n, n - f, ddist0, rtrip)
        if k == 'gen':
            print 'q pmf', ddist2.getPmfxy()
            print 'q c pmf', ddistc2.getPmfxy()
            print 'q r pmf', ddistr2.getPmfxy()
        print 'latency', ddist0.mean, ddist0.std
        print 'quorum sim', ddist1.mean, ddist1.std
        print 'quorum c sim', ddistc1.mean, ddistc1.std
        print 'quorum r sim', ddistr1.mean, ddistr1.std
        print 'quorum ana', ddist2.mean, ddist2.std
        print 'quorum c ana', ddistc2.mean, ddistc2.std
        print 'quorum r ana', ddistr2.mean, ddistr2.std
        ddist1.plot('/tmp/test_quorum_%s1.pdf'%k)
        ddistc1.plot('/tmp/test_quorumc_%s1.pdf'%k)
        ddistr1.plot('/tmp/test_quorumr_%s1.pdf'%k)
        ddist2.plot('/tmp/test_quorum_%s2.pdf'%k)
        ddistc2.plot('/tmp/test_quorumc_%s2.pdf'%k)
        ddistr2.plot('/tmp/test_quorumr_%s2.pdf'%k)
    print '===== end =====\n'

def testoodelay():
    print '===== test oodelay =====\n'
    intvl = 30
    def genexpointvl():
        return np.random.exponential(intvl)

    def genfixedintvl():
        return intvl

    lambd = 100
    a = 1.8
    m = 100
    lb = 50
    tb = 150
    ub = 1000
    def customlatency():
        r = np.random.random()
        if r < 0.2:
            return 30
        elif r < 0.4:
            return 60
        elif r < 0.6:
            return 90
        elif r < 0.8:
            return 120
        else:
            return 180

    def expolatency():
        return np.random.exponential(lambd)

    def paretolatency():
        return np.random.pareto(a) + m

    def uniformlatency():
        r = np.random.random()
        if r < 0.8:
            rr = np.random.random()
            return lb + rr * (tb - lb)
        else:
            rr = np.random.random()
            return tb + rr * (ub - tb)

    intvlkeys = {'fixed' : genfixedintvl,
                 'poisson' : genexpointvl}
    intvlopts = {'fixed' : 'fixed.interval',
                 'poisson' : 'poisson.lambda'}
    latencykeys = {'custom' : customlatency,
                   'expo' : expolatency,
                   'pareto' : paretolatency,
                   'uniform' : uniformlatency,
                  }
    for ik in intvlkeys:
        for lk in latencykeys:
            print '\n>>> %s, %s\n'%(ik, lk)
            x, y = runoodelay(intvlkeys[ik], latencykeys[lk])
            if lk == 'custom':
                ddist0 = DDist.create(x, h=30, tail=('b', 120), tnh=2)
            elif lk == 'expo':
                ddist0 = DDist.create(x, h=0.5)
            elif lk == 'uniform':
                ddist0 = DDist.create(x, h=0.5, tail=('b', 150), tnh=10)
            else:
                ddist0 = DDist.create(x, h=0.5, tail=('p', 0.1), tnh=10)
            if lk == 'custom':
                ddist1 = DDist.create(y, h=30)
            else:
                ddist1 = DDist.create(y, h=0.5)
            if ik == 'fixed':
                ddist2 = oodelay(ddist0, {'arrival.process' : 'fixed',
                                          'fixed.interval' : intvl})
            else:
                ddist2 = oodelay(ddist0, {'arrival.process' : 'poisson',
                                          'poisson.lambda' : 1.0 / intvl})
            print 'sim', ddist1.mean, ddist1.std
            print 'ana', ddist2.mean, ddist2.std
            if lk == 'custom':
                print 'sim pmf', ddist1.getPmfxy()
                print 'ana pmf', ddist2.getPmfxy()
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
    testQuorum()
    testoodelay()

def main():
    test()

if __name__ == '__main__':
    main()

