from math import floor, ceil

import numpy as np
#if __name__ == '__main__':
#    import matplotlib
#    matplotlib.use('pdf')
import matplotlib.pylab as plt

from randgen import RVGen

class DDist(object):
    def __init__(self, lb, pmfy, h=0.1, tpmfy=[], th=0.5):
        """A discretized distribution.

        @args:
            lb      --  the least value with non-zero probability.
            pmfy    --  values of pmf with the variable taken values
                        lb + i * self.h.
            h       --  interval.
            tpmfy   --  tail pmf with the variable taken values
                        lb + (len(pmfy) - 1) * self.h + i * self.th.
            th      --  tail interval.

        """
        #bounds
        #[_lb, _tb) and [_tb, _ub)
        self.h = float(h)
        self.th = floor(th / h) * h
        self.lb = floor(lb / h) * h
        self.tb = self.lb + len(pmfy) * self.h
        self.ub = self.tb + len(tpmfy) * self.th
        #pmf and cmf
        self.pmfy = pmfy
        self.cmfy = None
        self.tpmfy = tpmfy
        self.tcmfy = None
        #mean and std
        self._mean = None
        self._std = None
        #calculate cmf
        self.calcCmf()

    def calcCmf(self):
        self.cmfy = [self.pmfy[0]]
        for i in range(1, len(self.pmfy)):
            self.cmfy.append(self.cmfy[i - 1] + self.pmfy[i])
        if len(self.tpmfy) != 0:
            self.tcmfy = [self.cmfy[-1] + self.tpmfy[0]]
            for i in range(1, len(self.tpmfy)):
                self.tcmfy.append(self.tcmfy[i - 1] + self.tpmfy[i])
        else:
            self.tcmfy = []

    def pmf(self, x):
        if x < self._lb:
            return 0
        elif x < self._tb:
            i = int(floor((x - self._lb) / self.h))
            return self.pmfy[i]
        elif x < self._ub:
            i = int(floor((x - self._tb) / self.th))
            return self.tpmfy[i]
        else:
            return 0

    def cmf(self, x):
        if x < self._lb:
            return 0
        elif x < self._tb:
            i = int(floor((x - self._lb) / self.h))
            return self.cmfy[i]
        elif x < self._ub:
            i = int(floor((x - self._tb) / self.th))
            return self.tcmfy[i]
        else:
            return 1

    @property
    def llen(self):
        return len(self.pmfy)

    @property
    def tlen(self):
        return len(self.tpmfy)

    @property
    def mean(self):
        if self._mean is None:
            self._mean = 0
            for i, p in enumerate(self.pmfy):
                self._mean += (self.lb + i * self.h) * p
            for i, p in enumerate(self.tpmfy):
                self._mean += (self.tb + i * self.th) * p
        return self._mean

    @property
    def std(self):
        if self._std is None:
            mean = self.mean
            self._std = 0
            for i, p in enumerate(self.pmfy):
                value = self.lb + i * self.h
                self._std += (value - mean)**2 * p
            for i, p in enumerate(self.tpmfy):
                value = self.tb + i * self.th
                self._std += (value - mean)**2 * p
            self._std = np.sqrt(self._std)
        return self._std

    def __add__(self, ddist):
        if not isinstance(ddist, DDist):
            raise TypeError('%s is not of DDist type'%ddist)
        if self.h != ddist.h or self.th != ddist.th:
            raise ValueError(
                'DDists must have the same sample rate to add: '
                'self.h:%s, ddist.h:%s, self.th:%s, ddist.th:%s'
                %(self.h, ddist.h, self.th, ddist.th))
        #bounds and intervals
        #the addition bounds are set to:
        #   [lb1 + lb2, tb1 + tb2), [tb1 + tb2, ub1 + ub2)
        lb = self.lb + ddist.lb
        tb = self.tb + ddist.tb
        ub = self.ub + ddist.ub
        tnh = int((self.th + 0.5 * self.h) / self.h)
        #print 'bounds', lb, tb, ub, tnh
        #compute the front part
        n = self.llen + ddist.llen
        pmfy = [0.0] * n
        for i in range(self.llen):
            for j in range(ddist.llen):
                pmfy[i + j] += self.pmfy[i] * ddist.pmfy[j]
        for i in range(self.llen):
            end = min(ddist.tlen, (n - i - 1 - ddist.llen) / tnh + 1)
            for j in range(end):
                k = i + ddist.llen + j * tnh
                pmfy[k] += self.pmfy[i] * ddist.tpmfy[j]
        for i in range(ddist.llen):
            end = min(self.tlen, (n - i - 1 - self.llen) / tnh + 1)
            for j in range(end):
                k = i + self.llen + j * tnh
                pmfy[k] += ddist.pmfy[i] * self.tpmfy[j]
        #print 'pmfy', pmfy
        #compute the tail part
        tn = self.tlen + ddist.tlen
        tpmfy = [0.0] * tn
        for i in range(self.tlen):
            start = max(0, ddist.llen - i * tnh)
            for j in range(start, ddist.llen):
                n = i + int(floor(float(j - ddist.llen) / tnh))
                tpmfy[n] += self.tpmfy[i] * ddist.pmfy[j]
        for i in range(ddist.tlen):
            start = max(0, self.llen - i * tnh)
            for j in range(start, self.llen):
                n = i + int(floor(float(j - ddist.llen) / tnh))
                tpmfy[n] += ddist.tpmfy[i] * self.pmfy[j]
        for i in range(self.tlen):
            for j in range(ddist.tlen):
                tpmfy[i + j] += self.tpmfy[i] * self.tpmfy[j]
        #print 'tpmfy', tpmfy
        #return
        return DDist(lb, pmfy, h=self.h, tpmfy=tpmfy, th=self.th)

    def getPmfxy(self):
        x = []
        y = []
        for i, p in enumerate(self.pmfy):
            x.append(self.lb + i * self.h)
            y.append(p)
        for i, p in enumerate(self.tpmfy):
            x.append(self.tb + i * self.th)
            y.append(p)
        return x, y

    def getCmfxy(self):
        x = []
        y = []
        for i, p in enumerate(self.cmfy):
            x.append(self.lb + i * self.h)
            y.append(p)
        for i, p in enumerate(self.tcmfy):
            x.append(self.tb + i * self.th)
            y.append(p)
        return x, y

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
    def sample(cls, config, h=0.1, tailprob=0, tnh=1, num=100000):
        x = RVGen.run(config, num)
        return cls.create(x, h, tailprob, tnh)

    @classmethod
    def create(cls, samples, h=0.1, tailprob=0, tnh=1):
        li = int(floor(min(samples) / h))
        ui = int(floor(max(samples) / h))
        lb = li * h
        spmf = [0.0] * (ui - li + 1)
        for x in samples:
            i = int(floor(x / h)) - li
            spmf[i] += 1
        #normalize
        for i in range(len(spmf)):
            spmf[i] /= len(samples)
        #print 'spmf', spmf
        scmf = [spmf[0]]
        for i in range(1, len(spmf)):
            scmf.append(scmf[i - 1] + spmf[i])
        #print 'scmf', scmf
        #compute tail
        if tailprob == 0 or tnh == 1:
            return DDist(lb, spmf, h, [], h)
        else:
            p = 0
            n = 0
            for i in range(len(spmf)):
                p += spmf[i]
                if p > 1 - tailprob:
                    break
                n += 1
            #find the next position that mod th == 0
            th = tnh * h
            tb = ceil(float(lb + n * h) / th) * th
            n = int((tb - lb) / h)
            pmf = list(spmf[0 : n])
            tlen = (len(spmf) - 1 - n) / tnh + 1
            tpmf = [0.0] * tlen
            for i in range(n + 1, len(spmf)):
                k = (i - n - 1) / tnh
                tpmf[k] += spmf[i]
            return DDist(lb, pmf, h, tpmf, th)

#####  TEST  ##### 
def testAdd():
    print '===== test add ====='
    #two step
    print '\n>>>two step dist\n'
    ddist0 = DDist.sample({'rv.name':'twostep','p':0.4, 'inf':-5, 'sup':10}, h=1)
    print ddist0.getPmfxy()
    print ddist0.getCmfxy()
    ddist1 = ddist0 + ddist0
    print ddist1.getPmfxy()
    print ddist1.getCmfxy()
    ddist1.plot('/tmp/test_add_twostep.pdf')
    #two step long tail
    print '\n>>>two step tail dist\n'
    samples = [1, 1, 1, 1, 1,
               2, 2, 2, 2, 2,
               3, 3, 3, 3, 3,
               4, 4, 4, 8, 8]
    y = []
    for i in range(100000):
        x = []
        for j in range(2):
            r = np.random.random()
            if r < 0.25:
                x.append(1)
            elif r < 0.5:
                x.append(2)
            elif r < 0.75:
                x.append(3)
            elif r < 0.9:
                x.append(4)
            else:
                x.append(8)
        y.append(x[0] + x[1])
    ddist0 = DDist.create(samples, h=1, tailprob=0.75, tnh=4)
    print ddist0.getPmfxy(), ddist0.lb, ddist0.tb
    ddist1 = DDist.create(y, h=1, tailprob=0.30, tnh=4)
    print ddist1.getPmfxy(), ddist1.lb, ddist1.tb
    ddist2 = ddist0 + ddist0
    print ddist2.getPmfxy()
    print ddist2.getCmfxy()
    #normal
    print '\n>>>normal dist\n'
    mu = 10
    sigma = 3
    x = []
    y = []
    for i in range(100000):
        x1 = np.random.normal(mu, sigma)
        x2 = np.random.normal(mu, sigma)
        x.append(x1); x.append(x2)
        y.append(x1 + x2)
    ddist1 = DDist.create(x, h=0.5)
    ddist2 = DDist.create(y, h=0.5)
    ddist1.plot('/tmp/test_normal.pdf')
    ddist3 = ddist1 + ddist1
    print ddist1.mean, ddist1.std
    print ddist2.mean, ddist2.std
    print ddist3.mean, ddist3.std
    print ddist2.pmfy[0:10]
    print ddist3.pmfy[0:10]
    ddist2.plot('/tmp/test_addnormal1.pdf')
    ddist3.plot('/tmp/test_addnormal2.pdf')
    ddist4 = DDist.create(x, h=0.5, tailprob=0.1, tnh=5)
    ddist4.plot('/tmp/test_normal_tail.pdf')
    ddist5 = ddist4 + ddist4
    print ddist4.mean, ddist4.std
    print ddist5.mean, ddist5.std
    print ddist4.pmfy[0:10]
    print ddist5.pmfy[0:10]
    #pareto
    print '\n>>>pareto dist\n'
    a = 1.3
    m = 100
    x = np.random.pareto(a, 2000000) + m
    np.random.shuffle(x)
    y = []
    for i in range(1000000):
        y.append(x[i] + x[i + 1000000])
    ddist1 = DDist.create(x, h=0.5)
    ddist2 = DDist.create(y, h=0.5, tailprob=0.1, tnh=100)
    print 'pmf', ddist1.pmfy[0:10]
    print 'pmf', ddist2.pmfy, ddist2.lb
    #ddist3 = ddist1 + ddist2
    print ddist1.mean, ddist1.std
    print ddist2.mean, ddist2.std, ddist2.lb, ddist2.tb
    #print ddist3.mean, ddist3.std
    ddist1.plot('/tmp/test_pareto.pdf')
    ddist2.plot('/tmp/test_addpareto1.pdf')
    #ddist3.plot('/tmp/test_addpareto2.pdf')
    ddist4 = DDist.create(x, h=0.5, tailprob=0.1, tnh=100)
    ddist4.plot('/tmp/test_pareto_tail.pdf')
    ddist5 = ddist4 + ddist4
    print 'pmf', ddist4.pmfy, ddist4.lb, ddist4.tb
    print 'pmf', ddist5.pmfy, ddist5.lb, ddist5.tb
    print ddist4.mean, ddist4.std
    ddist4.plot('/tmp/test_addpareto3.pdf')
    print ddist5.mean, ddist5.std
    print '===== end =====\n'

def test():
    testAdd()

def main():
    test()

if __name__ == '__main__':
    main()
