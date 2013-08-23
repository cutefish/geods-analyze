from math import floor

import numpy as np
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
            self.tcmfy = [self.tpmfy[0]]
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
        print 'bounds', lb, tb, ub, tnh
        #compute the front part
        n = self.llen + ddist.llen
        pmfy = [0.0] * n
        for i in range(self.llen):
            for j in range(ddist.llen):
                pmfy[i + j] += self.pmfy[i] * ddist.pmfy[j]
        for i in range(self.llen):
            end = min(ddist.tlen, (n - i - ddist.llen) / tnh)
            for j in range(end):
                k = i + ddist.llen + j * tnh
                pmfy[k] += self.pmfy[i] * ddist.tpmfy[j]
        for i in range(ddist.llen):
            end = min(ddist.tlen, (n - i - ddist.llen) / tnh)
            for j in range(end):
                k = i + ddist.llen + j * tnh
                pmfy[k] += self.pmfy[i] * ddist.tpmfy[j]
        #print 'pmfy', pmfy
        #compute the tail part
        tn = self.tlen + ddist.tlen
        tpmfy = [0.0] * tn
        for i in range(self.tlen):
            start = max(0, ddist.llen - i * tnh)
            for j in range(start, ddist.llen, tnh):
                for k in range(tnh):
                    tpmfy[i + j] = self.tpmfy[i] + ddist.pmfy[j * tnh + k]
        for i in range(ddist.tlen):
            start = max(0, self.llen - i * tnh)
            for j in range(start, self.llen, tnh):
                for k in range(tnh):
                    tpmfy[i + j] = ddist.tpmfy[i] + self.pmfy[j * tnh + k]
        for i in range(self.tlen):
            for j in range(ddist.tlen):
                tpmfy[i + j] = self.tpmfy[i] * self.tpmfy[j]
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
    def sample(cls, config, h=0.1, th=None, num=100000):
        x = RVGen.run(config, num)
        return cls.create(x, h, th)

    @classmethod
    def create(cls, samples, h=0.1, th=None):
        if th is None:
            th = 5 * h
        li = int(floor(min(samples) / h))
        ui = int(floor(max(samples) / h))
        samplePmf = [0.0] * (ui - li + 1)
        for x in samples:
            i = int(floor(x / h)) - li
            samplePmf[i] += 1
        #normalize
        for i in range(len(samplePmf)):
            samplePmf[i] /= len(samples)
        #compute tail
        p = 0
        ti = 0
        for i in range(len(samplePmf)):
            p += samplePmf[i]
            ti += 1
            if p > 0.95:
                break
        tnh = int((th + 0.5 * h) / h)
        if len(samplePmf) - ti > 0.05 * tnh * len(samplePmf):
            pmf = list(samplePmf[0 : ti + 1])
            tpmf = []
            for i in range(ti + 1, len(samplePmf), tnh):
                k = (i - ti - 1) / tnh
                tpmf.append(samplePmf[i])
                for j in range(1, tnh):
                    tpmf[k] += samplePmf[i + j]
        else:
            pmf = samplePmf
            tpmf = []
        #compute bounds
        lb = li * h
        tb = li + len(pmf) * h
        return DDist(lb, pmf, h, tpmf, th)

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
    #mu = 10
    #sigma = 3 
    #x = []
    #y = []
    #for i in range(100000):
    #    x1 = np.random.normal(mu, sigma)
    #    x2 = np.random.normal(mu, sigma)
    #    x.append(x1)
    #    x.append(x2)
    #    y.append(x1 + x2)
    #ddist2 = DDist.create(y, h=0.5)
    #ddist3 = DDist.create(x, h=0.5)
    #ddist4 = ddist3 + ddist3
    #print ddist2.mean, ddist2.std, ddist2.lb, ddist2.ub
    #print ddist3.mean, ddist3.std
    #print ddist4.mean, ddist4.std, ddist4.lb, ddist4.ub
    #ddist2.plot('/tmp/test_add1.pdf')
    #ddist4.plot('/tmp/test_add2.pdf')
    #print '===== end =====\n'

def test():
    testAdd()

def main():
    test()

if __name__ == '__main__':
    main()
