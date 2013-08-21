from math import floor

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
        self.tb = self._lb + len(pmfy) * self.h
        self.ub = self._tb + len(tpmfy) * self.th
        #pmf and cmf
        self.pmfy = pmfy
        self.cmfy = None
        self.tpmfy = tpmfy
        self.tcmfy = None
        #mean and std
        self._mean = None
        self._std = None

    def calcCmf(self):
        if self.cmfy is None:
            self.cmfy = [self.pmfy[0]]
            for i in range(1, len(pmfy)):
                self.cmfy.append(self.cmfy[i - 1] + self.pmfy[i])
            assert self.tcmfy is None
            self.tcmfy = [self.tpmfy[0]]
            for i in range(1, len(self.tpmfy)):
                self.tcmfy.append(self.tcmfy[i - 1] + self.tpmfy[i])

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
        self.calcCmf()
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
        #compute the front part
        lb = self.lb + ddist.lb
        tb = self.tb + ddist.tb - self.h
        n = self.llen + ddist.llen - 1
        pmfy = [0.0] * n
        for i in range(self.llen):
            for j in range(ddist.llen):
                pmfy[i + j] += self.pmfy[i] * ddist.pmfy[j]
        tnh = int((self.th + 0.5 * self.h) / self.h)
        for i in range(self.llen):
            end = (n - i) / tnh
            for jj in range(0, ddist.llen, tnh):
                for kk in range(0, tnh):
                    j = 

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

