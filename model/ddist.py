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
        #bounds and intervals
        #the addition bounds are set to:
        #   [lb1 + lb2, tb1 + tb2), [tb1 + tb2, ub1 + ub2)
        lb = self.lb + ddist.lb
        tb = self.tb + ddist.tb
        ub = self.ub + ddist.ub
        tnh = int((self.th + 0.5 * self.h) / self.h)
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
        #compute the tail part
        tn = self.tlen + ddist.tlen
        tpmfy = [0.0] * n
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
        #return
        return DDist(lb, pmfy, h=self.h, tmpfy=tpmfy, th=self.th)

