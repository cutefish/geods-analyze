import sys

from numpy import exp
from scipy.misc import comb

from common import Markov2D

class TideModel(Markov2D):
    """Tide model.

    The model has three steps:
        1. txn arrival and epoch begin;
        2. epoch replication process;
        3. locking and execution.

    The arrival and epoch begin step has two useful parameters:
        t_a     --  time for a txn to wait for an epoch to start
        n_a     --  number of txns each epoch
        
        f(t_a) = \lambda e^{-\lambda (T - t_a)}
        E(t_a) = T - 1/\lambda + e^{-\lambda T} / \lambda
        F(n_a) = (\lambda T)^n_a e^{-\lambda T} / n_a!
        E(n_a) = \lambda T

    The replication process is currently modeled only by the latency.

    The locking and execution process is modeled by a Markov chain. Transitions are:
        (w, r) -> (w, r + 1)            -- arrive and run, p = alpha**w * beta * a * dt
        (w, r) -> (w + 1, r)            -- arrive but wait, p = (1 - alpha**w * beta) * a * dt
        (w, r) -> (w - k, r + k - 1)    -- commit, p = s(w, k, r) * r * c * dt

    """
    def __init__(self, N, D, d, a, l, r, e, epsilon=None):
        """
        @args:
            N   --  total allowed number of txns.
            D   --  total number of items.
            d   --  number of items each txn.
            a   --  arrival rate.
            l   --  latency
            r   --  execution_time.
            e   --  epoch time

        """
        Markov2D.__init__(self, N + 1, N + 1)
        self.N = N
        self.alpha = comb(D - d, d) / comb(D, d)
        self.getBeta = lambda x : comb(D - x * d, d) / comb(D, d)
        self.arvRate = 1.0 / a
        self.latency = l
        self.cmtRate = 1.0 / r
        self.epochLen = e
        self.dpValues = {}
        if epsilon is None:
            self.epsilon = 1.0 / self.size / 1000.0

    def fillOffDiagonal(self, Q):
        #arrive
        for w in range(0, self.N):
            for r in range(0, self.N - w):
                #do not have (w, 0) state
                if r == 0 and w != 0:
                    continue
                p = self.alpha**(w) * self.getBeta(r)
                q = 1 - p
                if p != 0:
                    Q[(self.state(w, r), self.state(w, r + 1))] = self.arvRate * p
                if q != 0:
                    Q[(self.state(w, r), self.state(w + 1, r))] = self.arvRate * q
        #commit
        for w in range(0, self.N + 1):
            for r in range(1, self.N - w + 1):
                for k in range(0, w + 1):
                    p = self.getKRunProb(w, r - 1, k, self.alpha, self.getBeta(r - 1))
                    #print '(%s, %s)->(%s,%s):%s'%(w, r, w-k, r+k-1, p)
                    #print
                    if p != 0:
                        Q[(self.state(w, r), self.state(w - k, r + k - 1))] = \
                           r * self.cmtRate * p

    def getKRunProb(self, w, r, k, alpha, beta):
        #to save execution time and memory
        if beta < self.epsilon:
            return 0
        #dynamic programming
        # check computed
        if (w, r, k) in self.dpValues:
            #print 'in (w,r,k)', self.dpValues[(w, r, k)]
            return self.dpValues[(w, r, k)]
        # boundary
        if w < k:
            self.dpValues[(w, r, k)] = 0
            #print 'w<k', v
            return 0
        if w == k:
            v = alpha**((w - 1)*w / 2) * beta**w
            if v < self.epsilon:
                v = 0
            self.dpValues[(w, r, k)] = v
            #print 'w==k', v
            return v
        if k == 0:
            v = 1
            for i in range(w):
                v *= 1 - alpha**(i) * beta
            if v < self.epsilon:
                v = 0
            self.dpValues[(w, r, k)] = v
            #print 'k==0', v
            return v
        #s(w, r, k) = s(w - 1, r, k - 1) * p_{w-1} + s(w - 1, r, k) * q_{w - 1}
        p = alpha**(w - 1) * beta
        q = 1 - p
        s1 = self.getKRunProb(w - 1, r, k - 1, alpha, beta)
        s2 = self.getKRunProb(w - 1, r, k, alpha, beta)
        v = s1 * p + s2 * q
        if v < self.epsilon:
            v = 0
        self.dpValues[(w, r, k)] = v
        #print '%s*%s + %s*%s = %s'%(s1, p, s2, q, v)
        return v

    def run(self):
        """
        Calculate the following parameters:
            nwait   --  number of the txn waiting to run
            nrun    --  number of the txns actually running
            nqueue  --  total number of txns in the system
            tserve  --  time between txn submission and leave the system
        """
        #Results are calculated in three steps
        print ('N=%s, alpha=%s, '
               'arvRate=%s, latency=%s, cmtRate=%s, epochLen=%s'
               %(self.N, self.alpha, 
                 self.arvRate, self.latency, self.cmtRate, self.epochLen))
        # step1
        nwait1 = self.arvRate * self.epochLen
        nrun1 = 0
        nqueue1 = nwait1
        tserve1 = self.epochLen - 1 / self.arvRate + \
                exp(-self.arvRate * self.epochLen) / self.arvRate
        print ('nwait1=%s, nrun1=%s, nqueue1=%s, tserve1=%s'
               %(nwait1, nrun1, nqueue1, tserve1))
        # step2
        nwait2 = self.arvRate * self.latency
        nrun2 = 0
        nqueue2 = nwait2
        tserve2 = self.latency
        print ('nwait2=%s, nrun2=%s, nqueue2=%s, tserve2=%s'
               %(nwait2, nrun2, nqueue2, tserve2))
        # step3
        pi = self.computePi()
        print 'sum(pi)=%s' %sum(pi)
        print self.pi2str(pi, 10)
        nwait3, nrun3, nqueue3, loss, tserve3 = self.getPiStats(pi, self.arvRate)
        print ('nwait3=%s, nrun3=%s, nqueue3=%s, loss=%s, tserve3=%s'
               %(nwait3, nrun3, nqueue3, loss, tserve3))
        # sum up
        nwait = nwait1 + nwait2 + nwait3
        nrun = nrun1 + nrun2 + nrun3
        nqueue = nqueue1 + nqueue2 + nqueue3
        tserve = tserve1 + tserve2 + tserve3
        print ('nwait=%s, nrun=%s, nqueue=%s, tserve=%s'
               %(nwait, nrun, nqueue, tserve))
        return nwait, nrun, nqueue, loss, tserve

def runModel(N, D, d, a, l, r, e):
    model = TideModel(N, D, d, a, l, r, e)
    return model.run()

def main():
    if not len(sys.argv) == 8:
        print 'tide <N> <D> <d> <a> <l> <r> <e>'
        sys.exit(-1)
    N = int(sys.argv[1])
    D = int(sys.argv[2])
    d = int(sys.argv[3])
    a = float(sys.argv[4])
    l = float(sys.argv[5])
    r = float(sys.argv[6])
    e = float(sys.argv[7])
    runModel(N, D, d, a, l, r, e)

if __name__ == '__main__':
    main()
